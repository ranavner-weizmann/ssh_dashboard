"""
MAMP Dashboard (Mobile Atmospheric Measurement Platform)
----------------------------------------------------------
Connects to a remote machine over SSH (password auth), creates a timestamped
CSV file in /home/rsp/drone_air_system/, and lets you write notes from a
local web dashboard. Each note is appended live to the remote CSV over SFTP.
Also supports downloading the full data set (logger output, drone data,
and notes) from the remote machine into a local folder.

Run with:  python app.py
Then open: http://127.0.0.1:5050

(Deliberately not port 5000 - macOS's AirPlay Receiver listens there by
default and intercepts the connection before Flask ever sees it, which
shows up as an inexplicable 403 in the browser rather than a connection
error. Harmless but unnecessary on Windows, where that conflict doesn't
exist - the port is just kept the same everywhere.)

Runs on both Windows and macOS. The couple of places that inherently need
platform-specific handling (auto-opening Chrome, listing external drives
for "Backup to External Drive") branch on sys.platform internally; there
is no separate Windows/macOS build of this file.

Can also be packaged as a double-clickable macOS .app with PyInstaller;
see build_app.sh / app.spec in the same project for the build steps.
"""

import bisect
import csv
import io
import json
import math
import os
import plistlib
import re
import shlex
import shutil
import stat
import statistics
import subprocess
import sys
import logging
from logging.handlers import RotatingFileHandler
import threading
import time
import webbrowser
from datetime import datetime, timedelta

import anthropic
import paramiko
from flask import Flask, render_template, request, jsonify
from werkzeug.exceptions import HTTPException
from flask_sock import Sock

# --- Path resolution: works both as a plain script and as a frozen
# PyInstaller .app bundle. ---
#
# As a plain script, __file__ points at this .py file, so its folder is
# the natural "app folder."
#
# Once frozen into a .app, sys.frozen is set and the app's own files live
# inside the (effectively read-only, ephemeral on launch) bundle resources,
# so we anchor user-facing folders like downloaded data under the user's
# home directory instead - a stable, writable location regardless of
# where the .app happens to be (Applications, Desktop, another Mac, etc).
if getattr(sys, "frozen", False):
    APP_DIR = sys._MEIPASS  # where bundled resources (templates/) extract to
    USER_DATA_DIR = os.path.join(os.path.expanduser("~"), "RemoteNotesDashboard")
else:
    APP_DIR = os.path.dirname(os.path.abspath(__file__))
    USER_DATA_DIR = os.path.abspath(os.path.join(APP_DIR, ".."))

app = Flask(
    __name__,
    template_folder=os.path.join(APP_DIR, "templates"),
    static_folder=os.path.join(APP_DIR, "static"),
)
app.secret_key = "drone-notes-local-secret"  # only used for local Flask session cookie
sock = Sock(app)

REMOTE_DIR = "/home/rsp/drone_air_system"
NOTES_SUBFOLDER = "remote_ssh_notes"

# Remote sources pulled in by "Download All Data".
# Each entry: (remote subpath under REMOTE_DIR, local folder name to save as)
DOWNLOAD_SOURCES = [
    ("uri_aplogger/output", "output"),
    ("data_from_drone", "data_from_drone"),
    (NOTES_SUBFOLDER, "remote_ssh_notes"),
]

RUN_ALL_SUBDIR = "uri_aplogger"
RUN_ALL_SCRIPT = "runall.py"
RUN_ALL_LOG = "runall.log"
RUN_ALL_REMOTE_VENV = "/home/rsp/drone_air_system/venv"
RUN_ALL_VENV_PYTHON = f"{RUN_ALL_REMOTE_VENV}/bin/python"
RUN_ALL_PID_FILE = f"{REMOTE_DIR}/{RUN_ALL_SUBDIR}/runall.pid"

# runall.py reads this once at startup - toggling a sensor here has no
# effect on an already-running session, only on the next launch.
SENSOR_CONFIG_REMOTE_PATH = f"{REMOTE_DIR}/{RUN_ALL_SUBDIR}/sensor_config.json"

RUN_C_APP_REMOTE_PATH = "/home/rsp/Payload-SDK/build/bin/dji_sdk_demo_on_rpi"
RUN_C_APP_LOG = "/home/rsp/Payload-SDK/build/bin/dji_sdk_demo_on_rpi.log"

# Where downloaded data lands. As a script: downloaded_outputs/ next to the
# ssh_dashboard app folder (unchanged from before). As a packaged .app:
# ~/RemoteNotesDashboard/downloaded_outputs, since the bundle's own folder
# isn't a sensible or guaranteed-writable place to store user data.
DOWNLOADS_ROOT = os.path.join(USER_DATA_DIR, "downloaded_outputs")

# One global connection state (this app is meant for single-user local use)
state = {
    "ssh": None,
    "sftp": None,
    "remote_path": None,
    "connected": False,
    "host": None,
    "username": None,
    # Kept in memory only (never written to disk) so "Sync Time" can reuse
    # it as the sudo password without asking again - same password you
    # already typed to log in over SSH.
    "password": None,
    "runall_pid": None,
    "lock": threading.Lock(),
}

# Progress tracking for the "Download All Data" / "Backup to External
# Drive" background job - both are the same underlying copy operation
# (see _run_download_job), just pointed at a different target_root, so
# they share this one piece of state. Only one such job runs at a time
# (single-user local app, and both share the one SFTP connection, which
# isn't safe to drive from two threads at once).
download_state = {
    "running": False,
    "kind": None,           # "download" or "backup" - which button started this job
    "total_files": 0,
    "done_files": 0,
    "current_source": None,
    "results": None,       # filled in once finished
    "target_root": None,
    "error": None,          # top-level error (e.g. couldn't create folder)
    "lock": threading.Lock(),
}


def _detach_ssh_handles():
    """
    Grabs the current SSH/SFTP handles and clears them from state,
    returning (ssh, sftp) so the caller can close them separately -
    shared by _mark_disconnected() and /disconnect, which both need to
    hand the handles off rather than close() them inline (see
    _close_ssh_handles_async's docstring for why). Caller must hold
    state["lock"].
    """
    ssh = state["ssh"]
    sftp = state["sftp"]
    state["ssh"] = None
    state["sftp"] = None
    return ssh, sftp


def _close_ssh_handles_async(ssh, sftp):
    """
    Closes SSH/SFTP handles in a background thread, best-effort. On a
    dead/flaky field link, close() can sit on a TCP teardown timeout for
    a while - closing inline, before state["connected"] flips to False,
    left /status (read without the lock, so it doesn't even wait on this)
    reporting "still connected" for however long that took. A quick
    refresh during that window landed right back in the "connected" view
    even though the user had already clicked Disconnect. Closing here
    instead, after the caller has already flipped the flag, means every
    other endpoint sees the session as gone the instant it actually is,
    regardless of how long the underlying socket takes to tear down.
    """
    if not (ssh or sftp):
        return

    def _close():
        try:
            if sftp:
                sftp.close()
        except Exception:
            pass
        try:
            if ssh:
                ssh.close()
        except Exception:
            pass

    threading.Thread(target=_close, daemon=True).start()


def _mark_disconnected():
    """
    Marks the session as no longer connected and tears down the
    SSH/SFTP handles. Call this from a route's `except` block whenever a
    remote command throws - on this kind of unattended field connection,
    that almost always means the physical link actually died (cable
    pulled, Pi lost power, out of Wi-Fi range), not a one-off command
    hiccup. Without this, state["connected"] just stays True forever
    after the transport is already dead, and the dashboard's status dot
    keeps lying about it.

    Callers must already hold state["lock"] - this doesn't acquire it
    itself, since most call sites are already inside a `with state["lock"]:`
    block and the lock is a plain non-reentrant threading.Lock.
    """
    ssh, sftp = _detach_ssh_handles()
    state["connected"] = False
    _close_ssh_handles_async(ssh, sftp)


def build_remote_path():
    ts = datetime.now().strftime("%Y%m%d")
    filename = f"remote_pc_notes_{ts}.csv"
    return f"{REMOTE_DIR}/{NOTES_SUBFOLDER}/{filename}", filename


# ---------- Crash visibility ----------
# Every fetch() in the page expects JSON. An uncaught exception used to come
# back as Werkzeug's HTML "500 Internal Server Error" page, which the UI
# could only render as a generic failure, and the traceback existed nowhere
# but the exe's console window. Log it to a file and say what it was.
LOG_PATH = os.path.join(USER_DATA_DIR, "ssh_dashboard.log")


def _setup_file_logging():
    try:
        os.makedirs(USER_DATA_DIR, exist_ok=True)
        handler = RotatingFileHandler(LOG_PATH, maxBytes=2_000_000, backupCount=2, encoding="utf-8")
        handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
        app.logger.addHandler(handler)
        app.logger.setLevel(logging.INFO)
        app.logger.info("dashboard started, pid %s", os.getpid())
    except OSError:
        pass  # unwritable location: console output only, as before


_setup_file_logging()


@app.errorhandler(Exception)
def _unhandled_error(e):
    if isinstance(e, HTTPException):
        return e  # real 404s etc. keep their normal response
    app.logger.exception("Unhandled error in %s %s", request.method, request.path)
    return jsonify({
        "ok": False,
        "error": f"Internal error: {type(e).__name__}: {e} (traceback in {LOG_PATH})",
    }), 500


@app.route("/")
def index():
    return render_template(
        "index.html",
        connected=state["connected"],
        host=state["host"],
        username=state["username"],
        remote_path=state["remote_path"],
    )



# ---------- OneDrive backup: copy downloaded sessions into the user's OneDrive ----------
# A plain copy of the sessions already downloaded to this computer
# (DOWNLOADS_ROOT) into a folder of the user's choosing inside their OneDrive
# folder. Nothing is ever deleted locally, and a session already in OneDrive
# is brought up to date: files that are new, grew or changed are copied
# again, unchanged ones skipped - a day's folder keeps gaining runs. The
# OneDrive location and the chosen subfolder are remembered per computer in
# USER_DATA_DIR so the same setup works on another laptop. The offline
# explorer can be pointed at the backup folder (?root=onedrive).

ONEDRIVE_SETTINGS_PATH = os.path.join(USER_DATA_DIR, "onedrive_settings.json")
ONEDRIVE_FALLBACK_ROOT = r"C:\Users\YRGROUP\OneDrive - weizmann.ac.il"


def _onedrive_detected_roots():
    """OneDrive folders this computer knows about, most likely first."""
    roots = []
    for var in ("OneDriveCommercial", "OneDrive", "OneDriveConsumer"):
        p = os.environ.get(var)
        if p and os.path.isdir(p) and p not in roots:
            roots.append(p)
    home = os.path.expanduser("~")
    try:
        for entry in sorted(os.listdir(home)):
            p = os.path.join(home, entry)
            if entry.lower().startswith("onedrive") and os.path.isdir(p) and p not in roots:
                roots.append(p)
    except OSError:
        pass
    if os.path.isdir(ONEDRIVE_FALLBACK_ROOT) and ONEDRIVE_FALLBACK_ROOT not in roots:
        roots.append(ONEDRIVE_FALLBACK_ROOT)
    return roots


def _onedrive_settings():
    try:
        with open(ONEDRIVE_SETTINGS_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except (OSError, ValueError):
        return {}


def _save_onedrive_settings(data):
    os.makedirs(USER_DATA_DIR, exist_ok=True)
    with open(ONEDRIVE_SETTINGS_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)


def _onedrive_root():
    """The OneDrive folder in use: the saved one if it still exists, else the first detected."""
    saved = _onedrive_settings().get("root")
    if saved and os.path.isdir(saved):
        return saved
    detected = _onedrive_detected_roots()
    return detected[0] if detected else None


def _onedrive_backup_dir():
    """<root>/<chosen folder>, or None until both are set and exist."""
    root = _onedrive_root()
    folder = _onedrive_settings().get("folder")
    if not root or not folder:
        return None
    path = os.path.join(root, folder)
    return path if os.path.isdir(path) else None


def _valid_folder_name(name):
    return bool(name) and name not in (".", "..") and not any(c in name for c in '/\\:*?"<>|')


def _onedrive_subfolders(root):
    out = []
    try:
        for entry in sorted(os.listdir(root), key=str.lower):
            if entry.startswith(".") or entry.startswith("~"):
                continue
            if os.path.isdir(os.path.join(root, entry)):
                out.append(entry)
    except OSError:
        pass
    return out


def _file_needs_copy(src, dst):
    """New, grown/shrunk, or modified since the copy - never 'identical'."""
    try:
        st = os.stat(src)
    except OSError:
        return False
    try:
        dt = os.stat(dst)
    except OSError:
        return True
    return st.st_size != dt.st_size or st.st_mtime > dt.st_mtime + 1


def _session_backup_state(local_dir, dest_dir):
    """Counts for one local session against its copy under dest_dir (may not exist)."""
    files = pending = 0
    size = pending_bytes = 0
    for dirpath, _dirs, names in os.walk(local_dir):
        rel = os.path.relpath(dirpath, local_dir)
        for name in names:
            src = os.path.join(dirpath, name)
            try:
                b = os.path.getsize(src)
            except OSError:
                continue
            files += 1
            size += b
            dst = os.path.join(dest_dir, rel, name) if dest_dir else None
            if dst is None or _file_needs_copy(src, dst):
                pending += 1
                pending_bytes += b
    if dest_dir is None or not os.path.isdir(dest_dir):
        status = "not backed up"
    elif pending == 0:
        status = "up to date"
    else:
        status = "changes to copy"
    return {"files": files, "bytes": size, "pending_files": pending, "pending_bytes": pending_bytes, "status": status}


def _onedrive_config_payload():
    root = _onedrive_root()
    settings = _onedrive_settings()
    folder = settings.get("folder")
    backup_dir = _onedrive_backup_dir()
    sessions = []
    if os.path.isdir(DOWNLOADS_ROOT):
        for name in sorted(os.listdir(DOWNLOADS_ROOT), key=str.lower):
            local = os.path.join(DOWNLOADS_ROOT, name)
            if not os.path.isdir(local):
                continue
            state_ = _session_backup_state(local, os.path.join(backup_dir, name) if backup_dir else None)
            state_["name"] = name
            sessions.append(state_)
    return {
        "ok": True,
        "root": root,
        "root_saved": settings.get("root"),
        "detected_roots": _onedrive_detected_roots(),
        "folders": _onedrive_subfolders(root) if root else [],
        "folder": folder if (folder and backup_dir) else None,
        "backup_dir": backup_dir,
        "downloads_root": DOWNLOADS_ROOT,
        "sessions": sessions,
    }


@app.route("/onedrive")
def onedrive_page():
    return render_template("onedrive.html")


@app.route("/onedrive/config")
def onedrive_config():
    return jsonify(_onedrive_config_payload())


@app.route("/onedrive/config", methods=["POST"])
def onedrive_config_save():
    data = request.get_json() or {}
    settings = _onedrive_settings()
    if "root" in data:
        root = (data.get("root") or "").strip()
        if root:
            if not os.path.isabs(root) or not os.path.isdir(root):
                return jsonify({"ok": False, "error": "That OneDrive folder does not exist on this computer."}), 400
            if os.path.normcase(os.path.abspath(root)) != os.path.normcase(os.path.abspath(settings.get("root") or "")):
                settings.pop("folder", None)   # a different OneDrive: the old subfolder no longer applies
            settings["root"] = os.path.abspath(root)
        else:
            settings.pop("root", None)
    if "folder" in data:
        folder = (data.get("folder") or "").strip()
        if folder:
            if not _valid_folder_name(folder):
                return jsonify({"ok": False, "error": "Invalid folder name."}), 400
            root = settings.get("root") if settings.get("root") and os.path.isdir(settings["root"]) else _onedrive_root()
            if not root or not os.path.isdir(os.path.join(root, folder)):
                return jsonify({"ok": False, "error": "That folder does not exist in the OneDrive folder."}), 400
            settings["folder"] = folder
        else:
            settings.pop("folder", None)
    _save_onedrive_settings(settings)
    return jsonify(_onedrive_config_payload())


@app.route("/onedrive/folder", methods=["POST"])
def onedrive_folder_create():
    data = request.get_json() or {}
    name = (data.get("name") or "").strip()
    if not _valid_folder_name(name):
        return jsonify({"ok": False, "error": "Invalid folder name."}), 400
    root = _onedrive_root()
    if not root:
        return jsonify({"ok": False, "error": "No OneDrive folder is set."}), 400
    try:
        os.makedirs(os.path.join(root, name), exist_ok=True)
    except OSError as e:
        return jsonify({"ok": False, "error": f"Could not create the folder: {e}"}), 500
    settings = _onedrive_settings()
    settings["root"] = root
    settings["folder"] = name
    _save_onedrive_settings(settings)
    return jsonify(_onedrive_config_payload())


_onedrive_job = {"running": False}
_onedrive_job_lock = threading.Lock()


def _run_onedrive_backup(session_names, backup_dir):
    job = _onedrive_job
    try:
        plan = []   # (src, dst, bytes, session)
        for name in session_names:
            local = os.path.join(DOWNLOADS_ROOT, name)
            for dirpath, _dirs, files in os.walk(local):
                rel = os.path.relpath(dirpath, local)
                for fname in files:
                    src = os.path.join(dirpath, fname)
                    dst = os.path.join(backup_dir, name, rel, fname)
                    if _file_needs_copy(src, dst):
                        try:
                            plan.append((src, dst, os.path.getsize(src), name))
                        except OSError:
                            continue
        with _onedrive_job_lock:
            job["total_files"] = len(plan)
            job["total_bytes"] = sum(p[2] for p in plan)
        for src, dst, size, name in plan:
            with _onedrive_job_lock:
                job["current"] = os.path.relpath(src, DOWNLOADS_ROOT)
            try:
                os.makedirs(os.path.dirname(dst), exist_ok=True)
                shutil.copy2(src, dst)
                if os.path.getsize(dst) != size:
                    raise OSError("size mismatch after copy")
                with _onedrive_job_lock:
                    job["copied_files"] += 1
                    job["copied_bytes"] += size
                    job["per_session"].setdefault(name, {"copied": 0, "errors": 0})["copied"] += 1
            except OSError as e:
                with _onedrive_job_lock:
                    job["errors"].append(f"{os.path.relpath(src, DOWNLOADS_ROOT)}: {e}")
                    job["per_session"].setdefault(name, {"copied": 0, "errors": 0})["errors"] += 1
    except Exception as e:
        app.logger.exception("OneDrive backup job failed")
        with _onedrive_job_lock:
            job["errors"].append(f"backup stopped: {type(e).__name__}: {e}")
    finally:
        with _onedrive_job_lock:
            job["running"] = False
            job["finished_at"] = time.time()
            job["current"] = None


@app.route("/onedrive/backup", methods=["POST"])
def onedrive_backup_start():
    data = request.get_json() or {}
    names = data.get("sessions") or []
    if not isinstance(names, list) or not names:
        return jsonify({"ok": False, "error": "Pick at least one session."}), 400
    for name in names:
        if not isinstance(name, str) or not _valid_folder_name(name) or not os.path.isdir(os.path.join(DOWNLOADS_ROOT, name)):
            return jsonify({"ok": False, "error": f"Unknown session: {name}"}), 400
    backup_dir = _onedrive_backup_dir()
    if not backup_dir:
        return jsonify({"ok": False, "error": "Choose a destination folder in OneDrive first."}), 400
    with _onedrive_job_lock:
        if _onedrive_job.get("running"):
            return jsonify({"ok": False, "error": "A backup is already running."}), 409
        _onedrive_job.clear()
        _onedrive_job.update({
            "running": True, "started_at": time.time(), "finished_at": None, "sessions": list(names),
            "backup_dir": backup_dir, "total_files": None, "total_bytes": 0,
            "copied_files": 0, "copied_bytes": 0, "current": None, "errors": [], "per_session": {},
        })
    threading.Thread(target=_run_onedrive_backup, args=(list(names), backup_dir), daemon=True).start()
    return jsonify({"ok": True, "started": True})


@app.route("/onedrive/progress")
def onedrive_progress():
    with _onedrive_job_lock:
        return jsonify({"ok": True, "job": dict(_onedrive_job)})


def _sessions_root():
    """
    Where the offline explorer's sessions live for this request: the local
    downloads folder, or the OneDrive backup folder when the page was
    opened with ?root=onedrive (every offline request carries it).
    """
    try:
        root = request.args.get("root") or ""
    except RuntimeError:
        root = ""
    if root == "onedrive":
        backup_dir = _onedrive_backup_dir()
        if backup_dir:
            return backup_dir
    return DOWNLOADS_ROOT


@app.route("/offline")
def offline():
    """
    The offline entry point - browses whatever's already sitting in
    downloaded_outputs/ (from a past "Download All Data" or "Backup to
    External Drive") straight off the local disk, no SSH connection
    needed. First step towards SSH being one option among several rather
    than the only way into the app.
    """
    return render_template("offline.html")


@app.route("/offline/sessions")
def offline_sessions():
    """
    Lists every folder directly under downloaded_outputs/ - each one is
    a past download's subfolder name - along with a rough sense of what's
    in it (run count under output/, if present) and when it was last
    touched, newest first.
    """
    sessions_root = _sessions_root()
    if not os.path.isdir(sessions_root):
        return jsonify({"ok": True, "sessions": []})

    sessions = []
    for name in os.listdir(sessions_root):
        full = os.path.join(sessions_root, name)
        if not os.path.isdir(full):
            continue
        run_count = 0
        output_dir = os.path.join(full, "output")
        if os.path.isdir(output_dir):
            run_count = sum(
                1 for entry in os.listdir(output_dir)
                if os.path.isdir(os.path.join(output_dir, entry))
            )
        sessions.append({
            "name": name,
            "run_count": run_count,
            "modified": os.path.getmtime(full),
        })

    sessions.sort(key=lambda s: s["modified"], reverse=True)
    return jsonify({"ok": True, "sessions": sessions})


# ---------- Offline: merging a session's runs, and viewing the result ----------
# A downloaded session folder often holds several runall.py runs from the
# same day (e.g. stopped and restarted between flights). These endpoints
# merge every run on one date into a single CSV per file "type" -
# merged_data, vitals_summary, and each per-sensor csv/<x>_data - so a
# whole day's flights can be browsed/plotted as one continuous dataset,
# then reuse the same table/column/plot_multi shapes as the online Data
# Viewer (reading local files instead of SFTP) so the frontend can drive
# both with the same plotting code.

_RUN_DIR_RE = re.compile(r"^(\d{8})_\d{6}$")


def _valid_session_name(name):
    return bool(name) and not any(c in name for c in ("/", "\\", "..")) and name not in (".", "..")


def _session_output_dir(session_name):
    return os.path.join(_sessions_root(), session_name, "output")


def _session_merged_dir(session_name, date):
    return os.path.join(_sessions_root(), session_name, "merged", date)


def _session_drone_dir(session_name):
    # "Download All Data"/"Backup to External Drive" save this session's
    # copy of the remote data_from_drone/ folder here (see
    # DOWNLOAD_SOURCES) - flat telemetry_<date>_<time>.csv files, one per
    # flight, unrelated to and not nested under output/ (the drone's own
    # PSDK C app writes these independently of uri_aplogger/runall.py).
    return os.path.join(_sessions_root(), session_name, "data_from_drone")


def _session_drone_merged_dir(session_name, date):
    # Kept under data_from_drone/ itself (not alongside merged_data_* in
    # _session_merged_dir) so a date's merged drone CSV can never be
    # mistaken for one more file under source="merged" - the two sources
    # stay physically as well as logically separate. See _merge_drone_day.
    return os.path.join(_session_drone_dir(session_name), "merged", date)


def _list_run_dirs_by_date(session_name):
    """
    Groups a session's output/<run> folders by the date embedded in their
    name (runall.py names them YYYYMMDD_HHMMSS), each date's runs sorted
    chronologically - the local-disk, per-session equivalent of
    /data_viewer/runs, grouped for merging rather than left flat.
    """
    output_dir = _session_output_dir(session_name)
    by_date = {}
    if os.path.isdir(output_dir):
        for entry in os.listdir(output_dir):
            if not os.path.isdir(os.path.join(output_dir, entry)):
                continue
            m = _RUN_DIR_RE.match(entry)
            if not m:
                continue
            by_date.setdefault(m.group(1), []).append(entry)
    for runs in by_date.values():
        runs.sort()
    return by_date


_DRONE_FILE_RE = re.compile(r"^telemetry_(\d{8})_\d{6}\.csv$")


def _list_drone_files_by_date(session_name):
    """
    Groups this session's data_from_drone/telemetry_<date>_<time>.csv
    files by the date embedded in their name - the drone-telemetry
    equivalent of _list_run_dirs_by_date, needed because
    data_from_drone/ (unlike a merged day's own folder) is flat across
    every date the drone ever flew in this session, not pre-split by day.
    """
    drone_dir = _session_drone_dir(session_name)
    by_date = {}
    if os.path.isdir(drone_dir):
        for entry in os.listdir(drone_dir):
            m = _DRONE_FILE_RE.match(entry)
            # The drone's onboard clock defaults to 2021 until it gets a
            # GPS fix, so pre-fix telemetry is stamped with a bogus 2021
            # date - exclude it rather than show it as a selectable date.
            if not m or m.group(1).startswith("2021"):
                continue
            by_date.setdefault(m.group(1), []).append(entry)
    for files in by_date.values():
        files.sort()
    return by_date


def _drone_day_merged_dir_if_present(name, date):
    """
    Returns _session_drone_merged_dir(name, date) if _merge_drone_day
    has actually written something there, else None - the single check
    _offline_source_dir/_offline_files_for_date both use to decide
    whether a date's drone view should serve the merged CSV or fall back
    to the individual flight files.
    """
    merged_dir = _session_drone_merged_dir(name, date)
    if os.path.isdir(merged_dir) and os.listdir(merged_dir):
        return merged_dir
    return None


def _offline_source_dir(name, source, date):
    """
    Resolves which local folder an offline read endpoint should look in -
    a merged day's own folder (source="merged", the default - every one
    of these endpoints predates the drone source and keeps behaving
    exactly as before), this date's merged drone CSV if one has been
    made (see _merge_drone_day), or otherwise this session's flat
    data_from_drone/ folder (source="drone"). Unlike _session_merged_dir,
    the flat drone folder isn't itself date-scoped - callers reading
    "every file for this date" under source="drone" need
    _offline_files_for_date below, not a plain directory listing.
    """
    if source == "drone":
        return _drone_day_merged_dir_if_present(name, date) or _session_drone_dir(name)
    return _session_merged_dir(name, date)


def _offline_files_for_date(name, source, date):
    """
    [(relpath, size), ...] for whichever files back this date's offline
    view - every CSV in the merged day's own folder (source="merged";
    that folder is already scoped to one date, same as
    _list_csv_files_in_dir elsewhere). For source="drone": this date's
    merged telemetry CSV alone, if _merge_drone_day has made one - a day
    with several flights would otherwise offer the same handful of
    column names once per flight, which is exactly what merging fixes -
    or otherwise every individual telemetry_*.csv flight file for this
    date out of the session's flat data_from_drone/ folder (everything
    else in that folder belongs to a different date and must stay
    excluded).
    """
    if source == "drone":
        merged_dir = _drone_day_merged_dir_if_present(name, date)
        if merged_dir:
            return _list_csv_files_in_dir(merged_dir)
        drone_dir = _session_drone_dir(name)
        files = []
        for fname in _list_drone_files_by_date(name).get(date, []):
            try:
                files.append((fname, os.path.getsize(os.path.join(drone_dir, fname))))
            except OSError:
                continue
        return files
    return _list_csv_files_in_dir(_session_merged_dir(name, date))


def _list_csv_files_in_dir(base_dir):
    """
    Local-disk equivalent of _list_run_csv_files: every .csv directly in
    base_dir, plus its csv/ subfolder - the same two-location shape every
    run (and now every merged day) uses.
    """
    files = []
    if os.path.isdir(base_dir):
        for name in sorted(os.listdir(base_dir)):
            if name.endswith(".csv"):
                files.append((name, os.path.getsize(os.path.join(base_dir, name))))
    csv_dir = os.path.join(base_dir, "csv")
    if os.path.isdir(csv_dir):
        for name in sorted(os.listdir(csv_dir)):
            if name.endswith(".csv"):
                files.append((f"csv/{name}", os.path.getsize(os.path.join(csv_dir, name))))
    return files


def _read_local_csv(base_dir, relpath):
    """
    Local-disk equivalent of _read_remote_csv: validates the relpath
    shape (reusing the same run-CSV validator - a merged day's folder has
    the identical root-file/csv-subfolder shape) and reads it straight
    off disk, no SSH involved.
    """
    if not _valid_csv_relpath(relpath):
        return None, (jsonify({"ok": False, "error": "Invalid file path."}), 400)
    full_path = os.path.join(base_dir, relpath)
    try:
        with open(full_path, "r", encoding="utf-8", errors="replace") as f:
            raw = f.read()
    except Exception as e:
        return None, (jsonify({"ok": False, "error": f"Could not read {relpath}: {e}"}), 500)
    return raw, None


def _merge_day(session_name, date, dest_dir=None):
    """
    Concatenates every run's merged_data_*.csv on `date` into one CSV,
    written to <session>/merged/<date>/merged_data_<date>_merged.csv.
    merged_data is runall.py's own per-run join of every instrument onto
    one row-per-timestamp table (imet_*, pom_*, trisonica_*, ... columns
    all together) - so concatenating it across a day's runs already
    yields one file with every instrument in it, without re-joining the
    per-sensor csv/ logs (which are each on their own independent
    timebase and would need a real join, not a concatenation, to combine
    - out of scope here since merged_data already did that per-run work).

    Runs are concatenated in chronological (run-folder-name) order, which
    is already time order within each run's own rows, so no separate sort
    pass is needed - flights on a given day are back-to-back and
    non-overlapping. A leading "_source_run" column (inserted right after
    the original time column, so column 0 is still the time axis - the
    same convention every unmerged CSV uses) traces each row back to the
    run it came from. Always regenerates from scratch, so re-merging
    after a fresh download just picks up whatever runs exist now rather
    than risking a stale partial merge.

    dest_dir, if given, is an arbitrary folder elsewhere on this machine
    (chosen in the "Merge" modal) that also gets a copy of the finished
    CSV. It's purely an extra copy for the user's own filing - the
    canonical copy under <session>/merged/<date>/ is always written
    regardless, since that's what this offline viewer's own
    files/columns/table/plot_multi endpoints read from. A failed copy
    (e.g. the folder disappeared, a full disk) doesn't fail the merge
    itself - the real merge already succeeded - it's reported back as
    dest_error instead.

    Returns ({"rows": N, "skipped_rows": N, "dest_path": path_or_None,
    "dest_error": message_or_None}, None) on success, or (None,
    error_message) if there was nothing to merge.
    """
    by_date = _list_run_dirs_by_date(session_name)
    runs = by_date.get(date)
    if not runs:
        return None, "No runs found for that date."

    output_dir = _session_output_dir(session_name)
    merged_dir = _session_merged_dir(session_name, date)

    entries = []  # [(run_name, full_path), ...] in run (= time) order
    for run in runs:
        run_dir = os.path.join(output_dir, run)
        for name in sorted(os.listdir(run_dir)):
            if name.endswith(".csv") and name.startswith("merged_data_"):
                entries.append((run, os.path.join(run_dir, name)))
                break  # one merged_data file per run

    if not entries:
        return None, "No merged_data CSV found in this date's runs."

    # Clear out anything already here before writing - re-merging is
    # meant to fully regenerate this date's output, and an older version
    # of this feature wrote a file per instrument instead of one; without
    # this, re-merging a date merged under that older behavior would
    # leave its per-instrument leftovers sitting alongside the new single
    # file rather than actually replacing them.
    if os.path.isdir(merged_dir):
        shutil.rmtree(merged_dir)
    os.makedirs(merged_dir, exist_ok=True)
    out_path = os.path.join(merged_dir, f"merged_data_{date}_merged.csv")

    # Runs on one day do not always share a header: a run started with a
    # different set of sensors enabled (or after a logger update that added
    # columns) has more or fewer columns than the others. The merge used to
    # keep only runs whose header matched the day's first run exactly and
    # silently drop the rest - a day whose 01:44 test run lacked the
    # TriSonica lost every real flight after it. Now the output header is
    # the union of every run's columns (first run's order, new columns
    # appended as they appear) and each row is written by column name,
    # blank where that run never had the column.
    headers = []
    for run, path in entries:
        with open(path, "r", newline="", encoding="utf-8", errors="replace") as in_f:
            try:
                headers.append((run, path, next(csv.reader(in_f))))
            except StopIteration:
                continue
    if not headers:
        return None, "This date's merged_data CSVs are empty."
    union = []
    seen = set()
    for _run, _path, h in headers:
        for col in h:
            if col not in seen:
                seen.add(col)
                union.append(col)
    time_col = headers[0][2][0]
    out_fields = [time_col, "_source_run"] + [c for c in union if c != time_col]

    total_rows = 0
    skipped_rows = 0
    with open(out_path, "w", newline="", encoding="utf-8") as out_f:
        writer = csv.DictWriter(out_f, fieldnames=out_fields, extrasaction="ignore")
        writer.writeheader()
        for run, path, this_header in headers:
            with open(path, "r", newline="", encoding="utf-8", errors="replace") as in_f:
                reader = csv.reader(in_f)
                next(reader, None)  # header, already known
                width = len(this_header)
                for row in reader:
                    if len(row) != width:
                        skipped_rows += 1
                        continue
                    record = dict(zip(this_header, row))
                    record["_source_run"] = run
                    if time_col not in record:
                        record[time_col] = row[0]
                    writer.writerow(record)
                    total_rows += 1
    result = {"rows": total_rows, "skipped_rows": skipped_rows, "dest_path": None, "dest_error": None,
              "runs": len(headers), "columns": len(out_fields)}
    if dest_dir:
        try:
            os.makedirs(dest_dir, exist_ok=True)
            dest_path = os.path.join(dest_dir, os.path.basename(out_path))
            shutil.copy2(out_path, dest_path)
            result["dest_path"] = dest_path
        except Exception as e:
            result["dest_error"] = f"Merged, but could not copy to {dest_dir}: {e}"

    return result, None


def _merge_drone_day(session_name, date, dest_dir=None):
    """
    The drone-telemetry equivalent of _merge_day: concatenates every
    flight (data_from_drone/telemetry_<date>_<time>.csv) on `date` into
    one CSV, written to
    <session>/data_from_drone/merged/<date>/telemetry_<date>_merged.csv.
    Once this exists, _offline_source_dir/_offline_files_for_date serve
    it instead of the individual flight files for source="drone" - a
    day with several flights previously showed every flight's identical
    column names duplicated once per flight in the variable picker;
    merging collapses that back down to one file, one set of variables,
    same as a merged sensor day.

    Unlike a sensor run's merged_data (independently-sampled instruments
    joined onto one row-per-timestamp table), every flight already
    shares the exact same schema straight from the drone's own logger,
    so this is a plain concatenation - no per-instrument join involved.
    Flights are concatenated in filename (= time) order, same
    chronological-by-construction assumption _merge_day relies on. A
    leading "_source_file" column traces each row back to which flight
    it came from, playing the same role _merge_day's "_source_run" does.
    Always regenerates from scratch, so re-merging after downloading
    more flights just picks up whatever's there now.

    Returns ({"rows": N, "skipped_rows": N, "dest_path": path_or_None,
    "dest_error": message_or_None}, None) on success, or (None,
    error_message) if there was nothing to merge.
    """
    flights = _list_drone_files_by_date(session_name).get(date, [])
    if not flights:
        return None, "No drone telemetry found for that date."

    drone_dir = _session_drone_dir(session_name)
    merged_dir = _session_drone_merged_dir(session_name, date)

    if os.path.isdir(merged_dir):
        shutil.rmtree(merged_dir)
    os.makedirs(merged_dir, exist_ok=True)
    out_path = os.path.join(merged_dir, f"telemetry_{date}_merged.csv")

    header = None
    total_rows = 0
    skipped_rows = 0
    with open(out_path, "w", newline="", encoding="utf-8") as out_f:
        writer = csv.writer(out_f)
        for fname in flights:
            path = os.path.join(drone_dir, fname)
            with open(path, "r", newline="", encoding="utf-8", errors="replace") as in_f:
                reader = csv.reader(in_f)
                try:
                    this_header = next(reader)
                except StopIteration:
                    continue
                if header is None:
                    header = this_header
                    writer.writerow([this_header[0], "_source_file"] + this_header[1:])
                elif this_header != header:
                    # Schema drift between flights (e.g. a firmware
                    # update mid-session) - skip rather than silently
                    # misaligning columns; that flight's own file is
                    # still there to view individually.
                    continue
                for row in reader:
                    if len(row) != len(this_header):
                        skipped_rows += 1
                        continue
                    writer.writerow([row[0], fname] + row[1:])
                    total_rows += 1

    result = {"rows": total_rows, "skipped_rows": skipped_rows, "dest_path": None, "dest_error": None}

    if dest_dir:
        try:
            os.makedirs(dest_dir, exist_ok=True)
            dest_path = os.path.join(dest_dir, os.path.basename(out_path))
            shutil.copy2(out_path, dest_path)
            result["dest_path"] = dest_path
        except Exception as e:
            result["dest_error"] = f"Merged, but could not copy to {dest_dir}: {e}"

    return result, None


@app.route("/offline/session/<name>/days")
def offline_session_days(name):
    """
    Lists this session's dates, each with its sensor run count and
    whether it's already been merged (the frontend uses this to offer
    "Merge" for a fresh date and "View merged data" for one already
    done), plus any drone telemetry flights on that date and whether
    *those* have already been merged into one CSV (see
    _merge_drone_day). A date can have either, both, or (for a
    drone-only date - the drone flew a shakedown flight with no sensor
    rig running) just drone_files, so the date list is the union of both
    sources rather than driven off sensor runs alone.
    """
    if not _valid_session_name(name):
        return jsonify({"ok": False, "error": "Invalid session name."}), 400

    by_date = _list_run_dirs_by_date(name)
    drone_by_date = _list_drone_files_by_date(name)
    drone_dir = _session_drone_dir(name)

    days = []
    for date in sorted(set(by_date) | set(drone_by_date), reverse=True):
        runs = by_date.get(date, [])
        merged_dir = _session_merged_dir(name, date)
        drone_merged_dir = _session_drone_merged_dir(name, date)
        days.append({
            "date": date,
            "run_count": len(runs),
            "merged": os.path.isdir(merged_dir) and bool(os.listdir(merged_dir)),
            "drone_files": [
                {"file": f, "size": os.path.getsize(os.path.join(drone_dir, f))}
                for f in drone_by_date.get(date, [])
            ],
            "drone_merged": os.path.isdir(drone_merged_dir) and bool(os.listdir(drone_merged_dir)),
        })
    return jsonify({"ok": True, "days": days})


@app.route("/offline/session/<name>/merge", methods=["POST"])
def offline_merge_day(name):
    if not _valid_session_name(name):
        return jsonify({"ok": False, "error": "Invalid session name."}), 400
    data = request.get_json() or {}
    date = (data.get("date") or "").strip()
    if not re.match(r"^\d{8}$", date):
        return jsonify({"ok": False, "error": "Invalid date."}), 400

    # Optional "also save a copy here" folder from the merge modal - must
    # be absolute (a relative path is meaningless from a browser, which
    # has no notion of this process's cwd) before it's ever used as a
    # write target.
    dest_dir = (data.get("dest_dir") or "").strip() or None
    if dest_dir and not os.path.isabs(dest_dir):
        return jsonify({"ok": False, "error": "Destination must be an absolute path."}), 400

    try:
        result, err = _merge_day(name, date, dest_dir=dest_dir)
    except Exception as e:
        return jsonify({"ok": False, "error": f"Merge failed: {e}"}), 500
    if err:
        return jsonify({"ok": False, "error": err}), 400

    return jsonify({
        "ok": True,
        "rows": result["rows"],
        "skipped_rows": result["skipped_rows"],
        "dest_path": result["dest_path"],
        "dest_error": result["dest_error"],
    })


@app.route("/offline/session/<name>/merge_drone", methods=["POST"])
def offline_merge_drone_day(name):
    if not _valid_session_name(name):
        return jsonify({"ok": False, "error": "Invalid session name."}), 400
    data = request.get_json() or {}
    date = (data.get("date") or "").strip()
    if not re.match(r"^\d{8}$", date):
        return jsonify({"ok": False, "error": "Invalid date."}), 400

    dest_dir = (data.get("dest_dir") or "").strip() or None
    if dest_dir and not os.path.isabs(dest_dir):
        return jsonify({"ok": False, "error": "Destination must be an absolute path."}), 400

    try:
        result, err = _merge_drone_day(name, date, dest_dir=dest_dir)
    except Exception as e:
        return jsonify({"ok": False, "error": f"Merge failed: {e}"}), 500
    if err:
        return jsonify({"ok": False, "error": err}), 400

    return jsonify({
        "ok": True,
        "rows": result["rows"],
        "skipped_rows": result["skipped_rows"],
        "dest_path": result["dest_path"],
        "dest_error": result["dest_error"],
    })


@app.route("/offline/session/<name>/merged/<date>/files")
def offline_merged_files(name, date):
    if not _valid_session_name(name):
        return jsonify({"ok": False, "error": "Invalid session name."}), 400
    if not re.match(r"^\d{8}$", date):
        return jsonify({"ok": False, "error": "Invalid date."}), 400
    files = _list_csv_files_in_dir(_session_merged_dir(name, date))
    return jsonify({"ok": True, "files": [{"path": path, "size": size} for path, size in files]})


@app.route("/offline/session/<name>/merged/<date>/columns")
def offline_merged_columns(name, date):
    """
    Column info for every file backing this date's view, in one
    response, so the plot picker can offer every instrument's data as
    one flat list. source="merged" (the default) means the merged day's
    single CSV; source="drone" means that date's telemetry_*.csv
    flight(s). Safe to read every file up front here (unlike the online
    Data Viewer, which fetches only the one file a user picks) since this
    is plain local disk I/O, not a slow/high-latency SSH round trip per
    file.
    """
    if not _valid_session_name(name):
        return jsonify({"ok": False, "error": "Invalid session name."}), 400
    if not re.match(r"^\d{8}$", date):
        return jsonify({"ok": False, "error": "Invalid date."}), 400
    source = (request.args.get("source") or "merged").strip()
    base_dir = _offline_source_dir(name, source, date)

    files = []
    for relpath, _size in _offline_files_for_date(name, source, date):
        raw, err = _read_local_csv(base_dir, relpath)
        if err:
            continue
        rows = list(csv.reader(io.StringIO(raw)))
        if not rows:
            continue
        columns = rows[0]
        files.append({
            "file": relpath,
            "columns": columns,
            "column_numeric": _compute_column_numeric(columns, rows[1:]),
        })
    return jsonify({"ok": True, "files": files})


@app.route("/offline/session/<name>/merged/<date>/table")
def offline_merged_table(name, date):
    if not _valid_session_name(name):
        return jsonify({"ok": False, "error": "Invalid session name."}), 400
    if not re.match(r"^\d{8}$", date):
        return jsonify({"ok": False, "error": "Invalid date."}), 400
    relpath = (request.args.get("file") or "").strip()
    try:
        page = max(1, int(request.args.get("page", "1")))
    except ValueError:
        page = 1

    merged_dir = _session_merged_dir(name, date)
    raw, err = _read_local_csv(merged_dir, relpath)
    if err:
        return err

    all_rows = list(csv.reader(io.StringIO(raw)))
    if not all_rows:
        return jsonify({
            "ok": True, "file": relpath,
            "columns": [], "column_numeric": [], "rows": [],
            "total_rows": 0, "page": 1, "page_size": DATA_VIEWER_PAGE_SIZE, "total_pages": 1,
        })

    columns = all_rows[0]
    data_rows = all_rows[1:]
    total_rows = len(data_rows)
    total_pages = max(1, math.ceil(total_rows / DATA_VIEWER_PAGE_SIZE))
    page = min(page, total_pages)
    start = (page - 1) * DATA_VIEWER_PAGE_SIZE
    page_rows = data_rows[start:start + DATA_VIEWER_PAGE_SIZE]

    return jsonify({
        "ok": True,
        "file": relpath,
        "columns": columns,
        "column_numeric": _compute_column_numeric(columns, data_rows),
        "rows": page_rows,
        "total_rows": total_rows,
        "page": page,
        "page_size": DATA_VIEWER_PAGE_SIZE,
        "total_pages": total_pages,
    })


@app.route("/offline/session/<name>/merged/<date>/plot_multi")
def offline_merged_plot_multi(name, date):
    if not _valid_session_name(name):
        return jsonify({"ok": False, "error": "Invalid session name."}), 400
    if not re.match(r"^\d{8}$", date):
        return jsonify({"ok": False, "error": "Invalid date."}), 400
    source = (request.args.get("source") or "merged").strip()
    relpath = (request.args.get("file") or "").strip()
    columns = [c for c in (request.args.get("columns") or "").split(",") if c]

    base_dir = _offline_source_dir(name, source, date)
    raw, err = _read_local_csv(base_dir, relpath)
    if err:
        return err
    if not columns:
        return jsonify({"ok": False, "error": "At least one column is required."}), 400

    reader = csv.DictReader(io.StringIO(raw))
    fieldnames = reader.fieldnames or []
    for column in columns:
        if column not in fieldnames:
            return jsonify({"ok": False, "error": f'Column "{column}" not found in this file.'}), 400
    time_column = fieldnames[0]

    series_points = {column: [] for column in columns}
    series_decimals = {column: 0 for column in columns}
    for row in reader:
        ts = row.get(time_column)
        if not ts:
            continue
        for column in columns:
            raw_val = (row.get(column) or "").strip()
            if not raw_val:
                continue
            try:
                value = float(raw_val)
            except ValueError:
                continue
            series_points[column].append({"t": ts, "v": value})
            if "." in raw_val:
                series_decimals[column] = max(series_decimals[column], min(4, len(raw_val.split(".")[-1])))

    return jsonify({
        "ok": True,
        "file": relpath,
        "time_column": time_column,
        "series": [
            {"column": column, "points": series_points[column], "decimals": series_decimals[column]}
            for column in columns
        ],
    })


# ---------- Offline: deeper exploration of a merged day (stats, correlation, ----------
# ---------- histogram, scatter) ----------
# All four read the same merged-day CSVs as /plot_multi above, just
# summarized differently. No numpy/pandas dependency in this project, so
# the stats/correlation math below is plain stdlib.

def _parse_numeric(raw_val):
    """
    Shared float-parse for these four endpoints - same rule as
    _compute_column_numeric's per-value check (guards against PEP 515's
    "_" digit-group separator turning a run-folder-name-shaped value like
    "20260813_151443", e.g. the merged CSVs' own _source_run column, into
    a number). Returns None for blank/non-numeric values instead of
    raising.
    """
    val = (raw_val or "").strip()
    if not val or "_" in val:
        return None
    try:
        return float(val)
    except ValueError:
        return None


def _values_by_column(raw, columns):
    """
    Parses already-fetched CSV text and returns ({column: [values...]},
    None) for the requested columns - blank/non-numeric cells simply
    excluded from that column's list, independently per column (so each
    column's list only reflects rows where *that* column had a value,
    same as /plot_multi). Only needs each column's own value
    distribution, not row-for-row alignment across columns (unlike
    correlation, which needs pairs from the same row and reads rows
    itself - see _correlation_matrix_from_raw). Split out from
    _read_merged_columns so both the local-disk and SFTP-backed callers
    can share it without a second read of their own source.

    Returns (None, error_response) if a requested column doesn't exist.
    """
    reader = csv.DictReader(io.StringIO(raw))
    fieldnames = reader.fieldnames or []
    for column in columns:
        if column not in fieldnames:
            return None, (jsonify({"ok": False, "error": f'Column "{column}" not found in this file.'}), 400)

    values = {column: [] for column in columns}
    for row in reader:
        for column in columns:
            v = _parse_numeric(row.get(column))
            if v is not None:
                values[column].append(v)
    return values, None


def _read_merged_columns(base_dir, relpath, columns):
    """
    Reads one local CSV once and extracts the requested columns' values
    via _values_by_column - the shared row source for /stats and
    /histogram. See _values_by_column for the value-extraction
    semantics.

    Returns (None, error_response) if the file can't be read or a
    requested column doesn't exist.
    """
    raw, err = _read_local_csv(base_dir, relpath)
    if err:
        return None, err
    return _values_by_column(raw, columns)


def _pearson(xs, ys):
    """
    Pearson correlation coefficient between two equal-length lists,
    computed by hand (no numpy/scipy in this project). Returns (None, n)
    if there are fewer than 2 points or either series is constant (zero
    variance has no meaningful correlation, and would divide by zero
    here).
    """
    n = len(xs)
    if n < 2:
        return None, n
    mean_x = sum(xs) / n
    mean_y = sum(ys) / n
    cov = sum((x - mean_x) * (y - mean_y) for x, y in zip(xs, ys))
    var_x = sum((x - mean_x) ** 2 for x in xs)
    var_y = sum((y - mean_y) ** 2 for y in ys)
    if var_x == 0 or var_y == 0:
        return None, n
    r = cov / math.sqrt(var_x * var_y)
    return max(-1.0, min(1.0, r)), n


def _stats_from_values(values, columns):
    """
    Summary statistics (count/mean/std/min/p25/median/p75/max) for each
    column, given its already-extracted values - a hand-rolled
    describe(), since this project has no pandas. Split out from
    _stats_for_columns so the Data Viewer's SFTP-backed assistant
    context can reuse the same computation on values it fetched itself,
    without a second read of its own source.
    """
    stats = []
    for column in columns:
        data = sorted(values[column])
        n = len(data)
        if n == 0:
            stats.append({
                "column": column, "count": 0, "mean": None, "std": None,
                "min": None, "p25": None, "median": None, "p75": None, "max": None,
            })
            continue
        if n >= 2:
            std = statistics.stdev(data)
            p25, median, p75 = statistics.quantiles(data, n=4, method="inclusive")
        else:
            std = None
            p25 = median = p75 = data[0]
        stats.append({
            "column": column, "count": n, "mean": statistics.mean(data), "std": std,
            "min": data[0], "p25": p25, "median": median, "p75": p75, "max": data[-1],
        })
    return stats


def _stats_for_columns(base_dir, relpath, columns):
    """
    Summary statistics for each column of a local CSV. Shared by /stats
    and the offline assistant's data context (below), so both describe a
    selection the same way.

    Returns (stats_list, None) on success, or (None, error_response).
    """
    values, err = _read_merged_columns(base_dir, relpath, columns)
    if err:
        return None, err
    return _stats_from_values(values, columns), None


def _correlation_matrix_from_raw(raw, columns):
    """
    Pairwise Pearson correlation between every pair of columns, parsed
    from already-fetched CSV text. Pairwise-deleted (each pair uses only
    the rows where both of that pair's columns have a value), not
    listwise - merged_data's columns come from independently-sampled
    instruments and are rarely all populated on the same row, so
    requiring every requested column at once would throw away most of
    the data; each pair still only ever compares values genuinely read
    off the same row. Split out from _correlation_matrix_for_columns so
    the Data Viewer's SFTP-backed assistant context can reuse it on text
    it fetched itself, without a second read of its own source.

    Returns (matrix_list, None) on success, or (None, error_response).
    """
    reader = csv.DictReader(io.StringIO(raw))
    fieldnames = reader.fieldnames or []
    for column in columns:
        if column not in fieldnames:
            return None, (jsonify({"ok": False, "error": f'Column "{column}" not found in this file.'}), 400)

    # Every requested column's parsed value per row, rows kept aligned -
    # unlike _values_by_column, which drops each column's blanks
    # independently and so loses the row alignment pairs need.
    rows = [{c: _parse_numeric(row.get(c)) for c in columns} for row in reader]

    matrix = []
    for i, a in enumerate(columns):
        for b in columns[i:]:
            xs, ys = [], []
            for row in rows:
                if row[a] is not None and row[b] is not None:
                    xs.append(row[a])
                    ys.append(row[b])
            r, n = _pearson(xs, ys)
            matrix.append({"a": a, "b": b, "r": r, "n": n})

    return matrix, None


def _correlation_matrix_for_columns(base_dir, relpath, columns):
    """
    Pairwise Pearson correlation for a local CSV's columns. Shared by
    /correlation and the offline assistant's data context (below).

    Returns (matrix_list, None) on success, or (None, error_response).
    """
    raw, err = _read_local_csv(base_dir, relpath)
    if err:
        return None, err
    return _correlation_matrix_from_raw(raw, columns)


@app.route("/offline/session/<name>/merged/<date>/stats")
def offline_merged_stats(name, date):
    if not _valid_session_name(name):
        return jsonify({"ok": False, "error": "Invalid session name."}), 400
    if not re.match(r"^\d{8}$", date):
        return jsonify({"ok": False, "error": "Invalid date."}), 400
    source = (request.args.get("source") or "merged").strip()
    relpath = (request.args.get("file") or "").strip()
    columns = [c for c in (request.args.get("columns") or "").split(",") if c]
    if not columns:
        return jsonify({"ok": False, "error": "At least one column is required."}), 400

    base_dir = _offline_source_dir(name, source, date)
    stats, err = _stats_for_columns(base_dir, relpath, columns)
    if err:
        return err
    return jsonify({"ok": True, "file": relpath, "stats": stats})


@app.route("/offline/session/<name>/merged/<date>/correlation")
def offline_merged_correlation(name, date):
    if not _valid_session_name(name):
        return jsonify({"ok": False, "error": "Invalid session name."}), 400
    if not re.match(r"^\d{8}$", date):
        return jsonify({"ok": False, "error": "Invalid date."}), 400
    source = (request.args.get("source") or "merged").strip()
    relpath = (request.args.get("file") or "").strip()
    columns = [c for c in (request.args.get("columns") or "").split(",") if c]
    if len(columns) < 2:
        return jsonify({"ok": False, "error": "At least two columns are required."}), 400

    base_dir = _offline_source_dir(name, source, date)
    matrix, err = _correlation_matrix_for_columns(base_dir, relpath, columns)
    if err:
        return err
    return jsonify({"ok": True, "file": relpath, "matrix": matrix})


@app.route("/offline/session/<name>/merged/<date>/histogram")
def offline_merged_histogram(name, date):
    """
    Equal-width histogram (bin edges + counts) for one column of one
    merged-day file - the distribution view alongside /stats and
    /correlation's summary numbers.
    """
    if not _valid_session_name(name):
        return jsonify({"ok": False, "error": "Invalid session name."}), 400
    if not re.match(r"^\d{8}$", date):
        return jsonify({"ok": False, "error": "Invalid date."}), 400
    source = (request.args.get("source") or "merged").strip()
    relpath = (request.args.get("file") or "").strip()
    column = (request.args.get("column") or "").strip()
    try:
        bin_count = max(1, min(100, int(request.args.get("bins", "20"))))
    except ValueError:
        bin_count = 20
    if not column:
        return jsonify({"ok": False, "error": "A column is required."}), 400

    base_dir = _offline_source_dir(name, source, date)
    values, err = _read_merged_columns(base_dir, relpath, [column])
    if err:
        return err

    data = values[column]
    if not data:
        return jsonify({"ok": True, "file": relpath, "column": column, "edges": [], "counts": [], "n": 0})

    lo, hi = min(data), max(data)
    if lo == hi:
        lo -= 0.5
        hi += 0.5
    width = (hi - lo) / bin_count
    counts = [0] * bin_count
    for v in data:
        idx = int((v - lo) / width)
        if idx >= bin_count:
            idx = bin_count - 1  # the max value itself would otherwise land one past the last edge
        counts[idx] += 1
    edges = [lo + i * width for i in range(bin_count + 1)]

    return jsonify({"ok": True, "file": relpath, "column": column, "edges": edges, "counts": counts, "n": len(data)})


@app.route("/offline/session/<name>/merged/<date>/scatter")
def offline_merged_scatter(name, date):
    """
    Paired (x, y) values - each pair read off the same row, so genuinely
    simultaneous readings - for two columns of one merged-day file, plus
    their least-squares regression line and Pearson r. The scatter
    counterpart to /correlation's single number; reads the file itself
    (like /correlation) rather than using _read_merged_columns, since it
    needs both raw values kept row-aligned rather than each column's
    blanks dropped separately.
    """
    if not _valid_session_name(name):
        return jsonify({"ok": False, "error": "Invalid session name."}), 400
    if not re.match(r"^\d{8}$", date):
        return jsonify({"ok": False, "error": "Invalid date."}), 400
    source = (request.args.get("source") or "merged").strip()
    relpath = (request.args.get("file") or "").strip()
    x_column = (request.args.get("x") or "").strip()
    y_column = (request.args.get("y") or "").strip()
    if not x_column or not y_column:
        return jsonify({"ok": False, "error": "Both x and y columns are required."}), 400

    base_dir = _offline_source_dir(name, source, date)
    raw, err = _read_local_csv(base_dir, relpath)
    if err:
        return err

    reader = csv.DictReader(io.StringIO(raw))
    fieldnames = reader.fieldnames or []
    for column in (x_column, y_column):
        if column not in fieldnames:
            return jsonify({"ok": False, "error": f'Column "{column}" not found in this file.'}), 400

    points = []
    for row in reader:
        x = _parse_numeric(row.get(x_column))
        y = _parse_numeric(row.get(y_column))
        if x is not None and y is not None:
            points.append({"x": x, "y": y})

    xs = [p["x"] for p in points]
    ys = [p["y"] for p in points]
    r, n = _pearson(xs, ys)

    regression = None
    if n >= 2:
        mean_x = sum(xs) / n
        mean_y = sum(ys) / n
        var_x = sum((x - mean_x) ** 2 for x in xs)
        if var_x > 0:
            slope = sum((x - mean_x) * (y - mean_y) for x, y in zip(xs, ys)) / var_x
            regression = {"slope": slope, "intercept": mean_y - slope * mean_x}

    return jsonify({
        "ok": True, "file": relpath, "x_column": x_column, "y_column": y_column,
        "points": points, "n": n, "r": r,
        "r_squared": (r * r) if r is not None else None,
        "regression": regression,
    })


# ---------- Offline: per-flight altitude profiles ----------
# One flight's instrument readings plotted against the drone's own
# altitude_agl_m, rather than against time. Two problems this solves that
# the plot/scatter endpoints above don't: (1) a day's merged drone
# telemetry is just every flight concatenated (see _merge_drone_day) with
# no per-flight boundaries recorded, so where one flight ends and the
# next begins has to be re-derived from the altitude trace itself; (2) a
# flight's altitude readings and the day's sensor readings are two files
# on independent timebases (sensors sample roughly every second in normal
# operation but the merged sensor CSV can have multi-minute gaps between
# runs - see _merge_day), so they need a nearest-timestamp join rather
# than the same-row pairing /scatter above does within one file.

FLIGHT_TAKEOFF_ALTITUDE_M = 2.0
FLIGHT_LANDING_ALTITUDE_M = 0.5
FLIGHT_LEAD_IN_SECONDS = 120
FLIGHT_TRAIL_OUT_SECONDS = 120
ALTITUDE_JOIN_MAX_GAP_SECONDS = 40


def _parse_csv_timestamp(raw):
    try:
        return datetime.strptime((raw or "").strip(), "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return None


def _read_drone_altitude_series(session_name, date):
    """
    (datetime, altitude_agl_m) pairs for one date's merged drone
    telemetry, sorted by time. Reads the merged file rather than the raw
    per-flight files because flight detection needs one continuous,
    chronologically-ordered stream across the whole day - _merge_drone_day
    already did that concatenation once, no reason to redo it here.

    Returns (None, error_response) if this date's drone flights haven't
    been merged yet (see _merge_drone_day) or have no altitude_agl_m
    column.
    """
    merged_dir = _drone_day_merged_dir_if_present(session_name, date)
    if not merged_dir:
        return None, (jsonify({"ok": False, "error": "This date's drone flights haven't been merged yet - use \"Merge drone\" first."}), 400)

    csv_path = os.path.join(merged_dir, f"telemetry_{date}_merged.csv")
    try:
        with open(csv_path, "r", encoding="utf-8", errors="replace") as f:
            reader = csv.DictReader(f)
            fieldnames = reader.fieldnames or []
            if "altitude_agl_m" not in fieldnames:
                return None, (jsonify({"ok": False, "error": "No altitude_agl_m column in this date's drone telemetry."}), 400)
            time_column = fieldnames[0]
            rows = []
            for row in reader:
                ts = _parse_csv_timestamp(row.get(time_column))
                alt = _parse_numeric(row.get("altitude_agl_m"))
                if ts is not None and alt is not None:
                    rows.append((ts, alt))
    except Exception as e:
        return None, (jsonify({"ok": False, "error": f"Could not read this date's drone telemetry: {e}"}), 500)

    rows.sort(key=lambda r: r[0])
    return rows, None


def _detect_flights(rows):
    """
    Splits one day's continuous (timestamp, altitude_agl_m) stream into
    individual flights, using altitude alone rather than file boundaries -
    a day's merged drone telemetry is a plain concatenation of whatever
    files existed (see _merge_drone_day), and one recorded file can hold
    more than one takeoff/landing.

    A flight starts once altitude climbs to FLIGHT_TAKEOFF_ALTITUDE_M,
    backdated by FLIGHT_LEAD_IN_SECONDS so some ground time right before
    takeoff is included. It ends FLIGHT_TRAIL_OUT_SECONDS after altitude
    drops to and *stays* at or below FLIGHT_LANDING_ALTITUDE_M - climbing
    back above that landing threshold before the wait is up resets the
    timer, so a brief mid-flight dip doesn't cut the flight short. No
    separate noise band is applied on top of these two thresholds: ground
    readings observed in this project's real telemetry sit dead flat
    (e.g. -0.01 m) with no jitter, so a plain crossing is enough.

    Returns [{"start": datetime, "end": datetime}, ...] in time order.
    """
    if not rows:
        return []

    flights = []
    state = "ground"
    takeoff_crossed_at = None
    landing_crossed_at = None
    data_start = rows[0][0]
    data_end = rows[-1][0]

    for ts, alt in rows:
        if state == "ground":
            if alt >= FLIGHT_TAKEOFF_ALTITUDE_M:
                state = "airborne"
                takeoff_crossed_at = ts
                landing_crossed_at = None
        else:
            if alt <= FLIGHT_LANDING_ALTITUDE_M:
                if landing_crossed_at is None:
                    landing_crossed_at = ts
                elif (ts - landing_crossed_at).total_seconds() >= FLIGHT_TRAIL_OUT_SECONDS:
                    start = max(data_start, takeoff_crossed_at - timedelta(seconds=FLIGHT_LEAD_IN_SECONDS))
                    end = min(data_end, landing_crossed_at + timedelta(seconds=FLIGHT_TRAIL_OUT_SECONDS))
                    flights.append({"start": start, "end": end})
                    state = "ground"
                    takeoff_crossed_at = None
                    landing_crossed_at = None
            else:
                landing_crossed_at = None

    # Still airborne when the data runs out (e.g. logging stopped before
    # a landing crossing was ever seen) - show it anyway rather than
    # silently dropping a flight that's simply missing its tail end.
    if state == "airborne" and takeoff_crossed_at is not None:
        start = max(data_start, takeoff_crossed_at - timedelta(seconds=FLIGHT_LEAD_IN_SECONDS))
        flights.append({"start": start, "end": data_end})

    return flights


def _nearest_within(sorted_times, sorted_values, query_time, max_gap_seconds):
    """
    Nearest-timestamp lookup into a (sorted_times, sorted_values) series -
    the join between one drone reading and the sensor data at
    /altitude_profile below. Returns (value, gap_seconds) for the closest
    reading, or (None, None) if nothing is within max_gap_seconds - a real
    sensor gap (e.g. between two runs, see _merge_day) that should be
    dropped rather than paired with a stale value that would otherwise
    look like real, unchanging data.
    """
    if not sorted_times:
        return None, None
    idx = bisect.bisect_left(sorted_times, query_time)
    candidates = [i for i in (idx - 1, idx) if 0 <= i < len(sorted_times)]
    best_idx, best_gap = None, None
    for i in candidates:
        gap = abs((sorted_times[i] - query_time).total_seconds())
        if best_gap is None or gap < best_gap:
            best_idx, best_gap = i, gap
    if best_idx is not None and best_gap <= max_gap_seconds:
        return sorted_values[best_idx], best_gap
    return None, None


# The flight plan these profiles are built for: the drone climbs in steps
# and hovers 35-60 s at each level (every 20 m from 0 to 200 m), so the
# natural bins are centred ON the levels - a bin at 60 m collects 50-70 m -
# rather than edges on round numbers. Samples taken while climbing between
# levels are excluded by default (hover_only), since they belong to no level.
ALTITUDE_BIN_SIZE_M = 20.0
ALTITUDE_BIN_SIZES_ALLOWED = (5.0, 10.0, 20.0, 25.0, 50.0)
HOVER_WINDOW_SECONDS = 5          # altitude must stay flat over +- this many seconds ...
HOVER_MAX_RANGE_M = 2.0           # ... to within this many metres to count as hovering
SENSOR_TO_DRONE_MAX_GAP_SECONDS = 5   # telemetry is 1 Hz; a sensor reading further than this from any kept altitude sample is dropped
PROFILE_MAX_RADIUS_M = 60.0       # the profile is over once the drone first gets this far from the launch point (the transit out)


def _bin_altitude_profile(points, bin_size=ALTITUDE_BIN_SIZE_M):
    """
    Groups (x=sensor value, y=altitude) points into bins centred on the
    multiples of bin_size (0, 20, 40, ... m for the default), each spanning
    centre +- bin_size/2, and summarises each bin's x values: mean +- std
    (what the chart draws), plus median / quartiles / min / max for the
    tooltip, where a few spikes would otherwise hide behind the mean.
    n is the number of instrument readings that fell in the bin.

    Returns [{"altitude": centre, "mean", "std", "median", "p25", "p75",
    "min", "max", "n"}, ...] sorted by altitude; empty bins are absent.
    """
    buckets = {}
    for p in points:
        centre = round(p["y"] / bin_size) * bin_size
        buckets.setdefault(centre, []).append(p["x"])

    bins = []
    for centre, values in sorted(buckets.items()):
        data = sorted(values)
        n = len(data)
        if n >= 2:
            p25, median, p75 = statistics.quantiles(data, n=4, method="inclusive")
        else:
            p25 = median = p75 = data[0]
        bins.append({
            "altitude": centre,
            "mean": sum(data) / n,
            "std": statistics.pstdev(data),
            "median": median, "p25": p25, "p75": p75,
            "min": data[0], "max": data[-1],
            "n": n,
        })
    return bins


def _hover_flags(rows, window_s=HOVER_WINDOW_SECONDS, max_range_m=HOVER_MAX_RANGE_M):
    """
    For time-sorted (timestamp, altitude) rows, True where the altitude
    stays within max_range_m over +- window_s - the drone holding a level
    rather than climbing or descending between levels.
    """
    n = len(rows)
    flags = [False] * n
    j0 = 0
    j1 = 0
    for i, (ts, _alt) in enumerate(rows):
        while j0 < n and (ts - rows[j0][0]).total_seconds() > window_s:
            j0 += 1
        while j1 < n and (rows[j1][0] - ts).total_seconds() <= window_s:
            j1 += 1
        window = [a for _, a in rows[j0:j1]]
        flags[i] = (max(window) - min(window)) <= max_range_m
    return flags


def _leg_flags(rows):
    """'up' for samples up to the flight's highest point, 'down' after it."""
    if not rows:
        return []
    i_max = max(range(len(rows)), key=lambda i: rows[i][1])
    return ["up" if i <= i_max else "down" for i in range(len(rows))]


@app.route("/offline/session/<name>/merged/<date>/flights")
def offline_merged_flights(name, date):
    """
    Auto-detected flights for one date - see _detect_flights. Powers the
    altitude-profile picker's flight dropdown in offline.html.
    """
    if not _valid_session_name(name):
        return jsonify({"ok": False, "error": "Invalid session name."}), 400
    if not re.match(r"^\d{8}$", date):
        return jsonify({"ok": False, "error": "Invalid date."}), 400

    rows, err = _read_drone_altitude_series(name, date)
    if err:
        return err

    flights = _detect_flights(rows)
    return jsonify({
        "ok": True,
        "flights": [
            {
                "index": i,
                "start": f["start"].strftime("%Y-%m-%d %H:%M:%S"),
                "end": f["end"].strftime("%Y-%m-%d %H:%M:%S"),
                "duration_s": (f["end"] - f["start"]).total_seconds(),
            }
            for i, f in enumerate(flights)
        ],
    })


@app.route("/offline/session/<name>/merged/<date>/altitude_profile")
def offline_merged_altitude_profile(name, date):
    """
    Pairs one flight's altitude_agl_m readings with one or more sensor
    columns' readings by nearest timestamp (see _nearest_within), then
    summarizes each into altitude bins (see _bin_altitude_profile) for a
    vertical-profile line (mean ± std by altitude) scoped to a single
    flight - one line per requested column, so several can be overlaid on
    one chart. columns is comma-separated (same convention as
    /plot_multi's columns=) and all read from the sensor CSV in one pass,
    so overlaying several variables doesn't re-read the file once per
    variable. flight_index refers to _detect_flights' output for this
    date - re-detected here rather than passed in as a time range, so the
    frontend only ever needs what /flights already gave it.
    """
    if not _valid_session_name(name):
        return jsonify({"ok": False, "error": "Invalid session name."}), 400
    if not re.match(r"^\d{8}$", date):
        return jsonify({"ok": False, "error": "Invalid date."}), 400
    columns = [c for c in (request.args.get("columns") or "").split(",") if c]
    if not columns:
        return jsonify({"ok": False, "error": "At least one column is required."}), 400
    try:
        flight_index = int(request.args.get("flight_index", ""))
    except ValueError:
        return jsonify({"ok": False, "error": "Invalid flight_index."}), 400
    try:
        bin_size = float(request.args.get("bin_size", ALTITUDE_BIN_SIZE_M))
    except ValueError:
        return jsonify({"ok": False, "error": "Invalid bin_size."}), 400
    if bin_size not in ALTITUDE_BIN_SIZES_ALLOWED:
        return jsonify({"ok": False, "error": f"bin_size must be one of {sorted(ALTITUDE_BIN_SIZES_ALLOWED)}."}), 400
    hover_only = (request.args.get("hover_only", "1") or "1").lower() not in ("0", "false", "no")
    leg = (request.args.get("leg") or "both").lower()
    if leg not in ("both", "up", "down"):
        return jsonify({"ok": False, "error": "leg must be both, up or down."}), 400

    track, err = _read_drone_track(name, date)
    if err:
        return err
    flights = _detect_flights([(r[0], r[1]) for r in track])
    if flight_index < 0 or flight_index >= len(flights):
        return jsonify({"ok": False, "error": "That flight was not found for this date."}), 400
    flight = flights[flight_index]
    flight_track = [r for r in track if flight["start"] <= r[0] <= flight["end"]]

    # The profile is the part of the flight flown over the launch point.
    # Once the drone heads out for the linear route (see /transect) it is
    # no longer profiling: that run, flown at one constant altitude, would
    # otherwise count as a long "hover" in one of the bins. So the profile
    # ends the first time the drone is PROFILE_MAX_RADIUS_M from launch.
    profile_truncated = False
    if len(flight_track) >= 10:
        head = flight_track[:10]
        lat0 = sum(r[2] for r in head) / len(head)
        lon0 = sum(r[3] for r in head) / len(head)
        for i, r in enumerate(flight_track):
            if _horizontal_distance_m(r[2], r[3], lat0, lon0) > PROFILE_MAX_RADIUS_M:
                flight_track = flight_track[:i]
                profile_truncated = True
                break
    flight_drone_rows = [(r[0], r[1]) for r in flight_track]

    # Which altitude samples count: hovering ones (unless asked for all),
    # on the requested leg of the flight.
    hover = _hover_flags(flight_drone_rows)
    legs = _leg_flags(flight_drone_rows)
    kept_rows = [
        row for row, is_hover, row_leg in zip(flight_drone_rows, hover, legs)
        if (is_hover or not hover_only) and (leg == "both" or row_leg == leg)
    ]
    drone_times = [r[0] for r in kept_rows]
    drone_alts = [r[1] for r in kept_rows]

    # Read only this flight's window of the day's sensor data.
    sensor_path = os.path.join(_session_merged_dir(name, date), f"merged_data_{date}_merged.csv")
    if not os.path.isfile(sensor_path):
        return jsonify({"ok": False, "error": "This date's sensor runs haven't been merged yet - use \"Merge\" first."}), 400
    try:
        with open(sensor_path, "r", encoding="utf-8", errors="replace") as f:
            reader = csv.DictReader(f)
            fieldnames = reader.fieldnames or []
            for column in columns:
                if column not in fieldnames:
                    return jsonify({"ok": False, "error": f'Column "{column}" not found in this date\'s sensor data.'}), 400
            time_column = fieldnames[0]
            sensor_series = {column: [] for column in columns}
            for row in reader:
                ts = _parse_csv_timestamp(row.get(time_column))
                if ts is None or ts < flight["start"] or ts > flight["end"]:
                    continue
                for column in columns:
                    val = _parse_numeric(row.get(column))
                    if val is not None:
                        sensor_series[column].append((ts, val))
    except Exception as e:
        return jsonify({"ok": False, "error": f"Could not read this date's sensor data: {e}"}), 500

    # Join from the instrument side: each sensor reading gets the altitude
    # the drone was at when it was taken (nearest kept telemetry sample
    # within SENSOR_TO_DRONE_MAX_GAP_SECONDS). Joining from the drone side
    # instead - every 1 Hz altitude sample grabbing its nearest sensor
    # value - repeats a slow instrument's reading many times (an MA200 at a
    # 30 s timebase would count 30 times per value), inflating n and
    # flattening the spread; this way n is honest instrument samples. A
    # reading with no kept altitude within reach was taken while climbing
    # between levels (with hover_only) or in a telemetry gap, and is dropped.
    series = []
    for column in columns:
        points = []
        dropped = 0
        for ts, val in sorted(sensor_series[column], key=lambda r: r[0]):
            alt, _gap_s = _nearest_within(drone_times, drone_alts, ts, SENSOR_TO_DRONE_MAX_GAP_SECONDS)
            if alt is None:
                dropped += 1
                continue
            points.append({"x": val, "y": alt})

        xs = [p["x"] for p in points]
        ys = [p["y"] for p in points]
        r, n = _pearson(xs, ys)
        series.append({
            "column": column,
            "bins": _bin_altitude_profile(points, bin_size),
            "n": n, "dropped": dropped, "r": r,
            "r_squared": (r * r) if r is not None else None,
        })

    return jsonify({
        "ok": True,
        "flight": {
            "index": flight_index,
            "start": flight["start"].strftime("%Y-%m-%d %H:%M:%S"),
            "end": flight["end"].strftime("%Y-%m-%d %H:%M:%S"),
        },
        "series": series,
        "bin_size": bin_size,
        "hover_only": hover_only,
        "leg": leg,
        "flight_seconds": len(flight_drone_rows),
        "kept_seconds": len(kept_rows),
        "profile_end": flight_drone_rows[-1][0].strftime("%Y-%m-%d %H:%M:%S") if flight_drone_rows else None,
        "profile_truncated_at_departure": profile_truncated,
        "max_gap_seconds": SENSOR_TO_DRONE_MAX_GAP_SECONDS,
    })


# ---------- Offline: per-flight linear route (the inbound run) ----------
# After the stepped profile the drone transits out to ~500 m from the launch
# point and then flies a straight run back in at constant altitude; only
# that inbound run is of interest (the outbound transit is not). It is found
# from the telemetry alone: the farthest point from launch starts it, and it
# ends when the drone is back within TRANSECT_END_DISTANCE_M of launch (or on
# the ground). Readings are binned by horizontal distance from launch.

# The route is flown as hover points - six of them, every 100 m from the far
# point back to the launch area, the last one over the launch area itself -
# so like the profile it is binned around those points (bins centred on the
# multiples of the bin size: the 300 m bin spans 250-350 m) and, by default,
# only readings taken while the drone held position count.
TRANSECT_MIN_DISTANCE_M = 150.0    # must get at least this far out to count as having a route
TRANSECT_MIN_ALTITUDE_M = 3.0      # ground samples are never part of the route
TRANSECT_ALTITUDE_BAND_M = 8.0     # route samples stay within this of the run's median altitude (drops the descent to land)
TRANSECT_BIN_SIZE_M = 100.0
TRANSECT_BIN_SIZES_ALLOWED = (25.0, 50.0, 100.0)
ROUTE_HOVER_WINDOW_SECONDS = 5     # position must stay put over +- this many seconds ...
ROUTE_HOVER_MAX_RANGE_M = 3.0      # ... to within this many metres to count as a hover point
ROUTE_HOVER_MIN_SECONDS = 15       # shorter pauses are not hover points


def _read_drone_track(session_name, date):
    """
    (datetime, altitude_agl_m, latitude_deg, longitude_deg) for one date's
    merged drone telemetry, sorted by time - _read_drone_altitude_series
    plus position. Returns (None, error_response) when the drone day has
    not been merged or lacks those columns.
    """
    merged_dir = _drone_day_merged_dir_if_present(session_name, date)
    if not merged_dir:
        return None, (jsonify({"ok": False, "error": "This date's drone flights haven't been merged yet - use \"Merge drone\" first."}), 400)
    csv_path = os.path.join(merged_dir, f"telemetry_{date}_merged.csv")
    try:
        with open(csv_path, "r", encoding="utf-8", errors="replace") as f:
            reader = csv.DictReader(f)
            fieldnames = reader.fieldnames or []
            for col in ("altitude_agl_m", "latitude_deg", "longitude_deg"):
                if col not in fieldnames:
                    return None, (jsonify({"ok": False, "error": f"No {col} column in this date's drone telemetry."}), 400)
            time_column = fieldnames[0]
            rows = []
            for row in reader:
                ts = _parse_csv_timestamp(row.get(time_column))
                alt = _parse_numeric(row.get("altitude_agl_m"))
                lat = _parse_numeric(row.get("latitude_deg"))
                lon = _parse_numeric(row.get("longitude_deg"))
                if ts is not None and alt is not None and lat is not None and lon is not None:
                    rows.append((ts, alt, lat, lon))
    except Exception as e:
        return None, (jsonify({"ok": False, "error": f"Could not read this date's drone telemetry: {e}"}), 500)
    rows.sort(key=lambda r: r[0])
    return rows, None


def _horizontal_distance_m(lat, lon, lat0, lon0):
    """Flat-earth distance in metres - fine for the few hundred metres a flight covers."""
    dx = (lon - lon0) * 111320.0 * math.cos(math.radians(lat0))
    dy = (lat - lat0) * 110540.0
    return math.hypot(dx, dy)


def _detect_transect(track_rows):
    """
    Finds the inbound run in one flight's (ts, alt, lat, lon) rows: from the
    farthest point from launch until the drone lands, keeping only samples
    at the run's own altitude (median, +- TRANSECT_ALTITUDE_BAND_M) so the
    descent to land is excluded while the final hover over the launch area
    is kept. Launch = the mean position of the first few on-ground samples.

    Returns {"far_m", "rows": [], ...} with empty rows when the flight never
    got TRANSECT_MIN_DISTANCE_M away (a profile-only flight), else
    {"rows": [(ts, distance_m, alt), ...], "start", "end", "far_m",
    "altitude_m" (the run's median altitude), "home": (lat, lon)}.
    """
    if len(track_rows) < 20:
        return None
    head = track_rows[:10]
    lat0 = sum(r[2] for r in head) / len(head)
    lon0 = sum(r[3] for r in head) / len(head)
    dist = [_horizontal_distance_m(r[2], r[3], lat0, lon0) for r in track_rows]
    i_far = max(range(len(dist)), key=lambda i: dist[i])
    empty = {"far_m": dist[i_far], "rows": [], "start": None, "end": None, "home": (lat0, lon0), "altitude_m": None}
    if dist[i_far] < TRANSECT_MIN_DISTANCE_M:
        return empty
    # The drone can drift outward during the far hover, putting the single
    # farthest sample at the END of that hover; start the run where the
    # drone first came within hover range of the far point instead, so the
    # whole first hover point is included.
    i_start = i_far
    while i_start > 0 and dist[i_start - 1] >= dist[i_far] - ROUTE_HOVER_MAX_RANGE_M * 2             and (track_rows[i_far][0] - track_rows[i_start - 1][0]).total_seconds() <= 180:
        i_start -= 1
    i_end = len(track_rows)
    for i in range(i_far + 1, len(track_rows)):
        if track_rows[i][1] < TRANSECT_MIN_ALTITUDE_M:
            i_end = i
            break
    airborne = [(track_rows[i][0], dist[i], track_rows[i][1]) for i in range(i_start, i_end)
                if track_rows[i][1] >= TRANSECT_MIN_ALTITUDE_M]
    if len(airborne) < 10:
        return empty
    route_alt = statistics.median(r[2] for r in airborne)
    rows = [r for r in airborne if abs(r[2] - route_alt) <= TRANSECT_ALTITUDE_BAND_M]
    if len(rows) < 10:
        return empty
    return {"rows": rows, "start": rows[0][0], "end": rows[-1][0], "far_m": dist[i_far],
            "altitude_m": route_alt, "home": (lat0, lon0)}


def _route_hover_points(rows, flags):
    """
    Groups consecutive hovering route samples into hover points: [{"distance_m",
    "seconds", "start"}] for every pause of at least ROUTE_HOVER_MIN_SECONDS.
    rows are (ts, distance, alt); flags the matching hover booleans.
    """
    points = []
    i = 0
    n = len(rows)
    while i < n:
        if not flags[i]:
            i += 1
            continue
        j = i
        while j + 1 < n and flags[j + 1] and (rows[j + 1][0] - rows[j][0]).total_seconds() <= 3:
            j += 1
        seconds = (rows[j][0] - rows[i][0]).total_seconds() + 1
        if seconds >= ROUTE_HOVER_MIN_SECONDS:
            points.append({
                "distance_m": sum(r[1] for r in rows[i:j + 1]) / (j - i + 1),
                "seconds": seconds,
                "start": rows[i][0].strftime("%Y-%m-%d %H:%M:%S"),
            })
        i = j + 1
    return points


def _summarize_values(values):
    data = sorted(values)
    n = len(data)
    if n >= 2:
        p25, median, p75 = statistics.quantiles(data, n=4, method="inclusive")
    else:
        p25 = median = p75 = data[0]
    return {"mean": sum(data) / n, "std": statistics.pstdev(data), "median": median,
            "p25": p25, "p75": p75, "min": data[0], "max": data[-1], "n": n}


def _bin_by_distance(points, bin_size):
    """
    Bins centred on the multiples of bin_size along the route (0, 100, 200,
    ... m for the default), each spanning centre +- bin_size/2, so a hover
    point at 300 m collects 250-350 m; same summary as the profile bins.
    points: [{"x": distance_m, "v": value}].
    """
    buckets = {}
    for p in points:
        buckets.setdefault(round(p["x"] / bin_size) * bin_size, []).append(p["v"])
    bins = []
    for centre, values in sorted(buckets.items()):
        summary = _summarize_values(values)
        summary.update({"distance": centre, "from": max(0.0, centre - bin_size / 2), "to": centre + bin_size / 2})
        bins.append(summary)
    return bins


@app.route("/offline/session/<name>/merged/<date>/transect")
def offline_merged_transect(name, date):
    """
    One flight's inbound linear run: sensor readings against horizontal
    distance from the launch point, binned every bin_size metres. Same
    flight indexing as /flights and /altitude_profile, same instrument-side
    join (each reading tagged with the drone position at its own time,
    nearest telemetry within SENSOR_TO_DRONE_MAX_GAP_SECONDS).
    """
    if not _valid_session_name(name):
        return jsonify({"ok": False, "error": "Invalid session name."}), 400
    if not re.match(r"^\d{8}$", date):
        return jsonify({"ok": False, "error": "Invalid date."}), 400
    columns = [c for c in (request.args.get("columns") or "").split(",") if c]
    if not columns:
        return jsonify({"ok": False, "error": "At least one column is required."}), 400
    try:
        flight_index = int(request.args.get("flight_index", ""))
    except ValueError:
        return jsonify({"ok": False, "error": "Invalid flight_index."}), 400
    try:
        bin_size = float(request.args.get("bin_size", TRANSECT_BIN_SIZE_M))
    except ValueError:
        return jsonify({"ok": False, "error": "Invalid bin_size."}), 400
    if bin_size not in TRANSECT_BIN_SIZES_ALLOWED:
        return jsonify({"ok": False, "error": f"bin_size must be one of {sorted(TRANSECT_BIN_SIZES_ALLOWED)}."}), 400
    hover_only = (request.args.get("hover_only", "1") or "1").lower() not in ("0", "false", "no")

    track, err = _read_drone_track(name, date)
    if err:
        return err
    flights = _detect_flights([(r[0], r[1]) for r in track])
    if flight_index < 0 or flight_index >= len(flights):
        return jsonify({"ok": False, "error": "That flight was not found for this date."}), 400
    flight = flights[flight_index]
    flight_rows = [r for r in track if flight["start"] <= r[0] <= flight["end"]]
    flight_info = {
        "index": flight_index,
        "start": flight["start"].strftime("%Y-%m-%d %H:%M:%S"),
        "end": flight["end"].strftime("%Y-%m-%d %H:%M:%S"),
    }

    transect = _detect_transect(flight_rows)
    if transect is None or not transect["rows"]:
        far = transect["far_m"] if transect else 0.0
        return jsonify({
            "ok": True, "flight": flight_info, "transect": None, "bin_size": bin_size, "series": [],
            "reason": f"No linear route in this flight: it got {far:.0f} m from the launch point at most "
                      f"(a route needs {TRANSECT_MIN_DISTANCE_M:.0f} m).",
        })

    all_rows = transect["rows"]
    # Hovering = horizontal position holding still (same test as the
    # profile's altitude hover, applied to distance from launch).
    hover = _hover_flags([(r[0], r[1]) for r in all_rows], ROUTE_HOVER_WINDOW_SECONDS, ROUTE_HOVER_MAX_RANGE_M)
    hover_points = _route_hover_points(all_rows, hover)
    t_rows = [r for r, h in zip(all_rows, hover) if h or not hover_only]
    if len(t_rows) < 2:
        t_rows = all_rows
    t_times = [r[0] for r in t_rows]
    t_dist = [r[1] for r in t_rows]
    alts = [r[2] for r in all_rows]
    duration_s = max(1.0, (all_rows[-1][0] - all_rows[0][0]).total_seconds())

    sensor_path = os.path.join(_session_merged_dir(name, date), f"merged_data_{date}_merged.csv")
    if not os.path.isfile(sensor_path):
        return jsonify({"ok": False, "error": "This date's sensor runs haven't been merged yet - use \"Merge\" first."}), 400
    try:
        with open(sensor_path, "r", encoding="utf-8", errors="replace") as f:
            reader = csv.DictReader(f)
            fieldnames = reader.fieldnames or []
            for column in columns:
                if column not in fieldnames:
                    return jsonify({"ok": False, "error": f'Column "{column}" not found in this date\'s sensor data.'}), 400
            time_column = fieldnames[0]
            sensor_series = {column: [] for column in columns}
            for row in reader:
                ts = _parse_csv_timestamp(row.get(time_column))
                if ts is None or ts < transect["start"] or ts > transect["end"]:
                    continue
                for column in columns:
                    val = _parse_numeric(row.get(column))
                    if val is not None:
                        sensor_series[column].append((ts, val))
    except Exception as e:
        return jsonify({"ok": False, "error": f"Could not read this date's sensor data: {e}"}), 500

    series = []
    for column in columns:
        points = []
        dropped = 0
        for ts, val in sorted(sensor_series[column], key=lambda r: r[0]):
            d, _gap = _nearest_within(t_times, t_dist, ts, SENSOR_TO_DRONE_MAX_GAP_SECONDS)
            if d is None:
                dropped += 1
                continue
            points.append({"x": d, "v": val})
        series.append({"column": column, "bins": _bin_by_distance(points, bin_size), "n": len(points), "dropped": dropped})

    return jsonify({
        "ok": True,
        "flight": flight_info,
        "transect": {
            "start": all_rows[0][0].strftime("%Y-%m-%d %H:%M:%S"),
            "end": all_rows[-1][0].strftime("%Y-%m-%d %H:%M:%S"),
            "duration_s": duration_s,
            "far_m": transect["far_m"],
            "end_m": all_rows[-1][1],
            "altitude_mean_m": sum(alts) / len(alts),
            "altitude_min_m": min(alts),
            "altitude_max_m": max(alts),
            "speed_mean_ms": (all_rows[0][1] - all_rows[-1][1]) / duration_s,
            "samples": len(all_rows),
            "hover_seconds": sum(1 for h in hover if h),
            "hover_points": hover_points,
        },
        "bin_size": bin_size,
        "hover_only": hover_only,
        "series": series,
        "max_gap_seconds": SENSOR_TO_DRONE_MAX_GAP_SECONDS,
    })


# ---------- Offline: a Claude-backed assistant scoped to one day's data ----------
# A small chat endpoint for the offline exploration page - grounded in
# the same statistics and correlations /stats and /correlation already
# compute, rather than raw data (keeps the prompt small) or general
# knowledge (would let it guess). Stateless like the rest of this app:
# the frontend keeps the conversation history in memory and resends it
# with every question.

ASSISTANT_MODEL = "claude-opus-5-5"
# Opus 5.5 can decline a request on safety grounds (stop_reason "refusal");
# "default" server-side fallbacks re-run such a request on a substitute
# model inside the same call instead of returning nothing. Needs the beta
# messages endpoint plus this beta flag.
ASSISTANT_BETAS = ["server-side-fallback-2026-07-01"]
ASSISTANT_MAX_TOKENS = 2000
ASSISTANT_TOOL_RESULT_MAX_CHARS = 8000   # a tool result longer than this is cut, with a note

ASSISTANT_SYSTEM_PREAMBLE = (
    "You are a data-analysis assistant built into a local dashboard for a drone-based "
    "air pollution research project. You're helping the operator understand the data "
    "currently on their screen - either a downloaded session's saved data, or whatever's "
    "live on the Pi right now - the summary statistics and correlations below are "
    "computed over the actual data, not estimated. Answer using these numbers; if a "
    "question needs something not shown here (specific data points, visual inspection of "
    "the plot itself) and you have no tool that can get it, say so plainly rather than "
    "guessing. Keep answers concise."
)

# Appended to the system prompt only when tools are offered (the online
# Data Viewer assistant - see _ask_claude_with_tools) - the base
# preamble above is shared with the offline assistant, which has no
# tools and must always defer on anything not already in its context.
ASSISTANT_TOOLS_ADDENDUM = (
    "\n\nYou also have tools to look beyond the variables currently on the plot: "
    "list_instruments to see what files exist for this run, check_instrument_health to "
    "check whether specific files are still being written to and whether their latest "
    "readings look anomalous, and scan_correlations to find strong relationships between "
    "different instruments' readings. Reach for these when the operator asks something "
    "broader than what's currently plotted - e.g. whether sensors are running properly, "
    "whether anything looks unusual, or whether instruments correlate with each other."
)

_anthropic_client = None


def _get_anthropic_client():
    """
    Lazily constructs the Anthropic client, reusing it across requests -
    constructing it eagerly at import time would make the whole app fail
    to start (or crash on first request) if ANTHROPIC_API_KEY isn't set,
    when every other feature in this dashboard works fine without it.

    Checks for credentials explicitly rather than trusting
    anthropic.Anthropic() to raise: the SDK doesn't validate credentials
    at construction time, only once a real request tries to build its
    headers - and a missing key surfaces there as a bare TypeError deep
    in request-building code, not a catchable AnthropicError. Raises
    RuntimeError with a message meant to be shown directly to the user
    if no credentials are configured.
    """
    global _anthropic_client
    if _anthropic_client is None:
        if not os.environ.get("ANTHROPIC_API_KEY") and not os.environ.get("ANTHROPIC_AUTH_TOKEN"):
            raise RuntimeError(
                "No ANTHROPIC_API_KEY is set in this process's environment. "
                "Set it and restart the dashboard to use the assistant."
            )
        _anthropic_client = anthropic.Anthropic()
    return _anthropic_client


def _ask_claude_with_context(context, question, history):
    """
    Shared Claude call for both /assistant/ask (offline exploration) and
    /assistant/ask_online (Data Viewer) - same model, system preamble,
    conversation-history handling, and error mapping; only how each
    endpoint builds its `context` text differs (a downloaded session's
    local files vs. whatever's live on the Pi over SFTP).

    Returns (result_dict, None) on success, or (None, error_response).
    """
    try:
        client = _get_anthropic_client()
    except RuntimeError as e:
        return None, (jsonify({"ok": False, "error": str(e)}), 500)

    messages = [{"role": m["role"], "content": m["content"]} for m in history]
    messages.append({"role": "user", "content": question})

    try:
        response = client.beta.messages.create(
            model=ASSISTANT_MODEL,
            max_tokens=ASSISTANT_MAX_TOKENS,
            output_config={"effort": "medium"},
            betas=ASSISTANT_BETAS,
            fallbacks="default",
            system=f"{ASSISTANT_SYSTEM_PREAMBLE}\n\n{context}",
            messages=messages,
        )
    except anthropic.APIStatusError as e:
        return None, (jsonify({"ok": False, "error": f"Claude API error: {e.message}"}), 502)
    except anthropic.APIConnectionError:
        return None, (jsonify({"ok": False, "error": "Could not reach the Claude API - check your internet connection."}), 502)
    except Exception as e:
        # Catches anything the SDK itself raises outside its own exception
        # hierarchy (e.g. a malformed-credential TypeError) - this call
        # goes out to a third party with failure modes the two branches
        # above don't fully cover, so it gets a safety net this app's
        # local-only endpoints don't need.
        return None, (jsonify({"ok": False, "error": f"Could not reach Claude: {e}"}), 502)

    if response.stop_reason == "refusal":
        return None, (jsonify({"ok": False, "error": "Claude declined to answer that question."}), 200)

    answer = "".join(block.text for block in response.content if block.type == "text")
    return {
        "ok": True,
        "answer": answer,
        "usage": {
            "input_tokens": response.usage.input_tokens,
            "output_tokens": response.usage.output_tokens,
        },
    }, None


def _build_offline_data_context(name, date, columns):
    """
    Renders the currently-selected variables' statistics (and, within
    each file, their pairwise correlation) as plain text for the
    assistant's system prompt - the same numbers /stats and
    /correlation return as JSON, just narrated, so answers are grounded
    in real numbers instead of guesses. columns is
    [{"file","column","source"}, ...], the same shape the offline page's
    activeSeries already is - "source" being "merged" or "drone" per
    entry, since one plot can mix a day's merged sensor data with that
    day's drone telemetry at once (the online Data Viewer's
    _build_online_data_context takes the same shape for the same
    reason).

    A request with nothing selected still gets a valid (if sparse)
    context - the system prompt tells the assistant to say so rather
    than fabricate. Returns (context_text, None), or (None,
    error_response) if a column lookup fails.
    """
    lines = [
        f"Session: {name}",
        f"Date: {date[:4]}-{date[4:6]}-{date[6:]}",
    ]

    if not columns:
        lines.append(
            "\nNo variables are currently added to the plot - the operator hasn't "
            "picked anything to look at yet in this conversation."
        )
        return "\n".join(lines), None

    by_file = {}
    for c in columns:
        by_file.setdefault((c.get("source") or "merged", c["file"]), []).append(c["column"])

    for (source, relpath), cols in by_file.items():
        file_label = "drone telemetry" if source == "drone" else "merged sensor data"
        base_dir = _offline_source_dir(name, source, date)
        stats, err = _stats_for_columns(base_dir, relpath, cols)
        if err:
            return None, err

        lines.append(f"\nVariables from {relpath} ({file_label}):")
        for s in stats:
            if s["count"] == 0:
                lines.append(f"  - {s['column']}: no numeric values found")
                continue
            std_txt = f"{s['std']:.4g}" if s["std"] is not None else "n/a (only one value)"
            lines.append(
                f"  - {s['column']}: n={s['count']}, mean={s['mean']:.4g}, std={std_txt}, "
                f"min={s['min']:.4g}, p25={s['p25']:.4g}, median={s['median']:.4g}, "
                f"p75={s['p75']:.4g}, max={s['max']:.4g}"
            )

        if len(cols) >= 2:
            matrix, err = _correlation_matrix_for_columns(base_dir, relpath, cols)
            if err:
                return None, err
            lines.append(f"\nPairwise correlation (Pearson r) within {relpath}:")
            for m in matrix:
                if m["a"] == m["b"]:
                    continue
                r_txt = f"{m['r']:.3f}" if m["r"] is not None else "undefined (a constant variable)"
                lines.append(f"  - {m['a']} vs {m['b']}: r={r_txt} (n={m['n']})")

    return "\n".join(lines), None



# ---------- Offline assistant tools: the whole merged day, on demand ----------
# The offline assistant used to see only a summary of the plotted variables.
# A merged day (~10k rows x ~200 columns) is far too big to put in a prompt,
# so instead the model gets tools: it asks for a column, a time window, a
# flight's profile or route, and the app computes the answer locally from the
# day's merged CSVs and returns a few lines of text. Every tool reads the
# same files the page's own endpoints read.

ASSISTANT_OFFLINE_ADDENDUM = (
    "\n\nYou also have tools that reach the WHOLE of this day's data, not just the plotted "
    "variables: list_variables (every column, with coverage), describe (statistics for any "
    "columns, optionally in a time window), time_series (a column summarised per interval "
    "over a window), correlate (Pearson r between any columns, sensor and/or drone), "
    "list_flights, vertical_profile (an instrument against altitude for one flight, the hover "
    "levels) and horizontal_profile (an instrument along the inbound linear route of one "
    "flight, the hover points), and raw_rows (a few rows around a moment). Use them whenever "
    "a question goes beyond the summary below; prefer one well-chosen call over many. Times "
    "are local Pi time on this date; say which tool results an answer rests on, and report "
    "numbers as returned. Column names must be exact - call list_variables if unsure."
)

ASSISTANT_OFFLINE_MAX_ITERATIONS = 10

_TIME_WINDOW_PROPS = {
    "start": {"type": "string", "description": "Window start, 'HH:MM' or 'HH:MM:SS' (this date), or 'YYYY-MM-DD HH:MM:SS'. Omit for the start of the day."},
    "end": {"type": "string", "description": "Window end, same formats. Omit for the end of the day."},
}
_SOURCE_PROP = {"type": "string", "enum": ["merged", "drone"],
                "description": "'merged' = the day's merged sensor CSV (default); 'drone' = the day's merged drone telemetry."}

ASSISTANT_OFFLINE_TOOLS = [
    {
        "name": "list_variables",
        "description": ("Every column available for this day, grouped by instrument, with how much of the "
                        "day each column has values (coverage) and the time span of the data. Call this "
                        "to learn exact column names before describe/correlate/time_series."),
        "input_schema": {"type": "object", "properties": {
            "source": {"type": "string", "enum": ["merged", "drone", "both"], "description": "Default 'both'."},
        }},
    },
    {
        "name": "describe",
        "description": ("Statistics for one or more columns: count, coverage, mean, std, min, quartiles, "
                        "max, and the first/last time a value exists - over the whole day or a time window."),
        "input_schema": {"type": "object", "properties": {
            "columns": {"type": "array", "items": {"type": "string"}, "description": "Exact column names."},
            "source": _SOURCE_PROP, **_TIME_WINDOW_PROPS,
        }, "required": ["columns"]},
    },
    {
        "name": "time_series",
        "description": ("One column summarised per interval (mean, min, max, n) across a time window - "
                        "for 'what was X doing between 13:10 and 13:20'. At most 120 intervals; the "
                        "interval is widened automatically if the window needs more."),
        "input_schema": {"type": "object", "properties": {
            "column": {"type": "string"},
            "source": _SOURCE_PROP, **_TIME_WINDOW_PROPS,
            "interval_s": {"type": "integer", "description": "Interval length in seconds (default 60)."},
        }, "required": ["column"]},
    },
    {
        "name": "correlate",
        "description": ("Pearson correlation between columns, pairwise on rows where both have values, "
                        "optionally within a time window. Sensor columns correlate within the merged "
                        "file; drone_columns (e.g. altitude_agl_m) are matched to sensor rows by timestamp."),
        "input_schema": {"type": "object", "properties": {
            "columns": {"type": "array", "items": {"type": "string"}, "description": "Sensor (merged) columns."},
            "drone_columns": {"type": "array", "items": {"type": "string"}, "description": "Drone telemetry columns to include (optional)."},
            **_TIME_WINDOW_PROPS,
        }, "required": ["columns"]},
    },
    {
        "name": "list_flights",
        "description": ("The flights detected in this day's drone telemetry: index, start/end, duration, "
                        "top altitude, how far the drone went from the launch point and whether that "
                        "flight includes a linear route. Flight indexes are used by the profile tools."),
        "input_schema": {"type": "object", "properties": {}},
    },
    {
        "name": "vertical_profile",
        "description": ("One flight's vertical profile: instrument readings binned by altitude around the "
                        "hover levels (default 20 m bins, hovering samples only). Returns per level: mean, "
                        "std, median, quartiles and number of readings."),
        "input_schema": {"type": "object", "properties": {
            "flight_index": {"type": "integer"},
            "columns": {"type": "array", "items": {"type": "string"}, "description": "Sensor (merged) columns."},
            "bin_size": {"type": "number", "enum": [5, 10, 20, 25, 50], "description": "Default 20."},
            "hover_only": {"type": "boolean", "description": "Default true."},
            "leg": {"type": "string", "enum": ["both", "up", "down"], "description": "Default 'both'."},
        }, "required": ["flight_index", "columns"]},
    },
    {
        "name": "horizontal_profile",
        "description": ("One flight's linear route (the inbound run from the far point back to the launch "
                        "area): instrument readings binned by horizontal distance from launch around the "
                        "hover points (default 100 m bins, hovering samples only). Also reports the "
                        "route's altitude and the hover points found."),
        "input_schema": {"type": "object", "properties": {
            "flight_index": {"type": "integer"},
            "columns": {"type": "array", "items": {"type": "string"}, "description": "Sensor (merged) columns."},
            "bin_size": {"type": "number", "enum": [25, 50, 100], "description": "Default 100."},
            "hover_only": {"type": "boolean", "description": "Default true."},
        }, "required": ["flight_index", "columns"]},
    },
    {
        "name": "raw_rows",
        "description": ("The actual rows for a few columns around one moment (at most 60 rows), for "
                        "'what exactly happened at 16:25'."),
        "input_schema": {"type": "object", "properties": {
            "columns": {"type": "array", "items": {"type": "string"}},
            "source": _SOURCE_PROP,
            "time": {"type": "string", "description": "Centre of the window, 'HH:MM[:SS]' or 'YYYY-MM-DD HH:MM:SS'."},
            "seconds": {"type": "integer", "description": "Half-width of the window in seconds (default 30)."},
        }, "required": ["columns", "time"]},
    },
]

_offline_table_cache = {}   # path -> (mtime, size, header, rows, coverage)
_OFFLINE_TABLE_CACHE_MAX = 6


def _offline_day_path(name, date, source):
    if source == "drone":
        merged_dir = _drone_day_merged_dir_if_present(name, date)
        if not merged_dir:
            return None, "This date's drone flights haven't been merged yet - use \"Merge drone\" first."
        return os.path.join(merged_dir, f"telemetry_{date}_merged.csv"), None
    path = os.path.join(_session_merged_dir(name, date), f"merged_data_{date}_merged.csv")
    if not os.path.isfile(path):
        return None, "This date's sensor runs haven't been merged yet - use \"Merge\" first."
    return path, None


def _offline_table(name, date, source):
    """
    (header, rows, coverage, None) for the day's merged sensor or drone CSV,
    parsed once per file version and cached; or (None, None, None, error).
    rows are lists aligned with header; coverage[col] = fraction of rows
    with a value.
    """
    path, err = _offline_day_path(name, date, source)
    if err:
        return None, None, None, err
    st = os.stat(path)
    cached = _offline_table_cache.get(path)
    if cached and cached[0] == st.st_mtime and cached[1] == st.st_size:
        return cached[2], cached[3], cached[4], None
    with open(path, "r", newline="", encoding="utf-8", errors="replace") as f:
        reader = csv.reader(f)
        header = next(reader, [])
        width = len(header)
        rows = [row for row in reader if len(row) == width]
    counts = [0] * width
    for row in rows:
        for i, v in enumerate(row):
            if v:
                counts[i] += 1
    coverage = {header[i]: (counts[i] / len(rows) if rows else 0.0) for i in range(width)}
    while len(_offline_table_cache) >= _OFFLINE_TABLE_CACHE_MAX:
        _offline_table_cache.pop(next(iter(_offline_table_cache)))
    _offline_table_cache[path] = (st.st_mtime, st.st_size, header, rows, coverage)
    return header, rows, coverage, None


def _tool_time(value, date):
    """Normalise a tool-supplied time to 'YYYY-MM-DD HH:MM:SS' (None if blank)."""
    v = (value or "").strip()
    if not v:
        return None
    if re.match(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$", v):
        return v
    if re.match(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}$", v):
        return v + ":00"
    if re.match(r"^\d{1,2}:\d{2}:\d{2}$", v):
        h, m, sec = v.split(":")
        return f"{date[:4]}-{date[4:6]}-{date[6:]} {int(h):02d}:{m}:{sec}"
    if re.match(r"^\d{1,2}:\d{2}$", v):
        h, m = v.split(":")
        return f"{date[:4]}-{date[4:6]}-{date[6:]} {int(h):02d}:{m}:00"
    raise ValueError(f"Unrecognised time {value!r}; use HH:MM, HH:MM:SS or YYYY-MM-DD HH:MM:SS.")


def _rows_between(rows, start, end):
    """Rows whose first field (fixed-width timestamp) lies in [start, end]."""
    if start is None and end is None:
        return rows
    return [r for r in rows if (start is None or r[0][:19] >= start) and (end is None or r[0][:19] <= end)]


def _g(x):
    return "n/a" if x is None else f"{x:.4g}"


def _offline_tool_list_variables(name, date, inp):
    which = (inp.get("source") or "both")
    out = []
    for source in (("merged", "drone") if which == "both" else (which,)):
        header, rows, coverage, err = _offline_table(name, date, source)
        if err:
            out.append(f"[{source}] {err}")
            continue
        span = f"{rows[0][0]} .. {rows[-1][0]}" if rows else "no rows"
        out.append(f"[{source}] {len(rows)} rows, {len(header)} columns, {span}")
        groups = {}
        for col in header[1:]:
            if col == "_source_run":
                continue
            if source == "drone":
                key = "drone"
            else:
                key = col.split("_", 1)[0] if "_" in col else "other"
            groups.setdefault(key, []).append(f"{col} ({coverage[col] * 100:.0f}%)")
        for key, cols in groups.items():
            out.append(f"  {key}: " + ", ".join(cols))
    return "\n".join(out), None


def _offline_tool_describe(name, date, inp):
    source = inp.get("source") or "merged"
    header, rows, coverage, err = _offline_table(name, date, source)
    if err:
        return None, err
    start, end = _tool_time(inp.get("start"), date), _tool_time(inp.get("end"), date)
    sel = _rows_between(rows, start, end)
    cols = [c for c in (inp.get("columns") or []) if c]
    missing = [c for c in cols if c not in header]
    if missing:
        return None, f"Unknown column(s) {missing} in {source}. Call list_variables for exact names."
    window = f"{start or rows[0][0] if rows else '?'} .. {end or rows[-1][0] if rows else '?'}"
    out = [f"[{source}] {len(sel)} rows in {window}"]
    for col in cols:
        idx = header.index(col)
        vals, first, last = [], None, None
        for r in sel:
            v = _parse_numeric(r[idx])
            if v is not None:
                vals.append(v)
                if first is None:
                    first = r[0]
                last = r[0]
        if not vals:
            out.append(f"  {col}: no numeric values in the window")
            continue
        st = _stats_from_values({col: vals}, [col])[0]
        out.append(f"  {col}: n={st['count']} ({len(vals) / max(1, len(sel)) * 100:.0f}% of rows), mean {_g(st['mean'])}, "
                   f"std {_g(st['std'])}, min {_g(st['min'])}, p25 {_g(st['p25'])}, median {_g(st['median'])}, "
                   f"p75 {_g(st['p75'])}, max {_g(st['max'])}; values from {first} to {last}")
    return "\n".join(out), None


def _offline_tool_time_series(name, date, inp):
    source = inp.get("source") or "merged"
    header, rows, _cov, err = _offline_table(name, date, source)
    if err:
        return None, err
    col = (inp.get("column") or "").strip()
    if col not in header:
        return None, f"Unknown column {col!r} in {source}. Call list_variables for exact names."
    start, end = _tool_time(inp.get("start"), date), _tool_time(inp.get("end"), date)
    sel = _rows_between(rows, start, end)
    if not sel:
        return f"No rows between {start} and {end}.", None
    idx = header.index(col)
    t0 = datetime.strptime(sel[0][0][:19], PLOT_TS_FORMAT)
    t1 = datetime.strptime(sel[-1][0][:19], PLOT_TS_FORMAT)
    span = max(1.0, (t1 - t0).total_seconds())
    interval = int(inp.get("interval_s") or 60)
    interval = max(1, interval)
    if span / interval > 120:
        interval = int(math.ceil(span / 120 / 60.0)) * 60
    buckets = {}
    for r in sel:
        v = _parse_numeric(r[idx])
        if v is None:
            continue
        ts = datetime.strptime(r[0][:19], PLOT_TS_FORMAT)
        k = int((ts - t0).total_seconds() // interval)
        buckets.setdefault(k, []).append(v)
    out = [f"[{source}] {col} from {sel[0][0]} to {sel[-1][0]}, {interval}s intervals (start, mean, min, max, n):"]
    for k in sorted(buckets):
        vals = buckets[k]
        t = (t0 + timedelta(seconds=k * interval)).strftime("%H:%M:%S")
        out.append(f"  {t}  {_g(sum(vals) / len(vals))}  {_g(min(vals))}  {_g(max(vals))}  {len(vals)}")
    return "\n".join(out), None


def _offline_tool_correlate(name, date, inp):
    header, rows, _cov, err = _offline_table(name, date, "merged")
    if err:
        return None, err
    start, end = _tool_time(inp.get("start"), date), _tool_time(inp.get("end"), date)
    sel = _rows_between(rows, start, end)
    cols = [c for c in (inp.get("columns") or []) if c]
    dcols = [c for c in (inp.get("drone_columns") or []) if c]
    missing = [c for c in cols if c not in header]
    if missing:
        return None, f"Unknown sensor column(s) {missing}. Call list_variables for exact names."
    series = {}
    for c in cols:
        idx = header.index(c)
        d = {}
        for i, r in enumerate(sel):
            v = _parse_numeric(r[idx])
            if v is not None:
                d[i] = v
        series[c] = d
    if dcols:
        dheader, drows, _dc, derr = _offline_table(name, date, "drone")
        if derr:
            return None, derr
        dmissing = [c for c in dcols if c not in dheader]
        if dmissing:
            return None, f"Unknown drone column(s) {dmissing}."
        by_second = {r[0][:19]: r for r in drows}
        for c in dcols:
            didx = dheader.index(c)
            d = {}
            for i, r in enumerate(sel):
                dr = by_second.get(r[0][:19])
                if dr is None:
                    continue
                v = _parse_numeric(dr[didx])
                if v is not None:
                    d[i] = v
            series["drone:" + c] = d
    names = [c for c in series if len(series[c]) >= 3 and len(set(series[c].values())) > 1]
    if len(names) < 2:
        return "Fewer than two of those columns have enough varying values in the window to correlate.", None
    matrix = _sparse_correlations(series, names)
    out = [f"Pearson r over {len(sel)} rows ({start or 'day start'} .. {end or 'day end'}), pairwise on shared rows:"]
    for m in sorted(matrix, key=lambda m: -abs(m["r"]) if m["r"] is not None else 0):
        r_txt = "n/a" if m["r"] is None else f"{m['r']:.3f}"
        out.append(f"  {m['a']} vs {m['b']}: r={r_txt} (n={m['n']})")
    return "\n".join(out), None


def _offline_tool_list_flights(name, date, inp):
    track, err = _read_drone_track(name, date)
    if err:
        return None, err[0].get_json()["error"]
    flights = _detect_flights([(r[0], r[1]) for r in track])
    if not flights:
        return "No flights detected in this date's drone telemetry.", None
    out = [f"{len(flights)} flight(s) on {date} (index: start-end, duration, top altitude, farthest from launch):"]
    for i, fl in enumerate(flights):
        rows = [r for r in track if fl["start"] <= r[0] <= fl["end"]]
        top = max(r[1] for r in rows) if rows else 0.0
        tr = _detect_transect(rows) if rows else None
        far = tr["far_m"] if tr else 0.0
        has_route = bool(tr and tr["rows"])
        out.append(f"  {i}: {fl['start'].strftime('%H:%M:%S')}-{fl['end'].strftime('%H:%M:%S')}, "
                   f"{(fl['end'] - fl['start']).total_seconds() / 60:.0f} min, top {top:.0f} m, far {far:.0f} m"
                   + (f", linear route at ~{tr['altitude_m']:.0f} m AGL" if has_route else ", no linear route"))
    return "\n".join(out), None


def _offline_internal_get(url):
    """Call one of this app's own GET endpoints in-process and return its JSON."""
    try:
        root = request.args.get("root") or ""
    except RuntimeError:
        root = ""
    if root:
        url += ("&" if "?" in url else "?") + "root=" + root
    with app.test_client() as tc:
        return tc.get(url).get_json() or {}


def _offline_tool_vertical_profile(name, date, inp):
    cols = ",".join(c for c in (inp.get("columns") or []) if c)
    if not cols:
        return None, "columns is required."
    q = (f"/offline/session/{name}/merged/{date}/altitude_profile?columns={cols}"
         f"&flight_index={int(inp.get('flight_index', 0))}&bin_size={inp.get('bin_size') or 20}"
         f"&hover_only={'0' if inp.get('hover_only') is False else '1'}&leg={inp.get('leg') or 'both'}")
    d = _offline_internal_get(q)
    if not d.get("ok"):
        return None, d.get("error", "profile failed")
    out = [f"Flight {d['flight']['index']} ({d['flight']['start'][11:]}-{d['flight']['end'][11:]}), "
           f"{d['bin_size']:.0f} m bins centred on the levels, hover_only={d['hover_only']}, leg={d['leg']}, "
           f"{d['kept_seconds']} s of hovering used" + (", profile ends at departure for the route" if d.get("profile_truncated_at_departure") else "")]
    for s in d["series"]:
        out.append(f"  {s['column']}: {s['n']} readings used, {s['dropped']} in transit dropped")
        for b in s["bins"]:
            out.append(f"    {b['altitude']:.0f} m: mean {_g(b['mean'])} ± {_g(b['std'])}, median {_g(b['median'])} "
                       f"[{_g(b['p25'])}–{_g(b['p75'])}], n={b['n']}")
    return "\n".join(out), None


def _offline_tool_horizontal_profile(name, date, inp):
    cols = ",".join(c for c in (inp.get("columns") or []) if c)
    if not cols:
        return None, "columns is required."
    q = (f"/offline/session/{name}/merged/{date}/transect?columns={cols}"
         f"&flight_index={int(inp.get('flight_index', 0))}&bin_size={inp.get('bin_size') or 100}"
         f"&hover_only={'0' if inp.get('hover_only') is False else '1'}")
    d = _offline_internal_get(q)
    if not d.get("ok"):
        return None, d.get("error", "route failed")
    if not d.get("transect"):
        return d.get("reason", "No linear route in this flight."), None
    t = d["transect"]
    out = [f"Flight {d['flight']['index']} route {t['start'][11:]}-{t['end'][11:]}: {t['far_m']:.0f} -> {t['end_m']:.0f} m from launch "
           f"at {t['altitude_mean_m']:.0f} m AGL ({t['altitude_min_m']:.0f}-{t['altitude_max_m']:.0f}), {t['duration_s'] / 60:.1f} min, "
           f"{d['bin_size']:.0f} m bins centred on hover points, hover_only={d['hover_only']}"]
    hp = t.get("hover_points") or []
    out.append("  hover points: " + (", ".join(f"{h['distance_m']:.0f} m ({h['seconds']:.0f} s)" for h in hp) if hp else "none (continuous run)"))
    for s in d["series"]:
        out.append(f"  {s['column']}: {s['n']} readings used, {s['dropped']} dropped")
        for b in s["bins"]:
            out.append(f"    {b['distance']:.0f} m [{b['from']:.0f}-{b['to']:.0f}]: mean {_g(b['mean'])} ± {_g(b['std'])}, "
                       f"median {_g(b['median'])} [{_g(b['p25'])}–{_g(b['p75'])}], n={b['n']}")
    return "\n".join(out), None


def _offline_tool_raw_rows(name, date, inp):
    source = inp.get("source") or "merged"
    header, rows, _cov, err = _offline_table(name, date, source)
    if err:
        return None, err
    cols = [c for c in (inp.get("columns") or []) if c]
    missing = [c for c in cols if c not in header]
    if missing:
        return None, f"Unknown column(s) {missing} in {source}."
    centre = _tool_time(inp.get("time"), date)
    if not centre:
        return None, "time is required."
    half = max(1, min(int(inp.get("seconds") or 30), 600))
    c = datetime.strptime(centre, PLOT_TS_FORMAT)
    start = (c - timedelta(seconds=half)).strftime(PLOT_TS_FORMAT)
    end = (c + timedelta(seconds=half)).strftime(PLOT_TS_FORMAT)
    sel = _rows_between(rows, start, end)[:60]
    if not sel:
        return f"No rows between {start} and {end}.", None
    idxs = [header.index(c) for c in cols]
    out = [f"[{source}] rows {start} .. {end} (time, " + ", ".join(cols) + "):"]
    for r in sel:
        out.append("  " + r[0][11:19] + "  " + "  ".join(r[i] or "-" for i in idxs))
    return "\n".join(out), None


_OFFLINE_TOOL_IMPL = {
    "list_variables": _offline_tool_list_variables,
    "describe": _offline_tool_describe,
    "time_series": _offline_tool_time_series,
    "correlate": _offline_tool_correlate,
    "list_flights": _offline_tool_list_flights,
    "vertical_profile": _offline_tool_vertical_profile,
    "horizontal_profile": _offline_tool_horizontal_profile,
    "raw_rows": _offline_tool_raw_rows,
}


def _dispatch_offline_tool(name, date, tool, tool_input):
    """Run one offline tool; any failure becomes text the model can explain."""
    impl = _OFFLINE_TOOL_IMPL.get(tool)
    if impl is None:
        return None, f"Unknown tool: {tool}"
    started = time.time()
    try:
        return impl(name, date, tool_input or {})
    except ValueError as e:
        return None, str(e)
    except Exception as e:
        app.logger.exception("offline assistant tool %s failed", tool)
        return None, f"{tool} failed: {type(e).__name__}: {e}"
    finally:
        app.logger.info("offline assistant tool %s took %.1fs", tool, time.time() - started)


@app.route("/assistant/ask", methods=["POST"])
def assistant_ask():
    data = request.get_json() or {}
    name = (data.get("session") or "").strip()
    date = (data.get("date") or "").strip()
    question = (data.get("question") or "").strip()
    columns = data.get("columns") or []
    history = data.get("history") or []

    if not _valid_session_name(name):
        return jsonify({"ok": False, "error": "Invalid session name."}), 400
    if not re.match(r"^\d{8}$", date):
        return jsonify({"ok": False, "error": "Invalid date."}), 400
    if not question:
        return jsonify({"ok": False, "error": "Ask something first."}), 400
    if not isinstance(columns, list) or not all(
        isinstance(c, dict)
        and isinstance(c.get("file"), str)
        and isinstance(c.get("column"), str)
        and c.get("source") in ("merged", "drone")
        for c in columns
    ):
        return jsonify({"ok": False, "error": "Invalid columns."}), 400
    if not isinstance(history, list) or not all(
        isinstance(m, dict) and m.get("role") in ("user", "assistant") and isinstance(m.get("content"), str)
        for m in history
    ):
        return jsonify({"ok": False, "error": "Invalid conversation history."}), 400

    context, err = _build_offline_data_context(name, date, columns)
    if err:
        return err

    result, err = _ask_claude_with_tools(
        context, question, history, ASSISTANT_OFFLINE_TOOLS,
        lambda tool, tool_input: _dispatch_offline_tool(name, date, tool, tool_input),
        ASSISTANT_OFFLINE_ADDENDUM, ASSISTANT_OFFLINE_MAX_ITERATIONS)
    if err:
        return err
    return jsonify(result)


def _build_online_data_context(run_name, columns):
    """
    The Data Viewer tab's counterpart to _build_offline_data_context -
    same idea (narrate each selected variable's statistics, plus
    pairwise correlation within a file, as plain text for the
    assistant's system prompt), just reading over SFTP from whatever's
    live on the Pi right now instead of a downloaded session's local
    files. columns is [{"file","column","source"}, ...] - the same
    shape dvState.activeSeries already is, "source" being "run" or
    "drone" per entry the same way /data_viewer/plot_multi takes it,
    since a Data Viewer plot can mix a run's own sensor columns with
    drone telemetry columns at once.

    Each distinct (source, file) is fetched over SFTP once and reused
    for both its stats and its correlation matrix, rather than once per
    computation - unlike the offline version, a second SFTP round trip
    per file isn't free.

    Returns (context_text, None), or (None, error_response) if a remote
    read fails - most commonly "not connected to remote host".
    """
    lines = [f"Live run: {run_name}" if run_name else "No run currently selected."]

    if not columns:
        lines.append(
            "\nNo variables are currently added to the plot - the operator hasn't "
            "picked anything to look at yet in this conversation."
        )
        return "\n".join(lines), None

    by_file = {}
    for c in columns:
        by_file.setdefault((c.get("source") or "run", c["file"]), []).append(c["column"])

    for (source, relpath), cols in by_file.items():
        file_label = "drone telemetry" if source == "drone" else f"run {run_name}"
        raw, err = _read_remote_csv(run_name, relpath, source=source)
        if err:
            return None, err

        values, err = _values_by_column(raw, cols)
        if err:
            return None, err
        stats = _stats_from_values(values, cols)

        lines.append(f"\nVariables from {relpath} ({file_label}):")
        for s in stats:
            if s["count"] == 0:
                lines.append(f"  - {s['column']}: no numeric values found")
                continue
            std_txt = f"{s['std']:.4g}" if s["std"] is not None else "n/a (only one value)"
            lines.append(
                f"  - {s['column']}: n={s['count']}, mean={s['mean']:.4g}, std={std_txt}, "
                f"min={s['min']:.4g}, p25={s['p25']:.4g}, median={s['median']:.4g}, "
                f"p75={s['p75']:.4g}, max={s['max']:.4g}"
            )

        if len(cols) >= 2:
            matrix, err = _correlation_matrix_from_raw(raw, cols)
            if err:
                return None, err
            lines.append(f"\nPairwise correlation (Pearson r) within {relpath}:")
            for m in matrix:
                if m["a"] == m["b"]:
                    continue
                r_txt = f"{m['r']:.3f}" if m["r"] is not None else "undefined (a constant variable)"
                lines.append(f"  - {m['a']} vs {m['b']}: r={r_txt} (n={m['n']})")

    return "\n".join(lines), None


# ---- Data Viewer assistant tools ----
# Beyond the always-included stats/correlation for whatever's currently
# plotted (_build_online_data_context above), the online assistant can
# also reach for these on demand - "are all sensors running properly",
# "anything unusual", "any strong correlations between instruments" -
# questions that need to look at data beyond what's on screen right
# now. Kept out of the offline assistant: these tools all read live off
# the Pi over the current SSH session, which a downloaded session has
# no equivalent of.

ASSISTANT_ONLINE_TOOLS = [
    {
        "name": "list_instruments",
        "description": (
            "Lists every CSV file available for the currently selected run on "
            "the Pi, plus the latest drone telemetry file if one exists. Call "
            "this first to see what's available before checking health or "
            "scanning for correlations."
        ),
        "input_schema": {"type": "object", "properties": {}},
    },
    {
        "name": "check_instrument_health",
        "description": (
            "Checks whether the given CSV files are still being written to, "
            "and whether their most recent readings look like statistical "
            "outliers or are stuck at a constant value, each compared against "
            "that file's own readings over the last 30 minutes. Get file names from "
            "list_instruments first."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "files": {
                    "type": "array",
                    "description": "Files to check, as returned by list_instruments.",
                    "items": {
                        "type": "object",
                        "properties": {
                            "file": {"type": "string"},
                            "source": {"type": "string", "enum": ["run", "drone"]},
                        },
                        "required": ["file", "source"],
                    },
                },
            },
            "required": ["files"],
        },
    },
    {
        "name": "scan_correlations",
        "description": (
            "Computes pairwise Pearson correlation across every numeric column "
            "in this run's own merged_data CSV, which already aligns readings "
            "from every instrument onto the same rows, and returns only the "
            "strong pairs - use this to spot relationships between different "
            "instruments. Only covers instruments present in the merged_data "
            "file for this run."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "min_r": {
                    "type": "number",
                    "description": "Minimum absolute correlation to report (default 0.6).",
                },
            },
        },
    },
]


def _tool_list_instruments(run_name):
    """
    Tool executor for list_instruments. Unlike the HTTP-endpoint helpers
    elsewhere in this file, tool executors return plain text (fed back
    to Claude as a tool_result) rather than a Flask response - see
    (result_text, error_text) below.
    """
    if not run_name:
        return None, "No run is currently selected in the Data Viewer."

    with state["lock"]:
        if not state["connected"] or not state["sftp"]:
            return None, "Not connected to remote host."
        run_dir = f"{REMOTE_DIR}/{RUN_ALL_SUBDIR}/output/{run_name}"
        files = _list_run_csv_files(state["sftp"], run_dir)
        drone_path = _find_latest_drone_telemetry_csv(state["sftp"])

    lines = [f"- {path} (source=run, {size} bytes)" for path, size in files]
    if drone_path:
        lines.append(f"- {os.path.basename(drone_path)} (source=drone)")
    if not lines:
        return f"No CSV files found for run {run_name}.", None
    return f"Files for run {run_name}:\n" + "\n".join(lines), None


# Both tools below used to read every row of every file they looked at and
# crunch it in pure Python - on a 30-hour run that is ~110k rows x 206
# columns for merged_data alone: minutes per question, gigabytes of row
# dicts, and a bare 500 when it fell over. They now look at the recent past
# only, which is also what "is this instrument healthy right now" means.
ASSISTANT_HEALTH_WINDOW_S = 30 * 60
ASSISTANT_CORR_WINDOW_S = 2 * 3600
ASSISTANT_CORR_MAX_ROWS = 2000

# Columns that are bookkeeping rather than measurements: never worth a
# z-score or a correlation, and they are many (every instrument's own
# timestamp/date/time/status/checksum lands in merged_data).
_NON_MEASUREMENT_COL = re.compile(
    r"(?i)(timestamp|date|time|datum|session|serial|checksum|firmware|version|status|config|optical|_id$|^id$)"
)


def _tool_check_instrument_health(run_name, files):
    """
    Tool executor for check_instrument_health - see
    ASSISTANT_ONLINE_TOOLS for the schema. Reports facts (age since last
    write, any flagged columns over the last ASSISTANT_HEALTH_WINDOW_S)
    rather than a healthy/unhealthy verdict; Claude does the judgment
    call, since this app has no reliable signal for whether the logger
    is even still supposed to be running.
    """
    if not files:
        return None, "No files given - call list_instruments first and pass some of its results here."

    window_min = ASSISTANT_HEALTH_WINDOW_S // 60
    report = []
    for f in files:
        relpath = (f or {}).get("file")
        source = (f or {}).get("source") or "run"
        if not relpath:
            continue

        mtime, err = _remote_csv_mtime(run_name, relpath, source)
        if err:
            report.append(f"{relpath}: could not stat file ({err[0].get_json()['error']})")
            continue
        age_s = time.time() - mtime
        age_txt = f"{age_s / 60:.1f} min ago" if age_s < 3600 else f"{age_s / 3600:.1f} hr ago"

        raw, err = _read_remote_csv_tail(run_name, relpath, source, ASSISTANT_HEALTH_WINDOW_S)
        if err:
            report.append(f"{relpath}: last written {age_txt}; could not read its content ({err[0].get_json()['error']})")
            continue

        reader = csv.DictReader(io.StringIO(raw))
        fieldnames = reader.fieldnames or []
        rows = list(reader)
        candidates = [c for c in fieldnames if not _NON_MEASUREMENT_COL.search(c)]
        values = {c: [] for c in candidates}
        for row in rows:
            for c in candidates:
                v = _parse_numeric(row.get(c))
                if v is not None:
                    values[c].append(v)
        numeric_cols = [c for c in candidates if len(values[c]) >= 2]

        flags = []
        for st in _stats_from_values(values, numeric_cols):
            data = values[st["column"]]
            last_val = data[-1]
            if st["std"]:
                z = (last_val - st["mean"]) / st["std"]
                if abs(z) >= 3:
                    flags.append(
                        f"{st['column']}: latest value {last_val:.4g} is {z:.1f} std devs from its "
                        f"mean over the last {window_min} min ({st['mean']:.4g})"
                    )
            tail = data[-5:]
            if len(tail) >= 5 and len(set(tail)) == 1:
                flags.append(f"{st['column']}: stuck at a constant value ({tail[0]:.4g}) for its last 5 readings")

        line = (f"{relpath} (source={source}): last written {age_txt}, "
                f"{len(rows)} rows in the last {window_min} min")
        line += "\n  - " + "\n  - ".join(flags) if flags else "\n  - no anomalies flagged"
        report.append(line)

    return "\n\n".join(report), None


def _sparse_correlations(series, cols):
    """
    Pairwise-deleted Pearson r for every pair of columns, where each
    column is {row_index: value} holding only the rows it has a value on.
    Iterating the sparser column of each pair and looking the other up
    keeps the cost near sum(min(n_a, n_b)) instead of pairs x rows - the
    30 s MA200 columns pair with the 1 Hz ones in a few hundred steps.
    """
    out = []
    for i, a in enumerate(cols):
        sa = series[a]
        for b in cols[i + 1:]:
            sb = series[b]
            small, big = (sa, sb) if len(sa) <= len(sb) else (sb, sa)
            xs, ys = [], []
            for idx, v in small.items():
                w = big.get(idx)
                if w is not None:
                    xs.append(v)
                    ys.append(w)
            r, n = _pearson(xs, ys)
            out.append({"a": a, "b": b, "r": r, "n": n})
    return out


def _tool_scan_correlations(run_name, min_r):
    """Tool executor for scan_correlations - see ASSISTANT_ONLINE_TOOLS for the schema."""
    if not run_name:
        return None, "No run is currently selected in the Data Viewer."

    with state["lock"]:
        if not state["connected"] or not state["sftp"]:
            return None, "Not connected to remote host."
        run_dir = f"{REMOTE_DIR}/{RUN_ALL_SUBDIR}/output/{run_name}"
        files = _list_run_csv_files(state["sftp"], run_dir)

    merged = [path for path, _ in files if os.path.basename(path).startswith("merged_data_")]
    if not merged:
        return "This run has no merged_data CSV yet, so there's nothing to cross-correlate across instruments.", None
    relpath = merged[0]

    raw, err = _read_remote_csv_tail(run_name, relpath, "run", ASSISTANT_CORR_WINDOW_S)
    if err:
        return None, err[0].get_json()["error"]

    reader = csv.DictReader(io.StringIO(raw))
    fieldnames = reader.fieldnames or []
    rows = list(reader)[-ASSISTANT_CORR_MAX_ROWS:]
    if not rows:
        return f"{relpath} has no rows in the last {ASSISTANT_CORR_WINDOW_S // 3600} h.", None

    series = {}
    for c in fieldnames:
        if _NON_MEASUREMENT_COL.search(c):
            continue
        d = {}
        for i, row in enumerate(rows):
            v = _parse_numeric(row.get(c))
            if v is not None:
                d[i] = v
        if len(d) >= 10 and len(set(d.values())) > 1:   # enough points, not constant
            series[c] = d
    cols = list(series)
    if len(cols) < 2:
        return f"{relpath} doesn't have enough varying numeric columns to correlate.", None

    matrix = _sparse_correlations(series, cols)
    strong = [m for m in matrix if m["r"] is not None and m["n"] >= 10 and abs(m["r"]) >= min_r]
    strong.sort(key=lambda m: -abs(m["r"]))
    span_txt = f"{rows[0].get(fieldnames[0], '?')} .. {rows[-1].get(fieldnames[0], '?')}"
    if not strong:
        return (f"No column pairs in {relpath} have |r| >= {min_r} over the last {len(rows)} merged rows "
                f"({span_txt}; {len(cols)} columns scanned)."), None

    lines = [f"Strong correlations in {relpath} over the last {len(rows)} merged rows "
             f"({span_txt}; {len(cols)} columns scanned, |r| >= {min_r}):"]
    for m in strong[:15]:
        lines.append(f"  - {m['a']} vs {m['b']}: r={m['r']:.3f} (n={m['n']})")
    return "\n".join(lines), None


def _dispatch_assistant_tool(run_name, name, tool_input):
    """
    Executes one Claude-requested tool call by name. Returns
    (result_text, None) on success or (None, error_text) - either way,
    the tool loop below feeds the text back to Claude as the next
    tool_result, so a failure here becomes something Claude can explain
    to the operator rather than an HTTP error.
    """
    tool_input = tool_input or {}
    started = time.time()
    try:
        if name == "list_instruments":
            return _tool_list_instruments(run_name)
        if name == "check_instrument_health":
            return _tool_check_instrument_health(run_name, tool_input.get("files") or [])
        if name == "scan_correlations":
            min_r = tool_input.get("min_r")
            if not isinstance(min_r, (int, float)):
                min_r = 0.6
            return _tool_scan_correlations(run_name, min_r)
        return None, f"Unknown tool: {name}"
    finally:
        app.logger.info("assistant tool %s took %.1fs", name, time.time() - started)


ASSISTANT_MAX_TOOL_ITERATIONS = 6


def _ask_claude_with_tools(context, question, history, tools, dispatch, addendum,
                           max_iterations=ASSISTANT_MAX_TOOL_ITERATIONS):
    """
    Online counterpart to _ask_claude_with_context - same model, system
    preamble, and error handling, but also offers ASSISTANT_ONLINE_TOOLS
    and loops on stop_reason == "tool_use": each requested tool is
    executed against the live SFTP session (_dispatch_assistant_tool)
    and its result fed back as the next turn, until Claude reaches a
    final text answer or ASSISTANT_MAX_TOOL_ITERATIONS is hit (a safety
    cap in case it keeps calling tools instead of answering). Token
    usage is summed across every round trip, not just the last one.

    Returns (result_dict, None) on success, or (None, error_response).
    """
    try:
        client = _get_anthropic_client()
    except RuntimeError as e:
        return None, (jsonify({"ok": False, "error": str(e)}), 500)

    messages = [{"role": m["role"], "content": m["content"]} for m in history]
    messages.append({"role": "user", "content": question})

    total_input_tokens = 0
    total_output_tokens = 0

    for _ in range(max_iterations):
        try:
            response = client.beta.messages.create(
                model=ASSISTANT_MODEL,
                max_tokens=ASSISTANT_MAX_TOKENS,
                output_config={"effort": "medium"},
                betas=ASSISTANT_BETAS,
                fallbacks="default",
                system=f"{ASSISTANT_SYSTEM_PREAMBLE}{addendum}\n\n{context}",
                messages=messages,
                tools=tools,
            )
        except anthropic.APIStatusError as e:
            return None, (jsonify({"ok": False, "error": f"Claude API error: {e.message}"}), 502)
        except anthropic.APIConnectionError:
            return None, (jsonify({"ok": False, "error": "Could not reach the Claude API - check your internet connection."}), 502)
        except Exception as e:
            return None, (jsonify({"ok": False, "error": f"Could not reach Claude: {e}"}), 502)

        total_input_tokens += response.usage.input_tokens
        total_output_tokens += response.usage.output_tokens

        if response.stop_reason == "refusal":
            return None, (jsonify({"ok": False, "error": "Claude declined to answer that question."}), 200)

        if response.stop_reason != "tool_use":
            answer = "".join(block.text for block in response.content if block.type == "text")
            return {
                "ok": True,
                "answer": answer,
                "usage": {"input_tokens": total_input_tokens, "output_tokens": total_output_tokens},
            }, None

        messages.append({"role": "assistant", "content": response.content})
        tool_result_blocks = []
        for block in response.content:
            if block.type != "tool_use":
                continue
            result_text, tool_err = dispatch(block.name, block.input)
            if result_text and len(result_text) > ASSISTANT_TOOL_RESULT_MAX_CHARS:
                result_text = (result_text[:ASSISTANT_TOOL_RESULT_MAX_CHARS]
                               + f"\n... [cut at {ASSISTANT_TOOL_RESULT_MAX_CHARS} characters - narrow the request]")
            tool_result_blocks.append({
                "type": "tool_result",
                "tool_use_id": block.id,
                "content": tool_err if tool_err else result_text,
                "is_error": bool(tool_err),
            })
        messages.append({"role": "user", "content": tool_result_blocks})

    return None, (jsonify({
        "ok": False,
        "error": "The assistant made too many tool calls without reaching an answer - try a more specific question.",
    }), 502)


@app.route("/assistant/ask_online", methods=["POST"])
def assistant_ask_online():
    data = request.get_json() or {}
    run_name = (data.get("run") or "").strip()
    question = (data.get("question") or "").strip()
    columns = data.get("columns") or []
    history = data.get("history") or []

    if run_name and not _valid_run_name(run_name):
        return jsonify({"ok": False, "error": "Invalid run name."}), 400
    if not question:
        return jsonify({"ok": False, "error": "Ask something first."}), 400
    if not isinstance(columns, list) or not all(
        isinstance(c, dict)
        and isinstance(c.get("file"), str)
        and isinstance(c.get("column"), str)
        and c.get("source") in ("run", "drone")
        for c in columns
    ):
        return jsonify({"ok": False, "error": "Invalid columns."}), 400
    if not isinstance(history, list) or not all(
        isinstance(m, dict) and m.get("role") in ("user", "assistant") and isinstance(m.get("content"), str)
        for m in history
    ):
        return jsonify({"ok": False, "error": "Invalid conversation history."}), 400

    context, err = _build_online_data_context(run_name, columns)
    if err:
        return err

    result, err = _ask_claude_with_tools(
        context, question, history, ASSISTANT_ONLINE_TOOLS,
        lambda tool, tool_input: _dispatch_assistant_tool(run_name, tool, tool_input),
        ASSISTANT_TOOLS_ADDENDUM)
    if err:
        return err
    return jsonify(result)


@app.route("/connect", methods=["POST"])
def connect():
    data = request.get_json()
    host = data.get("host", "").strip()
    username = data.get("username", "").strip()
    password = data.get("password", "")

    if not host or not username or not password:
        return jsonify({"ok": False, "error": "Host, username, and password are all required."}), 400

    with state["lock"]:
        try:
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh.connect(hostname=host, username=username, password=password, timeout=10)

            sftp = ssh.open_sftp()

            # Make sure the base directory exists
            try:
                sftp.chdir(REMOTE_DIR)
            except IOError:
                ssh.close()
                return jsonify({
                    "ok": False,
                    "error": f"Remote directory not found: {REMOTE_DIR}"
                }), 400

            # Make sure the notes subfolder exists (create it if missing)
            notes_dir = f"{REMOTE_DIR}/{NOTES_SUBFOLDER}"
            try:
                sftp.chdir(notes_dir)
            except IOError:
                try:
                    sftp.mkdir(notes_dir)
                except Exception as e:
                    ssh.close()
                    return jsonify({
                        "ok": False,
                        "error": f"Could not create subfolder {notes_dir}: {e}"
                    }), 400

            # Note: the CSV file itself is NOT created here. It's created
            # lazily on the first note (see add_note()), so its filename
            # timestamp reflects the time after you've had a chance to
            # use "Sync Time" - rather than locking in a name (and an
            # empty file) the moment you connect, before any clock fix.

            # Tear down any previous connection
            if state["ssh"]:
                try:
                    state["sftp"].close()
                    state["ssh"].close()
                except Exception:
                    pass

            state["ssh"] = ssh
            state["sftp"] = sftp
            state["remote_path"] = None
            state["connected"] = True
            state["host"] = host
            state["username"] = username
            state["password"] = password
            # A fresh connection might be a different host entirely, or the
            # same one with its data folders reset - don't let /plot_data
            # keep serving whatever paths were cached from before.
            _plot_path_cache.clear()
            _plot_points_cache.clear()
            _remote_csv_cache.clear()
            _column_numeric_cache.clear()

            return jsonify({
                "ok": True,
                "remote_path": None,
            })

        except paramiko.AuthenticationException:
            return jsonify({"ok": False, "error": "Authentication failed. Check username/password."}), 401
        except Exception as e:
            return jsonify({"ok": False, "error": f"Connection failed: {e}"}), 500


@app.route("/note", methods=["POST"])
def add_note():
    data = request.get_json()
    text = (data.get("text") or "").strip()

    if not text:
        return jsonify({"ok": False, "error": "Note text is empty."}), 400

    with state["lock"]:
        if not state["connected"] or not state["sftp"]:
            return jsonify({"ok": False, "error": "Not connected to remote host."}), 400

        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        try:
            # Lazily pick the CSV file on the first note of a session rather
            # than at connect time, so its date reflects "now" - after
            # you've had a chance to fix the Pi's clock with Sync Time -
            # instead of locking in a name the moment you connect. The
            # filename is per-day, so if a file for today already exists
            # (e.g. from an earlier session that disconnected and
            # reconnected), we append to it instead of creating another one.
            is_new_file = False
            if state["remote_path"] is None:
                remote_path, filename = build_remote_path()
                try:
                    state["sftp"].stat(remote_path)
                    file_exists = True
                except IOError:
                    file_exists = False

                if not file_exists:
                    header_buf = io.StringIO()
                    csv.writer(header_buf).writerow(["timestamp", "note"])
                    with state["sftp"].file(remote_path, "w") as f:
                        f.write(header_buf.getvalue())
                    is_new_file = True

                state["remote_path"] = remote_path

            # Append this note as a CSV row to the remote file
            buf = io.StringIO()
            writer = csv.writer(buf)
            writer.writerow([timestamp, text])
            line = buf.getvalue()

            with state["sftp"].open(state["remote_path"], "a") as f:
                f.write(line)

            return jsonify({
                "ok": True,
                "timestamp": timestamp,
                "text": text,
                "remote_path": state["remote_path"],
                "file_created": is_new_file,
            })

        except Exception as e:
            _mark_disconnected()
            return jsonify({"ok": False, "error": f"Failed to write note (connection may have dropped): {e}"}), 500


@app.route("/notes")
def get_notes():
    """
    Reads back whatever's already in today's notes CSV on the Pi, if it
    exists - so the session log can show earlier notes right after
    connecting, instead of only filling in from here on. Without this, a
    disconnect + reconnect (or just reloading the page) always started the
    log box empty even though the notes were sitting right there in the
    same file the next note would append to - nothing was actually lost,
    it just wasn't being displayed.

    Read-only: doesn't create the file or touch state["remote_path"] -
    add_note() already independently discovers and reuses today's existing
    file on the first note of a session, so this doesn't need to duplicate
    that.
    """
    with state["lock"]:
        if not state["connected"] or not state["sftp"]:
            return jsonify({"ok": False, "error": "Not connected to remote host."}), 400

        remote_path, _filename = build_remote_path()
        try:
            state["sftp"].stat(remote_path)
        except IOError:
            return jsonify({"ok": True, "notes": []})

        try:
            with state["sftp"].open(remote_path, "r") as f:
                raw = f.read().decode("utf-8", errors="replace")
        except Exception as e:
            _mark_disconnected()
            return jsonify({"ok": False, "error": f"Could not read notes file: {e}"}), 500

    notes = []
    for row in csv.DictReader(io.StringIO(raw)):
        timestamp = row.get("timestamp")
        text = row.get("note")
        if timestamp is None or text is None:
            continue
        notes.append({"timestamp": timestamp, "text": text})

    return jsonify({"ok": True, "notes": notes})


def _run_sudo_command(ssh, password, command, timeout=15):
    """
    Runs `command` on the remote host with sudo, feeding `password` to
    sudo's stdin (sudo -S) rather than passing it as a command-line
    argument, so it doesn't show up in the remote process list or shell
    history.

    Returns (success, stdout_text, stderr_text).
    `success` is True only if the command exited with status 0 AND
    sudo didn't reject the password (sudo's "Sorry, try again" /
    "incorrect password" goes to stderr and exit status != 0, so the
    exit-status check already covers it, but we also scan stderr for
    that phrase to give a clearer error message).
    """
    full_command = f"sudo -S -p '' {command}"
    stdin, stdout, stderr = ssh.exec_command(full_command, timeout=timeout)
    stdin.write(password + "\n")
    stdin.flush()
    stdin.channel.shutdown_write()

    exit_status = stdout.channel.recv_exit_status()
    out_text = stdout.read().decode("utf-8", errors="replace").strip()
    err_text = stderr.read().decode("utf-8", errors="replace").strip()

    success = exit_status == 0
    return success, out_text, err_text


def _parse_cpu_percent(top_output):
    """
    Parses `top -bn1`'s "%Cpu(s):  3.2 us, ..., 95.0 id, ..." summary line
    into a single busy-percent figure (100 - idle). Field order in that
    line can vary, so this greps for the "id" figure by name rather than
    a fixed column index. Returns None (instead of raising) if the line
    isn't found/parseable - CPU usage is a nice-to-have for the status
    bar and shouldn't block the time-sync info that shares this SSH call.
    """
    match = re.search(r"([\d.]+)\s*id", top_output)
    if not match:
        return None
    return round(100 - float(match.group(1)), 1)


def _get_pi_time_info(ssh):
    """
    Reads the Pi's current date/time, whether NTP sync is active (via
    timedatectl), and its current CPU usage (via top). Returns a dict;
    raises on SSH/command failure so callers can report a clear
    connection-level error.

    NTP being active matters because if systemd-timesyncd (or chronyd) is
    running and synced, a manual `timedatectl set-time` either gets
    rejected or gets silently overwritten shortly after - worth surfacing
    clearly instead of leaving the user wondering why the time reverted.
    """
    # `timedatectl show` gives stable, parseable key=value output for NTP
    # status. For the actual current time, we ask the Pi itself to format
    # its own local time (including its timezone abbreviation) rather than
    # fetching a Unix epoch and converting it through this Mac's timezone -
    # if the Mac and Pi are ever in different timezones (e.g. while
    # traveling), an epoch-based conversion would silently display the
    # wrong wall-clock time for the Pi. The CPU line is bundled into the
    # same round trip (behind a marker line) rather than a separate poll,
    # since this already runs every 15s regardless - but it runs FIRST,
    # before the epoch is captured. `top -bn1` isn't instant, and the
    # frontend compares that epoch against its own clock only after the
    # full SSH round trip completes; capturing the epoch any earlier than
    # "last thing this command does" would silently bake top's runtime
    # (plus the time to ship the rest of the output back) into the
    # apparent clock drift, even when the two clocks are actually in sync.
    stdin, stdout, stderr = ssh.exec_command(
        "top -bn1 | grep -m1 '^%Cpu'; "
        "echo '---time---'; "
        "timedatectl show --property=NTP --property=NTPSynchronized --property=Timezone; "
        "date '+%Y-%m-%d %H:%M:%S %Z'; "
        "date +%s",
        timeout=10,
    )
    exit_status = stdout.channel.recv_exit_status()
    out_text = stdout.read().decode("utf-8", errors="replace")
    err_text = stderr.read().decode("utf-8", errors="replace").strip()

    if exit_status != 0:
        raise RuntimeError(err_text or "timedatectl/date command failed on the remote host.")

    cpu_part, _, time_part = out_text.partition("---time---")

    info = {}
    pi_datetime_str = None
    pi_epoch = None
    for line in time_part.splitlines():
        line = line.strip()
        if not line:
            continue
        if "=" in line and not line[0].isdigit():
            key, _, value = line.partition("=")
            info[key.strip()] = value.strip()
        elif line.isdigit():
            # The `date +%s` output line - a plain Unix timestamp, used by
            # the frontend to detect clock drift against the Mac's clock.
            pi_epoch = int(line)
        else:
            # The pretty `date` output line: "2026-06-26 20:46:58 IDT"
            pi_datetime_str = line

    ntp_active = info.get("NTP") == "yes"
    ntp_synced = info.get("NTPSynchronized") == "yes"

    return {
        "pi_time": pi_datetime_str,
        "pi_epoch": pi_epoch,
        "pi_timezone": info.get("Timezone"),
        "ntp_active": ntp_active,
        "ntp_synchronized": ntp_synced,
        "cpu_percent": _parse_cpu_percent(cpu_part),
    }


@app.route("/pi_time")
def pi_time():
    """
    Read-only check: returns the Pi's current time and NTP status, for
    the live clock display. Does not require sudo.
    """
    with state["lock"]:
        if not state["connected"] or not state["ssh"]:
            return jsonify({"ok": False, "error": "Not connected to remote host."}), 400
        try:
            info = _get_pi_time_info(state["ssh"])
            return jsonify({"ok": True, **info})
        except Exception as e:
            # This poll runs every 15s regardless of what the user is doing,
            # so it's usually the fastest way to notice the physical link
            # actually died (cable pulled, Pi powered off) - mark it so the
            # status dot stops claiming we're still connected.
            _mark_disconnected()
            return jsonify({"ok": False, "error": f"Could not read Pi time: {e}"}), 500


@app.route("/sync_time", methods=["POST"])
def sync_time():
    """
    Sets the Pi's clock to match this Mac's current local time, using
    `sudo timedatectl set-time`. Reuses the SSH login password as the
    sudo password (same password on this kind of single-user Pi setup).

    If NTP sync is active on the Pi, this will likely be rejected or
    immediately overwritten, so we check for that first and report it
    clearly rather than claiming success when the time won't actually
    stick.
    """
    with state["lock"]:
        if not state["connected"] or not state["ssh"]:
            return jsonify({"ok": False, "error": "Not connected to remote host."}), 400

        ssh = state["ssh"]
        password = state["password"]

        try:
            pre_info = _get_pi_time_info(ssh)
        except Exception as e:
            _mark_disconnected()
            return jsonify({"ok": False, "error": f"Could not check Pi's NTP status: {e}"}), 500

        if pre_info["ntp_active"]:
            return jsonify({
                "ok": False,
                "error": (
                    "NTP time sync is active on the Pi, so a manual time "
                    "won't stick - it'll just get overwritten again. "
                    "Disable it first with: sudo timedatectl set-ntp false"
                ),
            }), 409

        mac_now = datetime.now()
        target_str = mac_now.strftime("%Y-%m-%d %H:%M:%S")

        try:
            success, out_text, err_text = _run_sudo_command(
                ssh, password, f"timedatectl set-time '{target_str}'"
            )
        except Exception as e:
            _mark_disconnected()
            return jsonify({"ok": False, "error": f"Failed to run sync command: {e}"}), 500

        if not success:
            lowered = err_text.lower()
            if "incorrect password" in lowered or "sorry, try again" in lowered or "authentication" in lowered:
                friendly = "Sudo rejected the password."
            else:
                friendly = err_text or "Unknown error setting the time."
            return jsonify({"ok": False, "error": friendly}), 400

        try:
            post_info = _get_pi_time_info(ssh)
        except Exception as e:
            # The set-time call itself succeeded; just couldn't re-read
            # afterwards to confirm. Still report success on the action,
            # but the connection dying between those two calls is a real
            # signal worth acting on too - mark it disconnected even
            # though this particular response still reports ok:True.
            _mark_disconnected()
            return jsonify({
                "ok": True,
                "mac_time": target_str,
                "pi_time": None,
                "warning": f"Time was set, but couldn't re-read it back to confirm: {e}",
            })

        # The normal success path: the time was set AND we successfully
        # re-read it back to confirm. This was previously missing its own
        # return - falling off the end of a Flask view function makes
        # Flask raise "View function did not return a valid response",
        # a 500 with an HTML (not JSON) body. The browser's fetch() then
        # throws when it tries to parse that as JSON, landing in the
        # frontend's catch block - which is why this showed up as "Could
        # not reach the local app server" even though sudo + the time set
        # genuinely succeeded on the Pi.
        return jsonify({
            "ok": True,
            "mac_time": target_str,
            "pi_time": post_info["pi_time"],
        })


@app.route("/restart_pi", methods=["POST"])
def restart_pi():
    """
    Reboots the remote Pi via `sudo reboot`, reusing the SSH login
    password as the sudo password (same pattern as sync_time).

    A reboot kills the SSH session out from under us almost immediately,
    so - unlike other sudo commands - a connection error right after
    issuing it is the expected outcome, not a failure. Either way the
    session is now dead, so we tear down local state the same way
    /disconnect does and let the frontend fall back to the connect
    screen instead of a status bar that claims we're still connected.
    """
    with state["lock"]:
        if not state["connected"] or not state["ssh"]:
            return jsonify({"ok": False, "error": "Not connected to remote host."}), 400

        ssh = state["ssh"]
        password = state["password"]

        try:
            _run_sudo_command(ssh, password, "reboot", timeout=10)
        except Exception:
            pass

        if state["ssh"]:
            try:
                state["sftp"].close()
                state["ssh"].close()
            except Exception:
                pass
        state["ssh"] = None
        state["sftp"] = None
        state["connected"] = False
        state["remote_path"] = None
        state["host"] = None
        state["username"] = None
        state["password"] = None

        return jsonify({
            "ok": True,
            "message": "Restart command sent - the Pi is rebooting. Reconnect once it's back up.",
        })


def _run_remote_runall(ssh):
    remote_dir = f"{REMOTE_DIR}/{RUN_ALL_SUBDIR}"
    script_path = f"{remote_dir}/{RUN_ALL_SCRIPT}"
    log_file = f"{remote_dir}/{RUN_ALL_LOG}"

    shell_script = (
        "set -e; "
        "cd " + shlex.quote(remote_dir) + "; "
        "if [ ! -x " + shlex.quote(RUN_ALL_VENV_PYTHON) + " ]; then echo 'Venv python not found' >&2; exit 1; fi; "
        "if [ ! -f " + shlex.quote(script_path) + " ]; then echo 'Script not found' >&2; exit 1; fi; "
        "export VIRTUAL_ENV=" + shlex.quote(RUN_ALL_REMOTE_VENV) + "; "
        "export PATH=" + shlex.quote(RUN_ALL_REMOTE_VENV + "/bin") + ":$PATH; "
        # PYTHONUNBUFFERED=1 (belt-and-suspenders with python's own -u flag
        # below) is the actual fix for "no output until it finishes": when
        # stdout is redirected to a file instead of a TTY, Python switches
        # from line-buffered to fully block-buffered, so print() output sits
        # in memory and never reaches runall.log until the internal buffer
        # fills or the process exits. -u disables that buffering entirely.
        "export PYTHONUNBUFFERED=1; "
        ": > " + shlex.quote(log_file) + "; "
        "nohup " + shlex.quote(RUN_ALL_VENV_PYTHON) + " -u " + shlex.quote(RUN_ALL_SCRIPT) + " > " + shlex.quote(log_file) + " 2>&1 < /dev/null & echo $! > " + shlex.quote(remote_dir + "/runall.pid") + "; "
        "sleep 2; "
        "if [ -f " + shlex.quote(log_file) + " ]; then tail -n 50 " + shlex.quote(log_file) + "; fi"
    )
    command = "bash -lc " + shlex.quote(shell_script)

    stdin, stdout, stderr = ssh.exec_command(command, timeout=30)
    out_text = stdout.read().decode("utf-8", errors="replace").strip()
    err_text = stderr.read().decode("utf-8", errors="replace").strip()
    exit_status = stdout.channel.recv_exit_status()
    return exit_status == 0, out_text, err_text


@app.route("/run_all_sensors", methods=["POST"])
def run_all_sensors():
    with state["lock"]:
        if not state["connected"] or not state["ssh"]:
            return jsonify({"ok": False, "error": "Not connected to remote host."}), 400

        # Check the pidfile on the Pi itself rather than trusting
        # state["runall_pid"] - that's only kept fresh by the /runall_output
        # poll, so it can still say "not running" even though runall.py is
        # genuinely running (started from another dashboard session, or by
        # hand over SSH). Launching a second instance on top of a live one
        # isn't safe: both would fight over the same serial ports/USB
        # devices, so this has to block it regardless of how the first one
        # was started.
        try:
            already_running, live_pid = _check_runall_running(state["ssh"])
        except Exception as e:
            _mark_disconnected()
            return jsonify({"ok": False, "error": f"Could not check runall.py status: {e}"}), 500

        if already_running:
            state["runall_pid"] = live_pid
            return jsonify({
                "ok": False,
                "already_running": True,
                "error": f"runall.py is already running (PID {live_pid}) - stop it before starting again.",
                "pid": live_pid,
            }), 409

        try:
            success, out_text, err_text = _run_remote_runall(state["ssh"])
        except Exception as e:
            _mark_disconnected()
            return jsonify({"ok": False, "error": f"Could not start remote sensors: {e}"}), 500

        if not success:
            return jsonify({
                "ok": False,
                "error": err_text or out_text or "Failed to start remote script.",
                "stderr": err_text,
                "stdout": out_text,
            }), 500

        pid_value = None
        try:
            remote_dir = f"{REMOTE_DIR}/{RUN_ALL_SUBDIR}"
            stdin_pid, stdout_pid, stderr_pid = state["ssh"].exec_command(
                "cat " + shlex.quote(remote_dir + "/runall.pid"), timeout=15
            )
            pid_text = stdout_pid.read().decode("utf-8", errors="replace").strip()
            if pid_text.isdigit():
                pid_value = int(pid_text)
        except Exception:
            pid_value = None

        # NOTE: deliberately not re-acquiring state["lock"] here - we're
        # already inside it (acquired at the top of this function). This
        # used to be `with state["lock"]:` again, which deadlocked every
        # single time this route succeeded: state["lock"] is a plain
        # threading.Lock (non-reentrant), so a thread trying to acquire a
        # lock it already holds blocks forever. That hang is why the
        # remote launch genuinely happened but the HTTP response to the
        # browser never arrived, leaving the button stuck on "starting...".
        state["runall_pid"] = pid_value
        # A fresh run means a fresh output/<timestamp> folder - drop any
        # cached CSV paths from the previous run so /plot_data doesn't keep
        # serving frozen data from a run that's no longer being written to.
        _plot_path_cache.clear()
        _plot_points_cache.clear()

        if not out_text and not err_text:
            try:
                remote_log = f"{REMOTE_DIR}/{RUN_ALL_SUBDIR}/{RUN_ALL_LOG}"
                stdin_tail, stdout_tail, stderr_tail = state["ssh"].exec_command(
                    "if [ -f " + shlex.quote(remote_log) + " ]; then tail -n 50 " + shlex.quote(remote_log) + "; fi",
                    timeout=15,
                )
                out_text = stdout_tail.read().decode("utf-8", errors="replace").strip()
                err_text = stderr_tail.read().decode("utf-8", errors="replace").strip()
            except Exception:
                pass

        return jsonify({
            "ok": True,
            "message": "Started Run All Sensors on the remote Pi.",
            "remote_log": f"{REMOTE_DIR}/{RUN_ALL_SUBDIR}/{RUN_ALL_LOG}",
            "output": out_text or err_text or "Launch command completed.",
            "stderr": err_text,
            "stdout": out_text,
            "pid": pid_value,
        })


def _run_remote_c_app(ssh, password):
    """
    Launches the DJI PSDK demo binary and verifies it actually stayed up.

    Two launch details matter (both learned the hard way - starting it
    blind with a bare absolute path reported "started" while the process
    died within milliseconds):
      - it must run from its own directory, since the PSDK demo loads its
        config via relative paths;
      - it needs root for UART/USB access to the flight controller (DJI's
        own docs run it as `sudo ./dji_sdk_demo_on_rpi`), so it's launched
        through sudo -S with the stored SSH password fed on stdin - same
        pattern as _run_sudo_command.

    Returns (success, message, log_tail).
    """
    remote_path = RUN_C_APP_REMOTE_PATH
    bin_dir = os.path.dirname(remote_path)
    binary_name = os.path.basename(remote_path)
    log_path = RUN_C_APP_LOG

    stdin, stdout, stderr = ssh.exec_command(
        "pgrep -f " + shlex.quote(binary_name), timeout=10
    )
    existing_pids = stdout.read().decode("utf-8", errors="replace").strip()
    if stdout.channel.recv_exit_status() == 0 and existing_pids:
        pid = existing_pids.splitlines()[0].strip()
        return True, f"PSDK app is already running (PID {pid}).", ""

    stdin, stdout, stderr = ssh.exec_command(
        "test -x " + shlex.quote(remote_path), timeout=10
    )
    if stdout.channel.recv_exit_status() != 0:
        return False, f"Binary not found or not executable: {remote_path}", ""

    # The inner sh does cd + nohup + background as root; sudo itself exits
    # right after the inner shell does, leaving the nohup'd binary running.
    inner = (
        "cd " + shlex.quote(bin_dir) + " && "
        "nohup " + shlex.quote(remote_path) + " > " + shlex.quote(log_path) + " 2>&1 < /dev/null & "
        "echo launched"
    )
    launch_cmd = "sudo -S -p '' sh -c " + shlex.quote(inner)
    stdin, stdout, stderr = ssh.exec_command(launch_cmd, timeout=15)
    stdin.write(password + "\n")
    stdin.flush()
    stdin.channel.shutdown_write()
    exit_status = stdout.channel.recv_exit_status()
    err_text = stderr.read().decode("utf-8", errors="replace").strip()

    if exit_status != 0:
        lowered = err_text.lower()
        if "sorry, try again" in lowered or "incorrect password" in lowered:
            return False, "Sudo rejected the password.", ""
        return False, err_text or "Failed to launch the PSDK app.", ""

    # The whole point: give it a moment, then confirm it's still alive
    # instead of trusting that the launch line printing "launched" means
    # anything. If it died, surface its own log so the actual error
    # (config missing, UART busy, wrong app key...) reaches the UI.
    verify_cmd = (
        "sleep 2; "
        "if pgrep -f " + shlex.quote(binary_name) + " > /dev/null; then echo ALIVE; fi; "
        "echo '---LOG---'; "
        "if [ -f " + shlex.quote(log_path) + " ]; then tail -n 30 " + shlex.quote(log_path) + "; fi"
    )
    stdin, stdout, stderr = ssh.exec_command(verify_cmd, timeout=20)
    verify_out = stdout.read().decode("utf-8", errors="replace")
    stdout.channel.recv_exit_status()

    head, _, log_tail = verify_out.partition("---LOG---")
    alive = "ALIVE" in head
    log_tail = log_tail.strip()

    if alive:
        return True, "PSDK app started and is running.", log_tail
    return False, "PSDK app exited right after starting - see its log output below.", log_tail


def _remote_pid_alive(ssh, pid):
    """
    Returns True if `pid` is still alive on the remote host, False if not.
    Uses `kill -0` (signal 0 just checks for existence/permission, sends
    nothing) instead of parsing `ps` output, since exit status alone is
    unambiguous: 0 = alive, non-zero = gone (or not ours - either way we
    can stop tracking it).
    """
    stdin, stdout, stderr = ssh.exec_command(f"kill -0 {pid}", timeout=10)
    exit_status = stdout.channel.recv_exit_status()
    return exit_status == 0


def _stop_remote_runall(ssh, pid, sigint_timeout=45):
    """
    Stops the tracked runall.py PID, verifying it actually exits instead of
    just firing a signal and hoping.

    runall.py's own SIGINT handler does a *layered* graceful shutdown:
    stop vitals (up to 10s), then the merger (up to 10s), then each sensor
    in turn (up to 5s each). With even two or three sensors that's
    comfortably 25-35+ seconds of legitimate shutdown time - so SIGINT
    needs a generous grace period. Escalating to SIGTERM/SIGKILL too early
    doesn't just fail to speed things up, it actively cuts runall.py's own
    cleanup short and can orphan its sensor/merger/vitals child processes,
    leaving them running on the Pi with nothing left to stop them.

    SIGTERM and SIGKILL get much shorter windows since they're the
    "something's actually stuck" fallback, not the expected path.

    Returns (stopped, final_signal_used_or_None, detail_message).
    """
    if not _remote_pid_alive(ssh, pid):
        return True, None, f"PID {pid} was already not running."

    for sig_name, sig_flag, wait_seconds in [
        ("SIGINT", "-INT", sigint_timeout),
        ("SIGTERM", "-TERM", 10),
        ("SIGKILL", "-KILL", 5),
    ]:
        ssh.exec_command(f"kill {sig_flag} {pid}", timeout=10)[1].channel.recv_exit_status()

        # Poll kill -0 every 0.5s instead of one long sleep, so a fast exit
        # (the common case for SIGINT/SIGTERM) returns quickly rather than
        # always paying the full wait_seconds, while a slow/stuck process
        # still gets the full window before we escalate.
        deadline_steps = max(1, int(wait_seconds / 0.5))
        for _ in range(deadline_steps):
            if not _remote_pid_alive(ssh, pid):
                return True, sig_name, f"Stopped runall.py (PID {pid}) with {sig_name}."
            stdin, stdout, stderr = ssh.exec_command("sleep 0.5", timeout=10)
            stdout.channel.recv_exit_status()

    return False, "SIGKILL", f"PID {pid} did not exit even after SIGKILL."


@app.route("/stop_runall", methods=["POST"])
def stop_runall():
    with state["lock"]:
        if not state["connected"] or not state["ssh"]:
            return jsonify({"ok": False, "error": "Not connected to remote host."}), 400
        ssh = state["ssh"]
        pid = state.get("runall_pid")

    if not pid:
        # state["runall_pid"] is only kept fresh by the /runall_output poll,
        # so it can still be empty right after connecting even though
        # runall.py is genuinely running (started elsewhere). Read the
        # pidfile straight off the Pi before giving up, so Stop works
        # regardless of how/who started it.
        try:
            running, live_pid = _check_runall_running(ssh)
        except Exception as e:
            with state["lock"]:
                _mark_disconnected()
            return jsonify({"ok": False, "error": f"Could not check runall.py status: {e}"}), 500
        if not running:
            return jsonify({"ok": False, "error": "runall.py is not currently running."}), 400
        pid = live_pid
        with state["lock"]:
            state["runall_pid"] = pid

    # Deliberately outside the lock: a clean SIGINT shutdown of runall.py
    # (vitals -> merger -> each sensor) can legitimately take up to ~45s,
    # and escalating through SIGTERM/SIGKILL on top of that pushes the
    # worst case to ~60s. Holding state["lock"] for all of that would block
    # every other endpoint (notes, pi_time, the output-polling that runs
    # every few seconds) for the same stretch, which is exactly what made
    # the whole dashboard look stuck rather than just the Stop button.
    try:
        stopped, sig_used, detail = _stop_remote_runall(ssh, pid)
    except Exception as e:
        with state["lock"]:
            _mark_disconnected()
        return jsonify({"ok": False, "error": f"Could not stop runall.py: {e}"}), 500

    with state["lock"]:
        if not stopped:
            # Leave runall_pid in place - it's still genuinely running, so
            # the UI should keep treating it as "running" rather than
            # silently forgetting about a process we never actually killed.
            return jsonify({
                "ok": False,
                "error": detail,
                "pid": pid,
            }), 500

        state["runall_pid"] = None
        return jsonify({
            "ok": True,
            "message": detail,
            "signal_used": sig_used,
            "pid": pid,
        })


@app.route("/runall_logs")
def runall_logs():
    """
    Lists the logs available for the current/most recent runall.py
    invocation, for the output-panel dropdown:
      - the top-level controller log (runall.log) the dashboard already
        tails, which only has runall.py's own status/lifecycle messages
      - every per-process log under the most recent output/<timestamp>/log/
        folder runall.py creates - this is where each sensor's, the
        merger's, and the vitals exporter's own prints actually land,
        since runall.py redirects each child's stdout/stderr there
        individually rather than to runall.log.
    """
    with state["lock"]:
        if not state["connected"] or not state["ssh"]:
            return jsonify({"ok": False, "error": "Not connected to remote host."}), 400

        remote_dir = f"{REMOTE_DIR}/{RUN_ALL_SUBDIR}"
        # Find the newest timestamped run folder under <remote_dir>/output/
        # (runall.py names them like 20260629_143000) and list its log/
        # subfolder. `ls -1` sorts lexically, which matches chronological
        # order for that timestamp format, so `tail -n 1` gets the latest.
        find_cmd = (
            "latest=$(ls -1 " + shlex.quote(remote_dir + "/output") + " 2>/dev/null | sort | tail -n 1); "
            "if [ -n \"$latest\" ]; then "
            "echo \"RUN_DIR:$latest\"; "
            "ls -1 " + shlex.quote(remote_dir + "/output") + "/\"$latest\"/log 2>/dev/null; "
            "fi"
        )

        try:
            stdin, stdout, stderr = state["ssh"].exec_command(find_cmd, timeout=15)
            out_text = stdout.read().decode("utf-8", errors="replace").strip()
        except Exception as e:
            _mark_disconnected()
            return jsonify({"ok": False, "error": f"Could not list run logs: {e}"}), 500

        run_dir_name = None
        per_process_logs = []
        for line in out_text.splitlines():
            line = line.strip()
            if not line:
                continue
            if line.startswith("RUN_DIR:"):
                run_dir_name = line.split("RUN_DIR:", 1)[1].strip()
            else:
                per_process_logs.append(line)

        logs = [{"id": "controller", "label": "Controller (runall.log)"}]
        for filename in sorted(per_process_logs):
            logs.append({
                "id": f"run:{filename}",
                "label": filename,
            })

        return jsonify({
            "ok": True,
            "logs": logs,
            "run_dir": run_dir_name,
        })


def _resolve_log_path(log_id, run_dir_name):
    """
    Maps a log id from the dropdown to its absolute remote path.
    'controller' -> the top-level runall.log this dashboard launches with.
    'run:<filename>' -> a per-process log inside the latest run's log/
    folder (sensor_controller.log, <sensor>.log, merger.log, vitals.log).
    Returns None for an unrecognized id so the caller can error cleanly
    instead of building a path to something we didn't intend.
    """
    remote_dir = f"{REMOTE_DIR}/{RUN_ALL_SUBDIR}"
    if log_id == "controller" or not log_id:
        return f"{remote_dir}/{RUN_ALL_LOG}"
    if log_id.startswith("run:") and run_dir_name:
        filename = log_id.split("run:", 1)[1]
        # Guard against path traversal via a crafted log id - filenames
        # here should only ever be plain names returned by /runall_logs.
        if "/" in filename or ".." in filename:
            return None
        return f"{remote_dir}/output/{run_dir_name}/log/{filename}"
    return None


@app.route("/runall_output")
def runall_output():
    log_id = (request.args.get("log") or "controller").strip()
    run_dir_name = (request.args.get("run_dir") or "").strip() or None

    with state["lock"]:
        if not state["connected"] or not state["ssh"]:
            return jsonify({"ok": False, "error": "Not connected to remote host."}), 400

        remote_log = _resolve_log_path(log_id, run_dir_name)
        if remote_log is None:
            return jsonify({"ok": False, "error": f"Unknown log: {log_id}"}), 400
        remote_pid = f"{REMOTE_DIR}/{RUN_ALL_SUBDIR}/runall.pid"

        try:
            stdin, stdout, stderr = state["ssh"].exec_command(
                "if [ -f " + shlex.quote(remote_log) + " ]; then tail -n 100 " + shlex.quote(remote_log) + "; else echo '(log file not found yet)'; fi; "
                "if [ -f " + shlex.quote(remote_pid) + " ]; then echo \"PID:\"; cat " + shlex.quote(remote_pid) + "; fi",
                timeout=15
            )
            raw_text = stdout.read().decode("utf-8", errors="replace").strip()
            err_text = stderr.read().decode("utf-8", errors="replace").strip()

            # The PID line is a marker we added for parsing, not part of
            # runall's actual output - split it out so it doesn't show up
            # as a stray "PID:\n1234" at the bottom of the output panel.
            lines = raw_text.splitlines()
            pid_text = None
            log_lines = lines
            for i, line in enumerate(lines):
                if line.startswith("PID:"):
                    log_lines = lines[:i]
                    if i + 1 < len(lines):
                        pid_text = lines[i + 1].strip()
                    break
            output_text = "\n".join(log_lines).strip()

            # The "is runall.py running" check always comes from the
            # controller's own PID, regardless of which log is currently
            # selected in the dropdown - viewing a sensor's log shouldn't
            # change what the Run/Stop buttons think is happening.
            running = False
            if pid_text and pid_text.isdigit():
                try:
                    running = _remote_pid_alive(state["ssh"], int(pid_text))
                except Exception:
                    running = False

            # Keep state["runall_pid"] in sync with what's actually running
            # remotely - this is what /stop_runall acts on, and this poll
            # (which runs every few seconds regardless of who started the
            # process) is the only thing that discovers the PID again after
            # e.g. the dashboard app itself restarts while runall.py keeps
            # running on the Pi. Without this, Stop would find no tracked
            # PID even though the status line correctly says "running".
            if running:
                state["runall_pid"] = int(pid_text)
            else:
                state["runall_pid"] = None

            return jsonify({
                "ok": True,
                "message": "Fetched latest runall output.",
                "output": output_text,
                "pid": pid_text if running else None,
                "running": running,
                "stderr": err_text,
                "log": log_id,
            })
        except Exception as e:
            # Like /pi_time, this poll runs continuously (every few seconds)
            # while the notes view is open, so it's one of the fastest ways
            # to notice the physical link actually died.
            _mark_disconnected()
            return jsonify({"ok": False, "error": f"Could not fetch runall output: {e}"}), 500


def _check_runall_running(ssh):
    """
    Reads runall.pid straight from the Pi and checks whether that PID is
    still alive. Deliberately independent of state["runall_pid"], which is
    only kept fresh by the /runall_output poll - on a page reload or right
    after reconnecting, that in-memory value can be stale (still None even
    though runall.py is actually running), which would be the wrong thing
    to gate sensor-config edits on.
    """
    stdin, stdout, stderr = ssh.exec_command(
        "if [ -f " + shlex.quote(RUN_ALL_PID_FILE) + " ]; then cat " + shlex.quote(RUN_ALL_PID_FILE) + "; fi",
        timeout=10,
    )
    pid_text = stdout.read().decode("utf-8", errors="replace").strip()
    stdout.channel.recv_exit_status()
    if not pid_text.isdigit():
        return False, None
    pid = int(pid_text)
    return _remote_pid_alive(ssh, pid), pid


def _find_json_object_span(text, start_idx):
    """
    Given the index of an opening '{' in `text`, returns the index just
    past its matching closing '}' (brace-depth scan that also tracks
    whether we're inside a JSON string, so braces inside string values
    don't throw off the count).
    """
    depth = 0
    i = start_idx
    in_string = False
    escape = False
    while i < len(text):
        ch = text[i]
        if in_string:
            if escape:
                escape = False
            elif ch == "\\":
                escape = True
            elif ch == '"':
                in_string = False
        else:
            if ch == '"':
                in_string = True
            elif ch == "{":
                depth += 1
            elif ch == "}":
                depth -= 1
                if depth == 0:
                    return i + 1
        i += 1
    raise ValueError("Unbalanced braces while scanning sensor_config.json")


def _set_sensor_enabled_in_text(raw_text, sensor_name, enabled):
    """
    Rewrites only the "enabled": true/false value inside the named
    sensor's block in sensor_config.json's raw text, leaving every other
    byte untouched. A full json.load() + json.dump() round-trip would
    silently reformat the whole file (indentation, key order, the hand-
    tweaked spacing in a couple of sensor blocks) since this file is
    hand-edited directly on the Pi and its formatting isn't ours to change
    as a side effect of flipping one flag.

    Raises ValueError if the sensor block or its "enabled" field can't be
    found, so callers can turn that into a clean error response instead of
    silently doing nothing.
    """
    key_pattern = re.compile(r'"' + re.escape(sensor_name) + r'"\s*:\s*\{')
    match = key_pattern.search(raw_text)
    if not match:
        raise ValueError(f'Could not locate "{sensor_name}" block in sensor_config.json')

    block_start = match.end() - 1  # index of the sensor's opening '{'
    block_end = _find_json_object_span(raw_text, block_start)
    block = raw_text[block_start:block_end]

    new_value = "true" if enabled else "false"
    new_block, count = re.subn(
        r'("enabled"\s*:\s*)(true|false)', lambda m: m.group(1) + new_value, block, count=1
    )
    if count == 0:
        raise ValueError(f'Could not find "enabled" field inside "{sensor_name}" block')

    return raw_text[:block_start] + new_block + raw_text[block_end:]


@app.route("/sensor_config")
def sensor_config():
    """
    Lists every sensor defined under the "sensors" key of sensor_config.json
    on the Pi, with its type and current enabled flag - driven entirely by
    whatever's actually in the file, so adding/removing/renaming a sensor
    there shows up here with no code change.
    """
    with state["lock"]:
        if not state["connected"] or not state["ssh"] or not state["sftp"]:
            return jsonify({"ok": False, "error": "Not connected to remote host."}), 400

        try:
            with state["sftp"].open(SENSOR_CONFIG_REMOTE_PATH, "r") as f:
                raw = f.read().decode("utf-8")
            running, _pid = _check_runall_running(state["ssh"])
        except Exception as e:
            _mark_disconnected()
            return jsonify({"ok": False, "error": f"Could not read sensor_config.json: {e}"}), 500

        try:
            parsed = json.loads(raw)
        except Exception as e:
            return jsonify({"ok": False, "error": f"sensor_config.json is not valid JSON: {e}"}), 500

        sensors = parsed.get("sensors", {})
        sensor_list = [
            {
                "name": name,
                "type": cfg.get("type", name) if isinstance(cfg, dict) else name,
                "enabled": bool(cfg.get("enabled", False)) if isinstance(cfg, dict) else False,
                # Lets the frontend tell which sensors "Check Sensor" can
                # actually run standalone (anything launched via the same
                # sensor_runner.py <name> CLI runall.py itself uses) versus
                # ones like spectro that use their own separate script.
                "script": cfg.get("script", "") if isinstance(cfg, dict) else "",
            }
            for name, cfg in sensors.items()
        ]

        return jsonify({"ok": True, "sensors": sensor_list, "runall_running": running})


@app.route("/sensor_config/toggle", methods=["POST"])
def sensor_config_toggle():
    data = request.get_json()
    name = (data.get("sensor") or "").strip()
    enabled = data.get("enabled")

    if not name or not isinstance(enabled, bool):
        return jsonify({"ok": False, "error": '"sensor" and a boolean "enabled" are required.'}), 400

    with state["lock"]:
        if not state["connected"] or not state["ssh"] or not state["sftp"]:
            return jsonify({"ok": False, "error": "Not connected to remote host."}), 400

        try:
            running, _pid = _check_runall_running(state["ssh"])
        except Exception as e:
            _mark_disconnected()
            return jsonify({"ok": False, "error": f"Could not check runall.py status: {e}"}), 500

        if running:
            return jsonify({
                "ok": False,
                "error": "Stop Run All Sensors before changing sensor config - runall.py only reads it at startup.",
            }), 409

        try:
            with state["sftp"].open(SENSOR_CONFIG_REMOTE_PATH, "r") as f:
                raw = f.read().decode("utf-8")
        except Exception as e:
            _mark_disconnected()
            return jsonify({"ok": False, "error": f"Could not read sensor_config.json: {e}"}), 500

        try:
            parsed = json.loads(raw)
        except Exception as e:
            return jsonify({"ok": False, "error": f"sensor_config.json is not valid JSON: {e}"}), 500

        if name not in parsed.get("sensors", {}):
            return jsonify({"ok": False, "error": f"Unknown sensor: {name}"}), 400

        try:
            new_raw = _set_sensor_enabled_in_text(raw, name, enabled)
        except ValueError as e:
            return jsonify({"ok": False, "error": str(e)}), 500

        # Belt-and-suspenders: re-parse the edited text and confirm it's
        # still valid JSON with exactly the intended change before writing
        # anything back - refusing to write beats corrupting the Pi's only
        # copy of sensor_config.json.
        try:
            reparsed = json.loads(new_raw)
            if reparsed["sensors"][name]["enabled"] != enabled:
                raise ValueError("edited value doesn't match what was requested")
        except Exception as e:
            return jsonify({
                "ok": False,
                "error": f"Refusing to write - edited config failed validation: {e}",
            }), 500

        try:
            with state["sftp"].open(SENSOR_CONFIG_REMOTE_PATH, "w") as f:
                f.write(new_raw)
        except Exception as e:
            _mark_disconnected()
            return jsonify({"ok": False, "error": f"Could not write sensor_config.json: {e}"}), 500

        return jsonify({"ok": True, "sensor": name, "enabled": enabled})


PLOT_MAX_POINTS = 300

# When the client asks for a time window (the Operation tab's shared
# "time frame" selector), the last `window` seconds of the CSV are returned
# instead of a fixed sample count - 300 samples is 5 min of a 1 Hz sensor
# but only 30 s of the 10 Hz TriSonica, so a sample count never meant the
# same span across the six plots. A long window of fast data is thinned
# evenly to this many points; the canvas cannot show more anyway.
PLOT_WINDOW_MAX_POINTS = 1500
PLOT_TS_FORMAT = "%Y-%m-%d %H:%M:%S"


def _window_points(points, window_s):
    """
    Trim `points` (oldest first, each {"t": "YYYY-MM-DD HH:MM:SS", "v": x})
    to the requested time frame and thin the result.

    window_s None  -> legacy behaviour, the last PLOT_MAX_POINTS samples
    window_s 0     -> the whole run, thinned
    window_s > 0   -> the last window_s seconds before the newest sample

    Every CSV these plots read (uri_aplogger sensors, vitals_summary, the
    PSDK's telemetry) writes fixed-width timestamps, so string order is
    time order and the cut is a plain comparison per row - no parsing.
    Returns (points, total_in_window) where total_in_window is the count
    before thinning so the UI can say when it is showing a subset.
    """
    if window_s is None:
        return points[-PLOT_MAX_POINTS:], min(len(points), PLOT_MAX_POINTS)
    if not points:
        return points, 0
    kept = points
    if window_s > 0:
        try:
            newest = datetime.strptime(points[-1]["t"][:19], PLOT_TS_FORMAT)
        except ValueError:
            return points[-PLOT_MAX_POINTS:], min(len(points), PLOT_MAX_POINTS)
        cutoff = (newest - timedelta(seconds=window_s)).strftime(PLOT_TS_FORMAT)
        i = len(points)
        while i > 0 and points[i - 1]["t"][:19] >= cutoff:
            i -= 1
        kept = points[i:]
    total = len(kept)
    if total > PLOT_WINDOW_MAX_POINTS:
        stride = -(-total // PLOT_WINDOW_MAX_POINTS)  # ceil
        thinned = kept[::stride]
        if thinned[-1] is not kept[-1]:
            thinned.append(kept[-1])  # always end on the newest sample
        kept = thinned
    return kept, total


def _run_span_s(run_dir_name, points):
    """
    Seconds from the start of the run to the newest sample - the upper end
    of the Operation tab's time-frame slider. runall.py names each run
    folder with its own start time (YYYYMMDD_HHMMSS, possibly suffixed), so
    that is the run start for every sensor alike; files outside a run
    folder (drone telemetry) fall back to their own first row.
    """
    if not points:
        return None
    try:
        newest = datetime.strptime(points[-1]["t"][:19], PLOT_TS_FORMAT)
    except ValueError:
        return None
    start = None
    if run_dir_name:
        try:
            start = datetime.strptime(run_dir_name[:15], "%Y%m%d_%H%M%S")
        except ValueError:
            start = None
    if start is None:
        try:
            start = datetime.strptime(points[0]["t"][:19], PLOT_TS_FORMAT)
        except ValueError:
            return None
    return max(0, int((newest - start).total_seconds()))

# Cache of each sensor's most-recently-resolved (run_dir_name, csv_path).
# /plot_data polls every few seconds per sensor, and on a slow/high-latency
# link each of the two directory listings it used to do on every single
# call (find the latest run folder, then find the latest CSV in it) can
# itself take seconds - multiplied by however many sensor plots are on
# screen, that backlog is what made the whole app feel frozen. The run
# folder and CSV filename don't actually change between polls within one
# flight, so only the first poll after a connection (or a fresh Run All
# Sensors start) pays that cost; every poll after that re-opens the same
# known path directly, falling back to a fresh lookup if that path ever
# stops working (self-healing against rotation, not just an optimization).
_plot_path_cache = {}


# ---------- Remote CSV cache: tail first, fetch only what is needed ----------
#
# Every live-plot poll and every Data Viewer click used to download the whole
# CSV over SFTP again. On the field link (Tailscale, often relayed) a plain
# paramiko read runs at ~0.1 MB/s because each 32 KB block is its own round
# trip, and prefetching only lifts that to ~0.5 MB/s - so even mirroring a
# file once is minutes of dead time on a multi-day run (a 1 Hz POPS log is
# ~1 MB per hour; merged_data several times that), with six plots queueing
# for it on one lock.
#
# So each file is mirrored *from the end*: the header line plus the newest
# chunk, which already covers hours of 1 Hz data. A longer time frame backs
# the mirror up only as far as its cutoff needs, "since start" and the Data
# Viewer (which need every row) back it up to the first data line, and once
# mirrored a region is never fetched again. The logger's CSVs are
# append-only, so later polls cost one `stat` plus the bytes added since.

_CSV_TAIL_CHUNK = 1 << 20          # first backward step: 1 MB (~1 h of 1 Hz data)
_CSV_BACKFILL_MAX_STEP = 16 << 20  # steps double up to this, to cut round trips
_CSV_HEADER_PROBE = 16 << 10       # the header line is always within the first 16 KB


class _RemoteCsv:
    """
    A remote CSV mirrored locally from the tail backwards.

    `header` is the header line; `text` holds complete data lines from byte
    offset `start` up to the last complete line in the file; `pending` is
    the raw bytes of a trailing line the sensor may still be writing.
    """
    __slots__ = ("path", "size", "mtime", "header", "data_start", "start",
                 "text", "pending", "last_used")

    def __init__(self, path):
        self.path = path
        self.size = 0            # bytes of the remote file accounted for
        self.mtime = None
        self.header = None       # header line including its newline
        self.data_start = 0      # offset of the first data line
        self.start = None        # offset where `text` begins (None: not mirrored yet)
        self.text = ""
        self.pending = b""
        self.last_used = 0.0

    @property
    def complete(self):
        return self.start is not None and self.start <= self.data_start


_remote_csv_cache = {}          # remote path -> _RemoteCsv
_REMOTE_CSV_CACHE_MAX = 12      # a run has ~7 files; keep a little history too


def _evict_remote_csv_cache():
    while len(_remote_csv_cache) > _REMOTE_CSV_CACHE_MAX:
        oldest = min(_remote_csv_cache.values(), key=lambda e: e.last_used)
        _remote_csv_cache.pop(oldest.path, None)


def _sftp_read_range(sftp, path, offset, length):
    """Bytes [offset, offset+length) of the remote file; pipelined when large."""
    if length <= 0:
        return b""
    with sftp.open(path, "rb") as f:
        f.seek(offset)
        if length > 65536:
            # Queue the block requests up front instead of one round trip
            # per 32 KB. file_size caps the prefetch at the end of our range.
            f.prefetch(offset + length)
        return f.read(length)


def _csv_first_field(line):
    comma = line.find(",")
    return (line[:comma] if comma != -1 else line).strip()


def _csv_first_ts(text):
    nl = text.find("\n")
    return _csv_first_field(text[:nl] if nl != -1 else text) or None


def _csv_last_ts(text):
    if not text:
        return None
    nl = text.rfind("\n", 0, len(text) - 1)
    return _csv_first_field(text[nl + 1:]) or None


def _mirror_append(entry, data):
    """Append raw bytes read from the file's end; keeps a partial last line aside."""
    chunk = entry.pending + data
    cut = chunk.rfind(b"\n")
    if cut == -1:
        entry.pending = chunk
    else:
        entry.text += chunk[:cut + 1].decode("utf-8", errors="replace")
        entry.pending = chunk[cut + 1:]


def _mirror_backfill(entry, sftp, new_start):
    """Extend the mirror backwards so that it begins at `new_start` (line-aligned)."""
    new_start = max(new_start, entry.data_start)
    if entry.start is None or new_start >= entry.start:
        return
    block = _sftp_read_range(sftp, entry.path, new_start, entry.start - new_start)
    if new_start > entry.data_start:
        # The block starts mid-line: drop up to and including the first newline.
        cut = block.find(b"\n")
        if cut == -1:
            return
        new_start += cut + 1
        block = block[cut + 1:]
    entry.text = block.decode("utf-8", errors="replace") + entry.text
    entry.start = new_start


def _get_remote_csv(sftp, path, need_from_s=None, need_all=False):
    """
    Return the mirror of `path`, brought up to date with the file on the Pi
    and covering at least: the header and the newest lines (always); every
    line within `need_from_s` seconds before the newest one (if given); the
    whole file (if need_all). Caller must hold state["lock"] (paramiko's
    SFTP client is not thread-safe). Raises IOError if the file does not
    exist; any other exception means the transport is dead and the caller
    should _mark_disconnected() as it would for a plain read.
    """
    st = sftp.stat(path)
    entry = _remote_csv_cache.get(path)
    if entry is None or st.st_size < entry.size or (st.st_size == entry.size and entry.mtime != st.st_mtime):
        entry = _RemoteCsv(path)           # new, truncated, or rewritten: start over
        _remote_csv_cache[path] = entry
        _evict_remote_csv_cache()
    entry.last_used = time.time()

    if entry.header is None:
        probe = _sftp_read_range(sftp, path, 0, min(st.st_size, _CSV_HEADER_PROBE))
        nl = probe.find(b"\n")
        if nl == -1:
            return entry                   # no complete header yet; try again next poll
        entry.header = probe[:nl + 1].decode("utf-8", errors="replace")
        entry.data_start = nl + 1
        tail_start = max(entry.data_start, st.st_size - _CSV_TAIL_CHUNK)
        tail = _sftp_read_range(sftp, path, tail_start, st.st_size - tail_start)
        if tail_start > entry.data_start:
            cut = tail.find(b"\n")
            if cut == -1:                  # one giant partial line: hold it all
                tail_start = st.st_size
                entry.pending, tail = tail, b""
            else:
                tail_start += cut + 1
                tail = tail[cut + 1:]
        entry.start = tail_start
        _mirror_append(entry, tail)
        entry.size, entry.mtime = st.st_size, st.st_mtime
    elif st.st_size > entry.size:
        _mirror_append(entry, _sftp_read_range(sftp, path, entry.size, st.st_size - entry.size))
        entry.size, entry.mtime = st.st_size, st.st_mtime

    if need_all:
        _mirror_backfill(entry, sftp, entry.data_start)
    elif need_from_s is not None and not entry.complete:
        newest = _csv_last_ts(entry.text)
        cutoff = None
        if newest:
            try:
                cutoff = (datetime.strptime(newest[:19], PLOT_TS_FORMAT)
                          - timedelta(seconds=need_from_s)).strftime(PLOT_TS_FORMAT)
            except ValueError:
                cutoff = None
        if cutoff is not None:
            step = _CSV_TAIL_CHUNK
            while not entry.complete:
                first = _csv_first_ts(entry.text)
                if first is not None and first[:19] < cutoff:
                    break                  # mirror already reaches past the cutoff
                _mirror_backfill(entry, sftp, entry.start - step)
                step = min(step * 2, _CSV_BACKFILL_MAX_STEP)
    return entry


# /plot_data parses each sensor's mirror incrementally too: the points list
# is kept per sensor and only lines added since the last poll are parsed. A
# backfill moves the mirror's start, which resets that sensor's list once.
_plot_points_cache = {}         # sensor -> {path, start, consumed, time_idx, value_idx, points}
_plot_points_lock = threading.Lock()


def _plot_points(sensor, csv_path, header, start, text, spec):
    """
    (points, error) for `sensor` from a mirror snapshot. points is the whole
    mirrored series oldest-first (windowing happens afterwards); error is a
    message when the configured column is not in this file.
    """
    time_col = spec.get("time_column", "Timestamp")
    value_col = spec["value_column"]
    scale = spec["scale"]
    with _plot_points_lock:
        pc = _plot_points_cache.get(sensor)
        if pc is None or pc["path"] != csv_path or pc["start"] != start or pc["consumed"] > len(text):
            pc = {"path": csv_path, "start": start, "consumed": 0,
                  "time_idx": None, "value_idx": None, "points": []}
            _plot_points_cache[sensor] = pc
        if pc["time_idx"] is None and header:
            cols = next(csv.reader(io.StringIO(header)), [])
            if time_col in cols and value_col in cols:
                pc["time_idx"] = cols.index(time_col)
                pc["value_idx"] = cols.index(value_col)
            else:
                pc["time_idx"] = pc["value_idx"] = -1
        if pc["time_idx"] == -1:
            return [], f'Column "{value_col}" is not in {os.path.basename(csv_path)} (older run?).'
        if pc["time_idx"] is not None and pc["consumed"] < len(text):
            ti, vi = pc["time_idx"], pc["value_idx"]
            need = max(ti, vi)
            pts = pc["points"]
            for row in csv.reader(io.StringIO(text[pc["consumed"]:])):
                if len(row) <= need:
                    continue
                ts = row[ti]
                raw_val = row[vi]
                if not ts or not raw_val:
                    continue
                try:
                    pts.append({"t": ts, "v": float(raw_val) * scale})
                except ValueError:
                    continue
            pc["consumed"] = len(text)
        return pc["points"], None

PLOT_SENSORS = {
    "pom": {
        "label": "Ozone (POM)",
        "csv_prefix": "pom_data_",
        "value_column": "Ozone_ppb",
        "unit": "ppb",
        "scale": 1.0,
    },
    "imet": {
        "label": "Temperature (iMet)",
        "csv_prefix": "imet_data_",
        "value_column": "temp",
        "unit": "°C",
        # The iMet's own PTU sentence reports temperature as a signed
        # fixed-point integer in hundredths of a degree (e.g. "+2530" means
        # 25.30C) and the logger writes that field straight through
        # unscaled, so it has to be divided back down here.
        "scale": 0.01,
    },
    # The logger already scales rel_hum to percent (device sends %RH x 10),
    # so unlike temp above it needs no scale factor here.
    "imet_rh": {
        "label": "Relative Humidity (iMet)",
        "csv_prefix": "imet_data_",
        "value_column": "rel_hum",
        "unit": "%",
        "scale": 1.0,
    },
    "cavity": {
        "label": "Inline Pressure (Cavity)",
        "csv_prefix": "cavity_data_",
        "value_column": "pressure_mb",
        "unit": "mb",
        # The Arduino firmware itself converts to millibars before sending
        # over UART, so this one is already a plain float - no scaling.
        "scale": 1.0,
    },
    # These four come from the vitals_summary_*.csv the vitals exporter
    # writes at ~1Hz in the run folder root (not the per-sensor csv/
    # subfolder) - its short column names (TECA, TECT, RHi, I) are already
    # plain floats, and the I column is simply empty while the spectrometer
    # isn't running, which the row parser below skips naturally.
    "teca": {
        "label": "Tec Current",
        "csv_prefix": "vitals_summary_",
        "value_column": "TECA",
        "unit": "A",
        "scale": 1.0,
        "in_run_root": True,
    },
    "tect": {
        "label": "Tec Temperature",
        "csv_prefix": "vitals_summary_",
        "value_column": "TECT",
        "unit": "°C",
        "scale": 1.0,
        "in_run_root": True,
    },
    "rhi": {
        "label": "RH inline",
        "csv_prefix": "vitals_summary_",
        "value_column": "RHi",
        "unit": "%",
        "scale": 1.0,
        "in_run_root": True,
    },
    "intensity": {
        "label": "Max Intensity",
        "csv_prefix": "vitals_summary_",
        "value_column": "I",
        "unit": "counts",
        "scale": 1.0,
        "in_run_root": True,
    },
    "trisonica": {
        "label": "Wind Speed (Trisonica)",
        "csv_prefix": "trisonica_data_",
        "value_column": "Wind_Speed",
        "unit": "m/s",
        "scale": 1.0,
    },
    # PM2.5 is the logger-derived mass column (uri_aplogger/derived.py:
    # histogram bins x assumed density / sampled volume), written on every
    # POPS row right after the raw b0..b15 bins. PM1_ug_m3 sits next to it
    # if the 1 um cut is ever preferred here.
    "pops": {
        "label": "PM2.5 (POPS)",
        "csv_prefix": "pops_data_",
        "value_column": "PM2.5_ug_m3",
        "unit": "µg/m³",
        "scale": 1.0,
    },
    "pops_pm1": {
        "label": "PM1 (POPS)",
        "csv_prefix": "pops_data_",
        "value_column": "PM1_ug_m3",
        "unit": "µg/m³",
        "scale": 1.0,
    },
    "pops_conc": {
        "label": "Particle Conc. (POPS)",
        "csv_prefix": "pops_data_",
        "value_column": "PartCon",
        "unit": "#/cm³",
        "scale": 1.0,
    },
    "partector2pro": {
        "label": "Particle Conc. (Partector2Pro)",
        "csv_prefix": "partector2pro_data_",
        "value_column": "number_cm3",
        "unit": "#/cm³",
        "scale": 1.0,
    },
    # AAE_fit is the logger's own 5-wavelength absorption Angstrom exponent
    # (uri_aplogger/derived.py), appended to every MA200 row; dimensionless.
    # The instrument's own smoothed "AAE" column is also in the CSV.
    "miniaeth": {
        "label": "Ångström exp. (MiniAeth)",
        "csv_prefix": "miniaeth_data_",
        "value_column": "AAE_fit",
        "unit": "",
        "scale": 1.0,
    },
    "miniaeth_bc": {
        "label": "Black Carbon (MiniAeth)",
        "csv_prefix": "miniaeth_data_",
        "value_column": "IR_BCc",
        "unit": "ng/m³",
        "scale": 1.0,
    },
    # Unlike every sensor above, drone telemetry doesn't live inside a
    # uri_aplogger run folder - it's written by the separate PSDK C app
    # straight into data_from_drone/ as flat telemetry_*.csv files, so it
    # needs the different "drone_telemetry" lookup in /plot_data below.
    # Its own CSV also spells the time column "timestamp" (lowercase),
    # unlike every uri_aplogger sensor's "Timestamp".
    "altitude_agl": {
        "label": "Altitude AGL (Drone)",
        "unit": "m",
        "scale": 1.0,
        "value_column": "altitude_agl_m",
        "time_column": "timestamp",
        "source": "drone_telemetry",
    },
}

DRONE_TELEMETRY_DIR = f"{REMOTE_DIR}/data_from_drone"


def _find_latest_run_dir(sftp, remote_dir):
    """
    Returns the name of the most recent output/<timestamp> run folder, or
    None if there isn't one yet. Run folders are named with a sortable
    YYYYMMDD_HHMMSS timestamp, so lexical order is chronological order -
    same assumption /runall_logs relies on via `ls | sort | tail -1`.
    """
    try:
        entries = sftp.listdir(f"{remote_dir}/output")
    except IOError:
        return None
    return sorted(entries)[-1] if entries else None


def _find_latest_sensor_csv(sftp, remote_dir, run_dir_name, csv_prefix, in_run_root=False):
    # Per-sensor CSVs live in the run's csv/ subfolder; the vitals summary
    # sits directly in the run folder root next to merged_data_*.csv.
    if in_run_root:
        csv_dir = f"{remote_dir}/output/{run_dir_name}"
    else:
        csv_dir = f"{remote_dir}/output/{run_dir_name}/csv"
    try:
        entries = sftp.listdir(csv_dir)
    except IOError:
        return None
    matches = sorted(e for e in entries if e.startswith(csv_prefix) and e.endswith(".csv"))
    return f"{csv_dir}/{matches[-1]}" if matches else None


def _find_latest_drone_telemetry_csv(sftp):
    """
    Returns the path to the most recently *written* telemetry_*.csv in
    data_from_drone/, or None if there isn't one yet. That folder is flat
    (no per-run subfolders like uri_aplogger/output), so this can't reuse
    _find_latest_run_dir/_find_latest_sensor_csv.

    Picked by mtime rather than the timestamp embedded in the filename -
    unlike the uri_aplogger run folder names (a trustworthy sort key since
    runall.py stamps them itself right when it starts), a handful of these
    files carry stale filename timestamps left over from before the Pi's
    clock was fixed, so mtime is the only reliable way to find the file
    actually being written to by the current flight.
    """
    try:
        entries = sftp.listdir_attr(DRONE_TELEMETRY_DIR)
    except IOError:
        return None
    matches = [e for e in entries if e.filename.startswith("telemetry_") and e.filename.endswith(".csv")]
    if not matches:
        return None
    latest = max(matches, key=lambda e: e.st_mtime)
    return f"{DRONE_TELEMETRY_DIR}/{latest.filename}"


@app.route("/plot_data")
def plot_data():
    """
    Reads the requested sensor's CSV from the current/most recent run
    folder and returns its most recent samples, for the live-plot dropdown.
    Trimming to PLOT_MAX_POINTS keeps the response small even as a long
    flight's CSV grows - a real-time plot only needs the recent window.
    """
    sensor = (request.args.get("sensor") or "").strip()
    spec = PLOT_SENSORS.get(sensor)
    if not spec:
        return jsonify({"ok": False, "error": f"Unknown sensor: {sensor}"}), 400

    # Optional time frame in seconds (0 = whole run). Absent -> legacy
    # fixed sample count, so older clients keep working unchanged.
    window_raw = request.args.get("window")
    window_s = None
    if window_raw is not None:
        try:
            window_s = max(0, int(float(window_raw)))
        except ValueError:
            return jsonify({"ok": False, "error": f"Bad window: {window_raw!r}"}), 400

    with state["lock"]:
        if not state["connected"] or not state["sftp"]:
            return jsonify({"ok": False, "error": "Not connected to remote host."}), 400

        sftp = state["sftp"]
        cached = _plot_path_cache.get(sensor)
        run_dir_name = cached["run_dir_name"] if cached else None
        csv_path = cached["csv_path"] if cached else None
        entry = None

        # What the mirror must cover for this request: a bit more than the
        # window so its first point sits before the cutoff; "since start"
        # (0) needs every row; a legacy call without a window gets an hour.
        need_all = window_s == 0
        need_from_s = None if need_all else (3600 if window_s is None else window_s + 60)

        if csv_path:
            try:
                entry = _get_remote_csv(sftp, csv_path, need_from_s, need_all)
            except IOError:
                # Cached path went stale (run rotated, file moved/renamed) -
                # drop it and fall through to a fresh lookup below rather
                # than treating this as a connection failure.
                _plot_path_cache.pop(sensor, None)
                csv_path = None
            except Exception as e:
                # Anything other than IOError here (SSHException, EOFError,
                # a dropped socket, ...) means the transport itself is dead.
                _mark_disconnected()
                return jsonify({"ok": False, "error": f"Could not read {csv_path}: {e}"}), 500

        if entry is None:
            if spec.get("source") == "drone_telemetry":
                csv_path = _find_latest_drone_telemetry_csv(sftp)
                if not csv_path:
                    return jsonify({"ok": False, "error": "No drone telemetry CSV found yet in data_from_drone."}), 404
            else:
                remote_dir = f"{REMOTE_DIR}/{RUN_ALL_SUBDIR}"
                run_dir_name = _find_latest_run_dir(sftp, remote_dir)
                if not run_dir_name:
                    return jsonify({"ok": False, "error": "No run folder found yet - start Run All Sensors first."}), 404

                csv_path = _find_latest_sensor_csv(
                    sftp, remote_dir, run_dir_name, spec["csv_prefix"],
                    in_run_root=spec.get("in_run_root", False),
                )
                if not csv_path:
                    return jsonify({"ok": False, "error": f"No {spec['label']} CSV found yet in this run."}), 404

            try:
                entry = _get_remote_csv(sftp, csv_path, need_from_s, need_all)
            except Exception as e:
                _mark_disconnected()
                return jsonify({"ok": False, "error": f"Could not read {csv_path}: {e}"}), 500

            _plot_path_cache[sensor] = {"run_dir_name": run_dir_name, "csv_path": csv_path}

        # str is immutable: grab a snapshot and parse outside the SFTP lock
        # so other requests are not held up by CPU work.
        header, start, text = entry.header, entry.start, entry.text

    points, col_err = _plot_points(sensor, csv_path, header, start, text, spec)
    if col_err:
        return jsonify({"ok": False, "error": col_err}), 404

    run_span_s = _run_span_s(run_dir_name, points)
    points, total_in_window = _window_points(points, window_s)

    return jsonify({
        "ok": True,
        "sensor": sensor,
        "label": spec["label"],
        "unit": spec["unit"],
        "run_dir": run_dir_name,
        "csv_file": os.path.basename(csv_path),
        "window_s": window_s,
        "total_in_window": total_in_window,
        "run_span_s": run_span_s,
        "points": points,
    })


# ---------- Data Viewer (browse every run folder on the Pi) ----------

DATA_VIEWER_PAGE_SIZE = 50
_column_numeric_cache = {}      # one entry: the file currently being paged through

# Run folders are plain timestamps (e.g. "20260706_185920") written by
# runall.py - this is deliberately strict (not just "no dotdot") since
# both this and the file relpath validator below feed straight into an
# SFTP path built from client-controlled query params.
_RUN_NAME_RE = re.compile(r"^[A-Za-z0-9_\-]+$")


def _valid_run_name(run_name):
    return bool(run_name) and bool(_RUN_NAME_RE.match(run_name))


def _valid_csv_relpath(relpath):
    """
    Only allows the shapes real files actually live at: a bare "<name>.csv"
    (a run folder's root - vitals_summary/merged_data - or data_from_drone/,
    both flat), or "csv/<name>.csv" in a run's csv/ subfolder (per-sensor
    logs) - see _find_latest_sensor_csv above, which reads from these same
    locations. Anything else (nested paths, "..", backslashes) is rejected
    before it ever reaches sftp.open().
    """
    if not relpath or not relpath.endswith(".csv") or ".." in relpath or "\\" in relpath:
        return False
    parts = relpath.split("/")
    if len(parts) == 1:
        return True
    return len(parts) == 2 and parts[0] == "csv"


@app.route("/data_viewer/runs")
def data_viewer_runs():
    """
    Lists every run folder under uri_aplogger/output on the Pi, newest
    first - the full flight history, not just the latest run /plot_data
    reads from.
    """
    with state["lock"]:
        if not state["connected"] or not state["sftp"]:
            return jsonify({"ok": False, "error": "Not connected to remote host."}), 400
        sftp = state["sftp"]
        output_dir = f"{REMOTE_DIR}/{RUN_ALL_SUBDIR}/output"
        try:
            entries = sftp.listdir(output_dir)
        except IOError:
            return jsonify({"ok": True, "runs": []})

        runs = []
        for name in entries:
            if not _valid_run_name(name):
                continue
            try:
                if stat.S_ISDIR(sftp.stat(f"{output_dir}/{name}").st_mode):
                    runs.append(name)
            except IOError:
                continue

    return jsonify({"ok": True, "runs": sorted(runs, reverse=True)})


def _list_run_csv_files(sftp, run_dir):
    """
    Returns [(relpath, size_bytes), ...] for every .csv file directly in
    the run folder root (vitals_summary/merged_data) and in its csv/
    subfolder (per-sensor logs) - the same two locations
    _find_latest_sensor_csv already knows about.
    """
    files = []
    try:
        for name in sftp.listdir(run_dir):
            if name.endswith(".csv"):
                files.append((name, sftp.stat(f"{run_dir}/{name}").st_size))
    except IOError:
        pass
    try:
        for name in sftp.listdir(f"{run_dir}/csv"):
            if name.endswith(".csv"):
                files.append((f"csv/{name}", sftp.stat(f"{run_dir}/csv/{name}").st_size))
    except IOError:
        pass
    return sorted(files)


@app.route("/data_viewer/files")
def data_viewer_files():
    run_name = (request.args.get("run") or "").strip()
    if not _valid_run_name(run_name):
        return jsonify({"ok": False, "error": "Invalid run name."}), 400

    with state["lock"]:
        if not state["connected"] or not state["sftp"]:
            return jsonify({"ok": False, "error": "Not connected to remote host."}), 400
        run_dir = f"{REMOTE_DIR}/{RUN_ALL_SUBDIR}/output/{run_name}"
        files = _list_run_csv_files(state["sftp"], run_dir)

    return jsonify({
        "ok": True,
        "run": run_name,
        "files": [{"path": path, "size": size} for path, size in files],
    })


@app.route("/data_viewer/drone_latest")
def data_viewer_drone_latest():
    """
    Just the *name* of the latest data_from_drone telemetry file, if any -
    a single directory listing, no file content read - so the "add
    variable" picker can offer it as a source instantly. Used to be
    folded into a since-removed /data_viewer/run_columns that read every
    file in a run (plus this one) up front in a single request: fine on
    a fast connection, but on a slow/high-latency link (SSH file reads
    here have been observed taking upwards of 15-30s *each*) that turned
    "open a run" into a multi-minute wait before the picker showed
    anything at all. Column info is now only ever fetched for the one
    file the user actually picks (see /data_viewer/table), same as this
    endpoint - listing is cheap, reading isn't.
    """
    with state["lock"]:
        if not state["connected"] or not state["sftp"]:
            return jsonify({"ok": False, "error": "Not connected to remote host."}), 400
        latest = _find_latest_drone_telemetry_csv(state["sftp"])

    return jsonify({"ok": True, "file": os.path.basename(latest) if latest else None})


def _remote_csv_path(run_name, relpath, source="run"):
    """
    Resolves and validates the full remote path for a run or drone CSV,
    without touching SFTP. Split out of _read_remote_csv so the
    assistant's health-check tool (below) can also stat a file's mtime
    without reading (and re-parsing) its full content just to answer
    "is this still alive."

    Returns (path, None) on success, or (None, error_response) if
    relpath (or run_name, unless source="drone") doesn't pass
    validation.
    """
    if not _valid_csv_relpath(relpath):
        return None, (jsonify({"ok": False, "error": "Invalid file path."}), 400)
    if source == "drone":
        return f"{DRONE_TELEMETRY_DIR}/{relpath}", None
    if not _valid_run_name(run_name):
        return None, (jsonify({"ok": False, "error": "Invalid run name."}), 400)
    return f"{REMOTE_DIR}/{RUN_ALL_SUBDIR}/output/{run_name}/{relpath}", None


def _read_remote_csv(run_name, relpath, source="run"):
    """
    Shared by the table and plot endpoints: validates run/file, reads
    the CSV over SFTP, and returns its raw decoded text. Returns
    (raw_text, None) on success or (None, (json_response, status)) on
    any failure, so callers can just `return err` and stop.

    source="drone" reads a flat file straight out of data_from_drone/
    instead of a run's output/<run_name>/ folder - drone telemetry isn't
    tied to any particular uri_aplogger run, so it needs its own base
    path and skips the run-name check entirely.
    """
    csv_path, err = _remote_csv_path(run_name, relpath, source)
    if err:
        return None, err

    with state["lock"]:
        if not state["connected"] or not state["sftp"]:
            return None, (jsonify({"ok": False, "error": "Not connected to remote host."}), 400)
        try:
            entry = _get_remote_csv(state["sftp"], csv_path, need_all=True)
            raw = (entry.header or "") + entry.text
        except IOError as e:
            return None, (jsonify({"ok": False, "error": f"Could not read {relpath}: {e}"}), 404)
        except Exception as e:
            _mark_disconnected()
            return None, (jsonify({"ok": False, "error": f"Could not read {relpath}: {e}"}), 500)

    return raw, None


def _read_remote_csv_tail(run_name, relpath, source, seconds):
    """
    Like _read_remote_csv but only the last `seconds` of the file (header
    included). The assistant's live tools look at *recent* readings, so
    they must not drag a multi-day file over the link and then parse all
    of it: this reads just the mirror's tail (see _get_remote_csv).
    Returns (raw_text, None) or (None, error_response).
    """
    csv_path, err = _remote_csv_path(run_name, relpath, source)
    if err:
        return None, err

    with state["lock"]:
        if not state["connected"] or not state["sftp"]:
            return None, (jsonify({"ok": False, "error": "Not connected to remote host."}), 400)
        try:
            entry = _get_remote_csv(state["sftp"], csv_path, need_from_s=seconds)
            return (entry.header or "") + entry.text, None
        except IOError as e:
            return None, (jsonify({"ok": False, "error": f"Could not read {relpath}: {e}"}), 404)
        except Exception as e:
            _mark_disconnected()
            return None, (jsonify({"ok": False, "error": f"Could not read {relpath}: {e}"}), 500)


def _remote_csv_mtime(run_name, relpath, source="run"):
    """
    Returns (mtime, None) - a file's last-modified time as a Unix
    timestamp - or (None, error_response). Used by the assistant's
    health-check tool to flag a sensor that's stopped writing, without
    needing to read the file's full content just to answer "is this
    still alive."
    """
    csv_path, err = _remote_csv_path(run_name, relpath, source)
    if err:
        return None, err

    with state["lock"]:
        if not state["connected"] or not state["sftp"]:
            return None, (jsonify({"ok": False, "error": "Not connected to remote host."}), 400)
        try:
            mtime = state["sftp"].stat(csv_path).st_mtime
        except Exception as e:
            _mark_disconnected()
            return None, (jsonify({"ok": False, "error": f"Could not stat {relpath}: {e}"}), 500)

    return mtime, None


def _compute_column_numeric(columns, data_rows):
    """
    A column counts as numeric (and so is worth offering as something to
    plot) if at least half of its non-empty values parse as a float -
    checked against every row, not just one page, since many of these
    CSVs (especially merged_data's per-sensor columns) are mostly blank
    except when that specific sensor happened to report on that
    particular merged row. Shared by /data_viewer/table and the offline
    merged-data endpoints (/offline/session/<name>/merged/<date>/*).
    """
    column_numeric = []
    for col_idx in range(len(columns)):
        seen = numeric = 0
        for row in data_rows:
            if col_idx >= len(row):
                continue
            val = row[col_idx].strip()
            if not val:
                continue
            seen += 1
            try:
                # float() treats "_" as a digit-group separator (PEP
                # 515), so e.g. a run-folder-name-shaped value like
                # "20260813_151443" (see the merged CSVs' _source_run
                # column) would otherwise silently parse as a number.
                # Real sensor readings never contain underscores.
                if "_" in val:
                    raise ValueError
                float(val)
                numeric += 1
            except ValueError:
                pass
        column_numeric.append(seen > 0 and (numeric / seen) >= 0.5)
    return column_numeric


@app.route("/data_viewer/table")
def data_viewer_table():
    """
    Returns one page (DATA_VIEWER_PAGE_SIZE rows) of the requested CSV,
    plus its full column list and, for each column, whether it's numeric -
    computed over every row, since the whole file is already in memory to
    compute total_rows/total_pages anyway.
    """
    run_name = (request.args.get("run") or "").strip()
    relpath = (request.args.get("file") or "").strip()
    source = (request.args.get("source") or "run").strip()
    try:
        page = max(1, int(request.args.get("page", "1")))
    except ValueError:
        page = 1

    raw, err = _read_remote_csv(run_name, relpath, source=source)
    if err:
        return err

    all_rows = list(csv.reader(io.StringIO(raw)))
    if not all_rows:
        return jsonify({
            "ok": True, "run": run_name, "file": relpath,
            "columns": [], "column_numeric": [], "rows": [],
            "total_rows": 0, "page": 1, "page_size": DATA_VIEWER_PAGE_SIZE, "total_pages": 1,
        })

    columns = all_rows[0]
    data_rows = all_rows[1:]
    total_rows = len(data_rows)
    total_pages = max(1, math.ceil(total_rows / DATA_VIEWER_PAGE_SIZE))
    page = min(page, total_pages)
    start = (page - 1) * DATA_VIEWER_PAGE_SIZE
    page_rows = data_rows[start:start + DATA_VIEWER_PAGE_SIZE]
    # Paging through a 50k-row file re-ran this full scan per click;
    # remember it per (file, length) so only new rows ever cost anything.
    numeric_key = (source, run_name, relpath, len(raw))
    column_numeric = _column_numeric_cache.get(numeric_key)
    if column_numeric is None:
        column_numeric = _compute_column_numeric(columns, data_rows)
        _column_numeric_cache.clear()
        _column_numeric_cache[numeric_key] = column_numeric

    return jsonify({
        "ok": True,
        "run": run_name,
        "file": relpath,
        "columns": columns,
        "column_numeric": column_numeric,
        "rows": page_rows,
        "total_rows": total_rows,
        "page": page,
        "page_size": DATA_VIEWER_PAGE_SIZE,
        "total_pages": total_pages,
    })


@app.route("/data_viewer/plot")
def data_viewer_plot():
    """
    Returns every parseable (timestamp, value) pair for one column of
    the requested CSV - full resolution, not trimmed like /plot_data's
    live view, since this is a static historical file rather than an
    ever-growing live one. The first column is always used as the time
    axis; every real CSV these tools write (vitals_summary, merged_data,
    and each per-sensor log) leads with a Timestamp-like column.
    """
    run_name = (request.args.get("run") or "").strip()
    relpath = (request.args.get("file") or "").strip()
    column = (request.args.get("column") or "").strip()
    source = (request.args.get("source") or "run").strip()

    raw, err = _read_remote_csv(run_name, relpath, source=source)
    if err:
        return err
    if not column:
        return jsonify({"ok": False, "error": "A column is required."}), 400

    reader = csv.DictReader(io.StringIO(raw))
    fieldnames = reader.fieldnames or []
    if column not in fieldnames:
        return jsonify({"ok": False, "error": f'Column "{column}" not found in this file.'}), 400
    time_column = fieldnames[0]

    points = []
    max_decimals = 0
    for row in reader:
        ts = row.get(time_column)
        raw_val = (row.get(column) or "").strip()
        if not ts or not raw_val:
            continue
        try:
            value = float(raw_val)
        except ValueError:
            continue
        points.append({"t": ts, "v": value})
        if "." in raw_val:
            max_decimals = max(max_decimals, min(4, len(raw_val.split(".")[-1])))

    return jsonify({
        "ok": True,
        "run": run_name,
        "file": relpath,
        "column": column,
        "time_column": time_column,
        "points": points,
        "decimals": max_decimals,
    })


@app.route("/data_viewer/plot_multi")
def data_viewer_plot_multi():
    """
    Same idea as /data_viewer/plot but for several columns of one file at
    once, read from a single SFTP fetch - used by the Data Viewer's overlay
    plot so picking N columns costs one round trip instead of N.
    """
    run_name = (request.args.get("run") or "").strip()
    relpath = (request.args.get("file") or "").strip()
    columns = [c for c in (request.args.get("columns") or "").split(",") if c]
    source = (request.args.get("source") or "run").strip()

    raw, err = _read_remote_csv(run_name, relpath, source=source)
    if err:
        return err
    if not columns:
        return jsonify({"ok": False, "error": "At least one column is required."}), 400

    reader = csv.DictReader(io.StringIO(raw))
    fieldnames = reader.fieldnames or []
    for column in columns:
        if column not in fieldnames:
            return jsonify({"ok": False, "error": f'Column "{column}" not found in this file.'}), 400
    time_column = fieldnames[0]

    series_points = {column: [] for column in columns}
    series_decimals = {column: 0 for column in columns}
    for row in reader:
        ts = row.get(time_column)
        if not ts:
            continue
        for column in columns:
            raw_val = (row.get(column) or "").strip()
            if not raw_val:
                continue
            try:
                value = float(raw_val)
            except ValueError:
                continue
            series_points[column].append({"t": ts, "v": value})
            if "." in raw_val:
                series_decimals[column] = max(series_decimals[column], min(4, len(raw_val.split(".")[-1])))

    return jsonify({
        "ok": True,
        "run": run_name,
        "file": relpath,
        "time_column": time_column,
        "series": [
            {"column": column, "points": series_points[column], "decimals": series_decimals[column]}
            for column in columns
        ],
    })


@app.route("/run_c_app", methods=["POST"])
def run_c_app():
    with state["lock"]:
        if not state["connected"] or not state["ssh"]:
            return jsonify({"ok": False, "error": "Not connected to remote host."}), 400

        try:
            success, message, log_tail = _run_remote_c_app(state["ssh"], state["password"])
        except Exception as e:
            _mark_disconnected()
            return jsonify({"ok": False, "error": f"Could not start C app: {e}"}), 500

        if not success:
            return jsonify({
                "ok": False,
                "error": message or "Failed to start C app.",
                "output": log_tail,
            }), 500

        return jsonify({
            "ok": True,
            "message": message,
            "remote_log": RUN_C_APP_LOG,
            "output": log_tail,
        })


def _local_file_matches(local_path, remote_size):
    """
    True if local_path already exists and is the same size as the remote
    file - the signal this app uses to decide a file doesn't need
    re-downloading. These logger CSVs are append-only, so a size match is
    a reliable (and cheap - no extra SFTP round trip beyond the listing we
    already fetch) stand-in for "unchanged".
    """
    try:
        return os.path.getsize(local_path) == remote_size
    except OSError:
        return False


def sftp_count_files(sftp, remote_dir, local_dir=None, force=False):
    """
    Recursively count files (not directories) under remote_dir that still
    need downloading - i.e. don't already exist locally with a matching
    size (see _local_file_matches). Pass local_dir=None, or force=True, to
    count every file unconditionally (force=True is how "full copy" backups
    ignore what's already on the drive). Returns 0 if remote_dir doesn't
    exist (missing sources don't error here; that's handled the same way
    during the actual download).
    """
    count = 0
    try:
        entries = sftp.listdir_attr(remote_dir)
    except (FileNotFoundError, IOError):
        return 0

    for entry in entries:
        remote_path = f"{remote_dir}/{entry.filename}"
        local_path = os.path.join(local_dir, entry.filename) if local_dir else None
        if stat.S_ISDIR(entry.st_mode):
            count += sftp_count_files(sftp, remote_path, local_path, force=force)
        elif force or not (local_path and _local_file_matches(local_path, entry.st_size)):
            count += 1
    return count


def sftp_download_dir(sftp, remote_dir, local_dir, on_file_done=None, force=False):
    """
    Recursively download remote_dir (and all its contents) into local_dir.
    Unless force=True, skips any file that already exists locally with a
    matching size (see _local_file_matches) - so downloading the same
    target folder again only pulls new or changed files instead of the
    whole tree. force=True re-downloads and overwrites everything
    regardless (a "full copy" backup rather than an incremental one).
    Returns (downloaded_count, skipped_count, error_or_none). Skips
    silently if remote_dir doesn't exist (treated as 0 files, no error) so
    a missing source doesn't block the others. Calls on_file_done() after
    each successful download, for progress tracking.
    """
    downloaded = 0
    skipped = 0

    try:
        entries = sftp.listdir_attr(remote_dir)
    except FileNotFoundError:
        return 0, 0, None
    except IOError:
        return 0, 0, None

    os.makedirs(local_dir, exist_ok=True)

    for entry in entries:
        remote_path = f"{remote_dir}/{entry.filename}"
        local_path = os.path.join(local_dir, entry.filename)

        if stat.S_ISDIR(entry.st_mode):
            sub_down, sub_skip, err = sftp_download_dir(sftp, remote_path, local_path, on_file_done, force=force)
            if err:
                return downloaded, skipped, err
            downloaded += sub_down
            skipped += sub_skip
        elif not force and _local_file_matches(local_path, entry.st_size):
            skipped += 1
        else:
            try:
                sftp.get(remote_path, local_path)
                downloaded += 1
                if on_file_done:
                    on_file_done()
            except Exception as e:
                return downloaded, skipped, f"Failed downloading {remote_path}: {e}"

    return downloaded, skipped, None


def sftp_count_named_files(remote_sizes, local_dir, force=False):
    """
    Given [(filename, size), ...] already known from an earlier
    listdir_attr call (see _list_remote_drone_files_by_date), counts how
    many still need downloading - no further SFTP round trip needed
    since the sizes are already in hand. The flat-folder counterpart to
    sftp_count_files, for the same reason sftp_download_named_files
    exists alongside sftp_download_dir: data_from_drone/ mixes every
    date's telemetry together, so a day-scoped download only wants
    specific files out of it, not the whole folder.
    """
    count = 0
    for filename, size in remote_sizes:
        local_path = os.path.join(local_dir, filename) if local_dir else None
        if force or not (local_path and _local_file_matches(local_path, size)):
            count += 1
    return count


def sftp_download_named_files(sftp, remote_dir, remote_sizes, local_dir, on_file_done=None, force=False):
    """
    Downloads only the given [(filename, size), ...] out of remote_dir
    (not everything in it) - see sftp_count_named_files above for why.
    Same skip-if-already-matches-by-size and force semantics as
    sftp_download_dir; sizes are passed in rather than re-stat'd since
    the caller already has them from listing the folder once.
    """
    downloaded = 0
    skipped = 0
    os.makedirs(local_dir, exist_ok=True)
    for filename, size in remote_sizes:
        remote_path = f"{remote_dir}/{filename}"
        local_path = os.path.join(local_dir, filename)
        if not force and _local_file_matches(local_path, size):
            skipped += 1
            continue
        try:
            sftp.get(remote_path, local_path)
            downloaded += 1
            if on_file_done:
                on_file_done()
        except Exception as e:
            return downloaded, skipped, f"Failed downloading {remote_path}: {e}"
    return downloaded, skipped, None


@app.route("/check_download_folder", methods=["POST"])
def check_download_folder():
    data = request.get_json()
    name = (data.get("name") or "").strip()

    if not name:
        return jsonify({"ok": False, "error": "Folder name is required."}), 400
    if any(c in name for c in ("/", "\\", "..")):
        return jsonify({"ok": False, "error": "Folder name contains invalid characters."}), 400

    target = os.path.join(DOWNLOADS_ROOT, name)
    exists = os.path.isdir(target)
    return jsonify({"ok": True, "exists": exists, "path": target})


# ---------- Downloading specific days ----------
# "Download Data"/"Backup to External Drive" can pull everything (the
# flow above) or just a chosen subset of dates - each date landing in
# its own <destination>/<date>/ folder rather than one folder the user
# names, so a day is immediately browsable on its own (same folder shape
# the offline exploration page already expects a "session" to have) and
# re-picking the same day later just tops up that same folder.

def _list_remote_run_dirs_by_date(sftp):
    """
    SFTP equivalent of _list_run_dirs_by_date (which does this over local
    disk for an already-downloaded session) - groups uri_aplogger/output's
    run folders on the Pi by the date embedded in their name.
    """
    output_dir = f"{REMOTE_DIR}/{RUN_ALL_SUBDIR}/output"
    by_date = {}
    try:
        entries = sftp.listdir(output_dir)
    except IOError:
        return by_date
    for name in entries:
        m = _RUN_DIR_RE.match(name)
        if not m:
            continue
        try:
            if not stat.S_ISDIR(sftp.stat(f"{output_dir}/{name}").st_mode):
                continue
        except IOError:
            continue
        by_date.setdefault(m.group(1), []).append(name)
    for runs in by_date.values():
        runs.sort()
    return by_date


def _list_remote_drone_files_by_date(sftp):
    """
    SFTP equivalent of _list_drone_files_by_date - groups
    data_from_drone/telemetry_<date>_<time>.csv files on the Pi by date,
    keeping each file's size from the same listdir_attr call so a
    day-scoped download doesn't need a second per-file stat round trip
    later. Returns {date: [(filename, size), ...]}.
    """
    by_date = {}
    try:
        entries = sftp.listdir_attr(DRONE_TELEMETRY_DIR)
    except IOError:
        return by_date
    for entry in entries:
        m = _DRONE_FILE_RE.match(entry.filename)
        # The drone's onboard clock defaults to 2021 until it gets a GPS
        # fix, so pre-fix telemetry is stamped with a bogus 2021 date -
        # exclude it rather than show it as a selectable date.
        if not m or m.group(1).startswith("2021"):
            continue
        by_date.setdefault(m.group(1), []).append((entry.filename, entry.st_size))
    for files in by_date.values():
        files.sort()
    return by_date


# build_remote_path() names each day's notes file remote_pc_notes_<date>.csv
# (one file per day - see its own docstring), which conveniently means
# notes are already inherently date-scoped, unlike the rest of
# remote_ssh_notes/ might suggest at a glance.
_NOTES_FILE_RE = re.compile(r"^remote_pc_notes_(\d{8})\.csv$")


def _list_remote_notes_files_by_date(sftp):
    """
    SFTP listing of remote_ssh_notes/remote_pc_notes_<date>.csv, keyed by
    the date in each filename - same shape as
    _list_remote_drone_files_by_date, so a day-scoped download can
    include that date's notes file the same way it includes that date's
    drone telemetry. Returns {date: [(filename, size), ...]} (at most one
    entry per date in practice, but a list for symmetry with the drone
    lookup and sftp_count_named_files/sftp_download_named_files's shared shape).
    """
    by_date = {}
    try:
        entries = sftp.listdir_attr(f"{REMOTE_DIR}/{NOTES_SUBFOLDER}")
    except IOError:
        return by_date
    for entry in entries:
        m = _NOTES_FILE_RE.match(entry.filename)
        if not m:
            continue
        by_date.setdefault(m.group(1), []).append((entry.filename, entry.st_size))
    for files in by_date.values():
        files.sort()
    return by_date


@app.route("/download_days")
def download_days():
    """
    Lists every date that currently has sensor runs, drone telemetry,
    and/or a notes file on the Pi, newest first - what the "specific
    days" picker in the download/backup modals offers. Also reports
    whether this machine already has a downloaded_outputs/<date>/ folder
    for that date, so the picker can show what's already been pulled
    down at least once (independent of whether a *backup* destination
    has it - that's a different, external location this app has no way
    to check without knowing which drive/folder to look at).
    """
    with state["lock"]:
        if not state["connected"] or not state["sftp"]:
            return jsonify({"ok": False, "error": "Not connected to remote host."}), 400
        sftp = state["sftp"]
        by_date = _list_remote_run_dirs_by_date(sftp)
        drone_by_date = _list_remote_drone_files_by_date(sftp)
        notes_by_date = _list_remote_notes_files_by_date(sftp)

    days = []
    for date in sorted(set(by_date) | set(drone_by_date) | set(notes_by_date), reverse=True):
        days.append({
            "date": date,
            "run_count": len(by_date.get(date, [])),
            "drone_file_count": len(drone_by_date.get(date, [])),
            "has_notes": bool(notes_by_date.get(date)),
            "downloaded_locally": os.path.isdir(os.path.join(DOWNLOADS_ROOT, date)),
        })
    return jsonify({"ok": True, "days": days})


def _run_days_download_job(sftp, dates, base_root, kind):
    """
    Same background-thread shape as _run_download_job (Phase 1 count,
    Phase 2 download, both under state["lock"] since the SFTP channel
    isn't safe to share across threads) - but for a chosen subset of
    dates rather than everything. Each date gets its own
    <base_root>/<date>/ folder (base_root is downloaded_outputs/ for
    "Download Data", or the chosen external volume/subfolder for
    "Backup to External Drive" - same job either way, just like
    _run_download_job's target_root already varies between those two).

    Pulls in that date's sensor runs, drone telemetry, and notes file -
    each one already knows how to name itself back to a date (see
    _list_remote_run_dirs_by_date/_list_remote_drone_files_by_date/
    _list_remote_notes_files_by_date), so nothing here is guessing.

    Always incremental (no force option, unlike _run_download_job) -
    the entire point of a day folder being named after its date is that
    re-picking the same day later tops it up rather than starting over.
    """
    try:
        with state["lock"]:
            output_dir = f"{REMOTE_DIR}/{RUN_ALL_SUBDIR}/output"
            notes_dir = f"{REMOTE_DIR}/{NOTES_SUBFOLDER}"
            by_date = _list_remote_run_dirs_by_date(sftp)
            drone_by_date = _list_remote_drone_files_by_date(sftp)
            notes_by_date = _list_remote_notes_files_by_date(sftp)

            # Phase 1: count files still needed across every selected date,
            # for the progress bar denominator - same reasoning as
            # _run_download_job's own Phase 1.
            total = 0
            plan = []
            for date in dates:
                target_root = os.path.join(base_root, date)
                runs = by_date.get(date, [])
                drone_files = drone_by_date.get(date, [])
                notes_files = notes_by_date.get(date, [])
                for run in runs:
                    total += sftp_count_files(sftp, f"{output_dir}/{run}", os.path.join(target_root, "output", run))
                if drone_files:
                    total += sftp_count_named_files(drone_files, os.path.join(target_root, "data_from_drone"))
                if notes_files:
                    total += sftp_count_named_files(notes_files, os.path.join(target_root, "remote_ssh_notes"))
                plan.append({
                    "date": date, "target_root": target_root,
                    "runs": runs, "drone_files": drone_files, "notes_files": notes_files,
                })

            with download_state["lock"]:
                download_state["total_files"] = total
                download_state["done_files"] = 0

            def on_file_done():
                with download_state["lock"]:
                    download_state["done_files"] += 1

            # Phase 2: download each date's runs, drone files, then notes
            results = []
            for entry in plan:
                date, target_root = entry["date"], entry["target_root"]
                run_downloaded = run_skipped = 0
                drone_downloaded = drone_skipped = 0
                notes_downloaded = notes_skipped = 0
                err = None

                for run in entry["runs"]:
                    with download_state["lock"]:
                        download_state["current_source"] = f"{date} - {run}"
                    d, s, e = sftp_download_dir(sftp, f"{output_dir}/{run}", os.path.join(target_root, "output", run), on_file_done)
                    run_downloaded += d
                    run_skipped += s
                    if e:
                        err = e
                        break

                if not err and entry["drone_files"]:
                    with download_state["lock"]:
                        download_state["current_source"] = f"{date} - drone telemetry"
                    d, s, e = sftp_download_named_files(
                        sftp, DRONE_TELEMETRY_DIR, entry["drone_files"],
                        os.path.join(target_root, "data_from_drone"), on_file_done,
                    )
                    drone_downloaded += d
                    drone_skipped += s
                    if e:
                        err = e

                if not err and entry["notes_files"]:
                    with download_state["lock"]:
                        download_state["current_source"] = f"{date} - notes"
                    d, s, e = sftp_download_named_files(
                        sftp, notes_dir, entry["notes_files"],
                        os.path.join(target_root, "remote_ssh_notes"), on_file_done,
                    )
                    notes_downloaded += d
                    notes_skipped += s
                    if e:
                        err = e

                results.append({
                    "date": date, "target_root": target_root,
                    "run_downloaded": run_downloaded, "run_skipped": run_skipped,
                    "drone_downloaded": drone_downloaded, "drone_skipped": drone_skipped,
                    "notes_downloaded": notes_downloaded, "notes_skipped": notes_skipped,
                    "error": err,
                })

        with download_state["lock"]:
            download_state["results"] = results
            download_state["current_source"] = None
            download_state["running"] = False

    except Exception as e:
        with download_state["lock"]:
            download_state["error"] = f"{kind.capitalize()} job failed: {e}"
            download_state["running"] = False


def _start_days_download_job(sftp, dates, base_root, kind):
    """
    Same "is one already running" atomicity as _start_copy_job, for
    _run_days_download_job's multi-target-root shape instead of a single
    target_root.
    """
    with download_state["lock"]:
        if download_state["running"]:
            running_kind = download_state.get("kind") or "job"
            return False, f"A {running_kind} is already in progress."

        download_state["running"] = True
        download_state["kind"] = kind
        download_state["total_files"] = 0
        download_state["done_files"] = 0
        download_state["current_source"] = None
        download_state["results"] = None
        download_state["target_root"] = base_root
        download_state["error"] = None

    thread = threading.Thread(target=_run_days_download_job, args=(sftp, dates, base_root, kind), daemon=True)
    thread.start()
    return True, None


def _valid_dates(raw_dates):
    return [d for d in (raw_dates or []) if isinstance(d, str) and re.match(r"^\d{8}$", d)]


@app.route("/download_selected_days", methods=["POST"])
def download_selected_days():
    data = request.get_json() or {}
    dates = _valid_dates(data.get("dates"))
    if not dates:
        return jsonify({"ok": False, "error": "Pick at least one day."}), 400

    with state["lock"]:
        if not state["connected"] or not state["sftp"]:
            return jsonify({"ok": False, "error": "Not connected to remote host."}), 400
        sftp = state["sftp"]

        started, error = _start_days_download_job(sftp, dates, DOWNLOADS_ROOT, kind="download_days")
        if not started:
            return jsonify({"ok": False, "error": error}), 409

        return jsonify({"ok": True, "started": True})


def _run_download_job(sftp, target_root, name, force=False, kind="download"):
    """
    Runs in a background thread. Updates download_state as it goes.
    Holds state['lock'] for the whole job since the SFTP channel is shared
    with the notes feature and isn't safe to use from two threads at once.
    Shared by both "Download All Data" and "Backup to External Drive" -
    they're the same copy operation, just aimed at a different target_root
    (and "kind" is only here so the frontend knows which panel to update).
    """
    try:
        with state["lock"]:
            # Phase 1: count files across all sources that still need
            # downloading - i.e. don't already exist in target_root with a
            # matching size - for the progress bar denominator. Files
            # already present locally are excluded here rather than just
            # fast-forwarded through in phase 2, so the progress bar (and
            # "X / Y files") reflects the actual work left to do on a
            # repeat download into the same folder. force=True (a "full
            # copy" backup) counts everything instead.
            total = 0
            for remote_subpath, local_name in DOWNLOAD_SOURCES:
                remote_full = f"{REMOTE_DIR}/{remote_subpath}"
                local_full = os.path.join(target_root, local_name)
                total += sftp_count_files(sftp, remote_full, local_full, force=force)

            with download_state["lock"]:
                download_state["total_files"] = total
                download_state["done_files"] = 0

            def on_file_done():
                with download_state["lock"]:
                    download_state["done_files"] += 1

            # Phase 2: actually download each source, updating progress as files complete
            results = []
            for remote_subpath, local_name in DOWNLOAD_SOURCES:
                with download_state["lock"]:
                    download_state["current_source"] = remote_subpath

                remote_full = f"{REMOTE_DIR}/{remote_subpath}"
                local_full = os.path.join(target_root, local_name)

                downloaded, skipped, err = sftp_download_dir(sftp, remote_full, local_full, on_file_done, force=force)
                results.append({
                    "source": remote_subpath,
                    "local_folder": local_name,
                    "file_count": downloaded,
                    "skipped_count": skipped,
                    "error": err,
                })

        with download_state["lock"]:
            download_state["results"] = results
            download_state["current_source"] = None
            download_state["running"] = False

    except Exception as e:
        with download_state["lock"]:
            download_state["error"] = f"{kind.capitalize()} job failed: {e}"
            download_state["running"] = False


def _start_copy_job(sftp, target_root, name, force, kind):
    """
    Shared by /download_all and /backup_to_drive - both just kick off
    _run_download_job with a different target_root/force/kind. Checking
    "is one already running" and marking this one as running happens
    atomically under download_state["lock"] here (rather than a separate
    check-then-later-set, like the two routes used to each do
    independently), since both share the one SFTP connection and can't run
    concurrently. Returns (started, error_message_or_None).
    """
    with download_state["lock"]:
        if download_state["running"]:
            running_kind = download_state.get("kind") or "job"
            return False, f"A {running_kind} is already in progress."

        download_state["running"] = True
        download_state["kind"] = kind
        download_state["total_files"] = 0
        download_state["done_files"] = 0
        download_state["current_source"] = None
        download_state["results"] = None
        download_state["target_root"] = target_root
        download_state["error"] = None

    thread = threading.Thread(
        target=_run_download_job,
        args=(sftp, target_root, name),
        kwargs={"force": force, "kind": kind},
        daemon=True,
    )
    thread.start()
    return True, None


@app.route("/download_all", methods=["POST"])
def download_all():
    data = request.get_json()
    name = (data.get("name") or "").strip()

    if not name:
        return jsonify({"ok": False, "error": "Folder name is required."}), 400
    if any(c in name for c in ("/", "\\", "..")):
        return jsonify({"ok": False, "error": "Folder name contains invalid characters."}), 400

    with state["lock"]:
        if not state["connected"] or not state["sftp"]:
            return jsonify({"ok": False, "error": "Not connected to remote host."}), 400

        sftp = state["sftp"]
        target_root = os.path.join(DOWNLOADS_ROOT, name)

        try:
            os.makedirs(target_root, exist_ok=True)
        except Exception as e:
            return jsonify({"ok": False, "error": f"Could not create local folder: {e}"}), 500

        started, error = _start_copy_job(sftp, target_root, name, force=False, kind="download")
        if not started:
            return jsonify({"ok": False, "error": error}), 409

        return jsonify({"ok": True, "started": True})


@app.route("/download_progress")
def download_progress():
    with download_state["lock"]:
        return jsonify({
            "running": download_state["running"],
            "kind": download_state["kind"],
            "total_files": download_state["total_files"],
            "done_files": download_state["done_files"],
            "current_source": download_state["current_source"],
            "results": download_state["results"],
            "target_root": download_state["target_root"],
            "error": download_state["error"],
        })


def _list_external_volumes_macos():
    """
    Returns [{"name": ..., "path": ...}, ...] for every currently-mounted
    external (non-internal, non-network) volume on this Mac. Scans
    /Volumes and asks `diskutil info` about each entry rather than parsing
    the full disk list, since that's the simplest way to get a definite
    yes/no on "is this actually external" (Internal/NetworkVolume flags)
    plus its mount point in one call. Skips disk images (BusProtocol
    "Disk Image") too - a mounted .dmg isn't a real external drive worth
    offering here.

    Best-effort: any single volume that fails to inspect is just skipped
    rather than failing the whole list.
    """
    volumes = []
    try:
        entries = os.listdir("/Volumes")
    except OSError:
        return volumes

    for name in entries:
        path = f"/Volumes/{name}"
        if not os.path.isdir(path):
            continue
        try:
            result = subprocess.run(
                ["diskutil", "info", "-plist", path],
                capture_output=True, timeout=5,
            )
            if result.returncode != 0:
                continue
            info = plistlib.loads(result.stdout)
        except Exception:
            continue

        if info.get("Internal", True):
            continue
        if info.get("NetworkVolume", False):
            continue
        if info.get("BusProtocol") == "Disk Image":
            continue

        volumes.append({"name": info.get("VolumeName") or name, "path": path})

    return volumes


def _list_external_volumes_windows():
    """
    Returns [{"name": ..., "path": ...}, ...] for plausible backup drive
    targets on Windows: every removable drive (USB flash, SD cards -
    DRIVE_REMOVABLE) plus every non-system fixed drive (DRIVE_FIXED) -
    Windows commonly reports USB external HDDs/SSDs as "Fixed" rather than
    "Removable", so DRIVE_FIXED can't be excluded outright the way it can
    be assumed-internal on macOS. The boot/system drive (usually C:) is
    always excluded.

    This can't distinguish "external fixed drive" from "a second truly
    internal drive" as cleanly as macOS's diskutil Internal flag does -
    Windows doesn't expose that distinction as directly. That's an
    acceptable trade-off here: the result is only ever offered as labeled
    choices in a dropdown you pick from, never auto-selected, so an
    unlikely false positive (an internal secondary drive showing up as an
    option) is low-risk, not a silent write to the wrong place.

    Uses only the two most basic, decades-stable kernel32 calls
    (GetLogicalDrives/GetDriveTypeW) rather than anything more elaborate
    (WMI, the registry, GetVolumeInformationW for a friendly label) to
    keep this as simple and reliable as possible.
    """
    import ctypes
    import shutil
    import string

    DRIVE_REMOVABLE = 2
    DRIVE_FIXED = 3

    system_drive = (os.environ.get("SystemDrive") or "C:").upper()
    volumes = []
    try:
        bitmask = ctypes.windll.kernel32.GetLogicalDrives()
    except Exception:
        return volumes

    for i, letter in enumerate(string.ascii_uppercase):
        if not (bitmask >> i) & 1:
            continue
        drive = f"{letter}:"
        if drive == system_drive:
            continue
        root_path = f"{drive}\\"

        try:
            drive_type = ctypes.windll.kernel32.GetDriveTypeW(root_path)
        except Exception:
            continue
        if drive_type not in (DRIVE_REMOVABLE, DRIVE_FIXED):
            continue

        try:
            _total, _used, free = shutil.disk_usage(root_path)
        except OSError:
            # A drive letter can be assigned per GetLogicalDrives but not
            # actually ready (e.g. an empty card reader slot) - skip it
            # rather than offering a target nothing can actually be
            # written to.
            continue

        volumes.append({"name": f"{root_path} ({free / (1024 ** 3):.1f} GB free)", "path": root_path})

    return volumes


def _list_external_volumes():
    """
    Returns [{"name": ..., "path": ...}, ...] for the plausible "Backup to
    External Drive" targets on this machine - dispatches to a
    platform-specific implementation since Windows and macOS expose "what
    drives are attached" completely differently (drive letters + WinAPI
    calls vs. /Volumes + diskutil). Returns [] on any other platform
    (e.g. Linux - not supported yet) rather than raising.
    """
    if sys.platform == "win32":
        volumes = _list_external_volumes_windows()
    elif sys.platform == "darwin":
        volumes = _list_external_volumes_macos()
    else:
        volumes = []
    return sorted(volumes, key=lambda v: v["name"].lower())


@app.route("/external_volumes")
def external_volumes():
    return jsonify({"ok": True, "volumes": _list_external_volumes()})


def _common_destination_folders():
    """
    Returns [{"name": ..., "path": ...}, ...] for the handful of obvious
    "save a copy of the merged CSV here" targets on this machine -
    Desktop, Downloads, Documents, and the home folder itself - each only
    offered if it actually exists, since not every OS/account has all of
    them (e.g. a fresh Linux account may have no Desktop folder).
    """
    home = os.path.expanduser("~")
    candidates = [("Desktop", "Desktop"), ("Downloads", "Downloads"), ("Documents", "Documents"), ("Home folder", "")]
    folders = []
    for label, sub in candidates:
        path = os.path.join(home, sub) if sub else home
        if os.path.isdir(path):
            folders.append({"name": label, "path": path})
    return folders


@app.route("/offline/merge_destination_options")
def offline_merge_destination_options():
    """
    Quick-pick targets for the "Merge" modal's optional "also save a copy
    to..." folder - common folders under this user's home directory plus
    any currently-mounted external drives (same list "Backup to External
    Drive" uses), so picking a destination doesn't require typing a full
    path by hand for the common cases.
    """
    return jsonify({
        "ok": True,
        "common": _common_destination_folders(),
        "volumes": _list_external_volumes(),
    })


@app.route("/check_backup_folder", methods=["POST"])
def check_backup_folder():
    data = request.get_json()
    volume_path = (data.get("volume_path") or "").strip()
    name = (data.get("name") or "").strip()

    if not name:
        return jsonify({"ok": False, "error": "Folder name is required."}), 400
    if any(c in name for c in ("/", "\\", "..")):
        return jsonify({"ok": False, "error": "Folder name contains invalid characters."}), 400

    # Only ever write under a volume this Mac itself currently reports as a
    # mounted external drive - re-validated fresh against the live list
    # rather than trusting the path the browser sent, since it's about to
    # be used as a filesystem write target.
    valid_paths = {v["path"] for v in _list_external_volumes()}
    if volume_path not in valid_paths:
        return jsonify({"ok": False, "error": "That drive is no longer connected. Refresh the drive list and try again."}), 400

    target = os.path.join(volume_path, name)
    exists = os.path.isdir(target)
    return jsonify({"ok": True, "exists": exists, "path": target})


@app.route("/backup_to_drive", methods=["POST"])
def backup_to_drive():
    data = request.get_json()
    volume_path = (data.get("volume_path") or "").strip()
    name = (data.get("name") or "").strip()
    force = bool(data.get("force", False))

    if not name:
        return jsonify({"ok": False, "error": "Folder name is required."}), 400
    if any(c in name for c in ("/", "\\", "..")):
        return jsonify({"ok": False, "error": "Folder name contains invalid characters."}), 400

    valid_paths = {v["path"] for v in _list_external_volumes()}
    if volume_path not in valid_paths:
        return jsonify({"ok": False, "error": "That drive is no longer connected. Refresh the drive list and try again."}), 400

    with state["lock"]:
        if not state["connected"] or not state["sftp"]:
            return jsonify({"ok": False, "error": "Not connected to remote host."}), 400

        sftp = state["sftp"]
        target_root = os.path.join(volume_path, name)

        try:
            os.makedirs(target_root, exist_ok=True)
        except Exception as e:
            return jsonify({"ok": False, "error": f"Could not create folder on drive: {e}"}), 500

        started, error = _start_copy_job(sftp, target_root, name, force=force, kind="backup")
        if not started:
            return jsonify({"ok": False, "error": error}), 409

        return jsonify({"ok": True, "started": True})


# ---------- Backup straight from the Pi into the OneDrive backup folder ----------
# The connected dashboard's counterpart to "Backup to External Drive": the
# same copy jobs, aimed at the OneDrive destination chosen on the OneDrive
# page (<OneDrive>/<folder>/<name> for "all data", <OneDrive>/<folder>/<date>
# per day for "specific days" - the same layout the OneDrive page's local
# backup produces, so both routes converge on the same folders).

@app.route("/onedrive/pi_backup/status")
def onedrive_pi_backup_status():
    backup_dir = _onedrive_backup_dir()
    return jsonify({"ok": True, "backup_dir": backup_dir, "root": _onedrive_root(),
                    "folder": _onedrive_settings().get("folder") if backup_dir else None})


@app.route("/onedrive/pi_backup/check_folder", methods=["POST"])
def onedrive_pi_backup_check_folder():
    data = request.get_json() or {}
    name = (data.get("name") or "").strip()
    if not _valid_folder_name(name):
        return jsonify({"ok": False, "error": "Folder name contains invalid characters."}), 400
    backup_dir = _onedrive_backup_dir()
    if not backup_dir:
        return jsonify({"ok": False, "error": "No OneDrive destination is set - choose one on the OneDrive Backup page first."}), 400
    target = os.path.join(backup_dir, name)
    return jsonify({"ok": True, "exists": os.path.isdir(target), "path": target})


@app.route("/onedrive/pi_backup", methods=["POST"])
def onedrive_pi_backup():
    data = request.get_json() or {}
    scope = (data.get("scope") or "all").strip()
    backup_dir = _onedrive_backup_dir()
    if not backup_dir:
        return jsonify({"ok": False, "error": "No OneDrive destination is set - choose one on the OneDrive Backup page first."}), 400
    with _onedrive_job_lock:
        if _onedrive_job.get("running"):
            return jsonify({"ok": False, "error": "A local-to-OneDrive backup is running on the OneDrive page; wait for it to finish."}), 409

    with state["lock"]:
        if not state["connected"] or not state["sftp"]:
            return jsonify({"ok": False, "error": "Not connected to remote host."}), 400
        sftp = state["sftp"]

        if scope == "days":
            dates = _valid_dates(data.get("dates"))
            if not dates:
                return jsonify({"ok": False, "error": "Pick at least one day."}), 400
            started, error = _start_days_download_job(sftp, dates, backup_dir, kind="backup_days")
            if not started:
                return jsonify({"ok": False, "error": error}), 409
            return jsonify({"ok": True, "started": True, "target_root": backup_dir})

        name = (data.get("name") or "").strip()
        force = bool(data.get("force", False))
        if not _valid_folder_name(name):
            return jsonify({"ok": False, "error": "Folder name is required and must not contain path characters."}), 400
        target_root = os.path.join(backup_dir, name)
        try:
            os.makedirs(target_root, exist_ok=True)
        except Exception as e:
            return jsonify({"ok": False, "error": f"Could not create folder in OneDrive: {e}"}), 500
        started, error = _start_copy_job(sftp, target_root, name, force=force, kind="backup")
        if not started:
            return jsonify({"ok": False, "error": error}), 409
        return jsonify({"ok": True, "started": True, "target_root": target_root})


@app.route("/backup_selected_days", methods=["POST"])
def backup_selected_days():
    """
    "Backup to External Drive"'s counterpart to /download_selected_days -
    same _run_days_download_job, just aimed at the chosen drive
    (<volume_path>/<date>/ per date) instead of downloaded_outputs/, and
    with no subfolder-name step - same reasoning as the local flow, each
    date names its own folder.
    """
    data = request.get_json() or {}
    volume_path = (data.get("volume_path") or "").strip()
    dates = _valid_dates(data.get("dates"))

    if not dates:
        return jsonify({"ok": False, "error": "Pick at least one day."}), 400

    valid_paths = {v["path"] for v in _list_external_volumes()}
    if volume_path not in valid_paths:
        return jsonify({"ok": False, "error": "That drive is no longer connected. Refresh the drive list and try again."}), 400

    with state["lock"]:
        if not state["connected"] or not state["sftp"]:
            return jsonify({"ok": False, "error": "Not connected to remote host."}), 400
        sftp = state["sftp"]

        started, error = _start_days_download_job(sftp, dates, volume_path, kind="backup_days")
        if not started:
            return jsonify({"ok": False, "error": error}), 409

        return jsonify({"ok": True, "started": True})


@sock.route("/terminal_ws")
def terminal_ws(ws):
    """
    Bridges an xterm.js terminal in the browser to a real interactive shell
    on the Pi, over a dedicated channel on the existing SSH transport (the
    same login already used for everything else - no second password
    prompt). Deliberately not held under state["lock"]: a shell session can
    sit open indefinitely, and holding the lock for its whole lifetime
    would freeze every other endpoint (notes, pi_time, runall polling) for
    as long as the terminal stayed open.

    Message protocol (both directions, JSON text frames):
      {"type": "input", "data": "<keystrokes>"}   browser -> server
      {"type": "resize", "cols": N, "rows": N}     browser -> server
      {"type": "output", "data": "<shell bytes>"}  server -> browser
      {"type": "error", "data": "<message>"}       server -> browser
    """
    with state["lock"]:
        if not state["connected"] or not state["ssh"]:
            ws.send(json.dumps({"type": "error", "data": "Not connected to remote host.\r\n"}))
            return
        ssh = state["ssh"]

    try:
        channel = ssh.get_transport().open_session()
        channel.get_pty(term="xterm-256color", width=80, height=24)
        channel.invoke_shell()
    except Exception as e:
        try:
            ws.send(json.dumps({"type": "error", "data": f"Could not start terminal: {e}\r\n"}))
        except Exception:
            pass
        return

    channel.settimeout(0.0)
    stop_event = threading.Event()

    def reader():
        # Polls the channel instead of blocking on recv() so it can also
        # notice stop_event (set by the writer loop below when the browser
        # side closes) and exit promptly instead of blocking forever on a
        # shell that's just sitting idle.
        try:
            while not stop_event.is_set():
                if channel.recv_ready():
                    data = channel.recv(4096)
                    if not data:
                        break
                    ws.send(json.dumps({"type": "output", "data": data.decode("utf-8", errors="replace")}))
                elif channel.closed or channel.exit_status_ready():
                    break
                else:
                    time.sleep(0.02)
        except Exception:
            pass
        finally:
            stop_event.set()
            try:
                ws.close()
            except Exception:
                pass

    reader_thread = threading.Thread(target=reader, daemon=True)
    reader_thread.start()

    try:
        while not stop_event.is_set():
            try:
                message = ws.receive(timeout=1)
            except Exception:
                break
            if message is None:
                continue
            try:
                msg = json.loads(message)
            except Exception:
                continue
            msg_type = msg.get("type")
            if msg_type == "input":
                channel.send(msg.get("data", ""))
            elif msg_type == "resize":
                try:
                    cols = int(msg.get("cols", 80))
                    rows = int(msg.get("rows", 24))
                    channel.resize_pty(width=max(cols, 1), height=max(rows, 1))
                except Exception:
                    pass
    finally:
        stop_event.set()
        try:
            channel.close()
        except Exception:
            pass


CHECKABLE_SENSOR_SCRIPT = "sensor_runner.py"


@sock.route("/sensor_check_ws")
def sensor_check_ws(ws):
    """
    Streams live output from a single sensor's standalone sensor_runner.py
    process on the Pi - the exact same script/CLI runall.py itself launches
    per sensor, just run here as a lone foreground process instead of part
    of the full pipeline. For confirming a sensor is wired up and producing
    data (e.g. before a flight) without starting everything. Nothing is
    saved by this dashboard; this is a live view only, in the same spirit
    as terminal_ws.

    Two kinds of output get streamed, since sensor_runner.py's own stdout
    turns out to only be status/log lines (generic_sensor.py's write_data()
    logs a 3-field sample per row, not the full row - see "Written: ..."):
      - "output": the process's own stdout/stderr (startup messages,
        reconnect attempts, the truncated per-row sample log line, etc).
      - "data": actual full rows tailed straight from the CSV file the
        process is writing (its path is parsed out of that same stdout,
        from the "Output file: <path>" line generic_sensor.py logs right
        after creating it) - this is the real, complete sensor output the
        "output" stream alone doesn't show.

    The browser's first (and only expected) message must be
    {"sensor": "<name>"}; every server message after that is
    {"type": "output"|"data"|"error", "data": "..."}. A PTY is allocated
    for both the sensor process and the tail process specifically so that
    closing either channel (browser closes the socket, navigates away,
    etc.) delivers SIGHUP and actually kills them - a plain exec_command
    channel doesn't reliably do that, and leaving either running detached
    would keep the sensor's port (or the CSV file) tied up.

    Blocked entirely while runall.py is running: a second reader opening
    the same serial/USB port runall.py already has open is a real conflict,
    not just a formality - same reasoning as gating sensor_config/toggle.
    """
    with state["lock"]:
        if not state["connected"] or not state["ssh"] or not state["sftp"]:
            ws.send(json.dumps({"type": "error", "data": "Not connected to remote host.\r\n"}))
            return
        ssh = state["ssh"]
        sftp = state["sftp"]

    try:
        first_message = ws.receive(timeout=10)
        msg = json.loads(first_message) if first_message else {}
    except Exception:
        msg = {}

    sensor_name = (msg.get("sensor") or "").strip().lower()
    if not sensor_name:
        ws.send(json.dumps({"type": "error", "data": "No sensor specified.\r\n"}))
        return

    try:
        running, _pid = _check_runall_running(ssh)
    except Exception as e:
        try:
            ws.send(json.dumps({"type": "error", "data": f"Could not check runall.py status: {e}\r\n"}))
        except Exception:
            pass
        return
    if running:
        ws.send(json.dumps({"type": "error", "data": "Stop Run All Sensors before checking a sensor.\r\n"}))
        return

    try:
        with sftp.open(SENSOR_CONFIG_REMOTE_PATH, "r") as f:
            config = json.loads(f.read().decode("utf-8"))
    except Exception as e:
        ws.send(json.dumps({"type": "error", "data": f"Could not read sensor_config.json: {e}\r\n"}))
        return

    sensor_cfg = config.get("sensors", {}).get(sensor_name)
    if not isinstance(sensor_cfg, dict) or sensor_cfg.get("script") != CHECKABLE_SENSOR_SCRIPT:
        ws.send(json.dumps({"type": "error", "data": f'"{sensor_name}" cannot be checked this way.\r\n'}))
        return

    remote_dir = f"{REMOTE_DIR}/{RUN_ALL_SUBDIR}"
    command = (
        "cd " + shlex.quote(remote_dir) + " && exec "
        + shlex.quote(RUN_ALL_VENV_PYTHON) + " -u "
        + shlex.quote(CHECKABLE_SENSOR_SCRIPT) + " " + shlex.quote(sensor_name)
    )

    try:
        channel = ssh.get_transport().open_session()
        channel.get_pty()
        channel.exec_command(command)
    except Exception as e:
        try:
            ws.send(json.dumps({"type": "error", "data": f"Could not start check: {e}\r\n"}))
        except Exception:
            pass
        return

    channel.settimeout(0.0)
    stop_event = threading.Event()
    tail_channel_box = {"channel": None}

    OUTPUT_FILE_RE = re.compile(r"Output file: (\S+\.csv)")
    # Bounds how long the "output" stream is scanned for the "Output file:"
    # line before giving up - it normally shows up within the first couple
    # hundred bytes, right after the process starts. Without this cap, a
    # sensor that never reaches that line (e.g. it errors out first) would
    # keep this buffer growing for the whole life of the check.
    DETECT_SCAN_LIMIT = 4096

    def start_tailing(csv_relpath):
        csv_path = csv_relpath if csv_relpath.startswith("/") else f"{remote_dir}/{csv_relpath}"
        try:
            tail_channel = ssh.get_transport().open_session()
            tail_channel.get_pty()
            tail_channel.exec_command("tail -n +1 -F " + shlex.quote(csv_path))
            tail_channel.settimeout(0.0)
        except Exception:
            return
        tail_channel_box["channel"] = tail_channel

        def tail_reader():
            try:
                while not stop_event.is_set():
                    if tail_channel.recv_ready():
                        data = tail_channel.recv(4096)
                        if not data:
                            break
                        ws.send(json.dumps({"type": "data", "data": data.decode("utf-8", errors="replace")}))
                    elif tail_channel.closed or tail_channel.exit_status_ready():
                        break
                    else:
                        time.sleep(0.1)
            except Exception:
                pass

        threading.Thread(target=tail_reader, daemon=True).start()

    def reader():
        detect_buffer = ""
        tailing_started = False
        try:
            while not stop_event.is_set():
                if channel.recv_ready():
                    data = channel.recv(4096)
                    if not data:
                        break
                    text = data.decode("utf-8", errors="replace")
                    ws.send(json.dumps({"type": "output", "data": text}))

                    if not tailing_started:
                        detect_buffer += text
                        match = OUTPUT_FILE_RE.search(detect_buffer)
                        if match:
                            tailing_started = True
                            start_tailing(match.group(1))
                        elif len(detect_buffer) > DETECT_SCAN_LIMIT:
                            tailing_started = True  # give up looking
                elif channel.closed or channel.exit_status_ready():
                    break
                else:
                    time.sleep(0.05)
        except Exception:
            pass
        finally:
            stop_event.set()
            try:
                ws.close()
            except Exception:
                pass

    reader_thread = threading.Thread(target=reader, daemon=True)
    reader_thread.start()

    try:
        while not stop_event.is_set():
            try:
                message = ws.receive(timeout=1)
            except Exception:
                break
            if message is None:
                continue
            # There's nothing to send after the initial sensor pick - any
            # further message from the browser just means "stop".
            break
    finally:
        stop_event.set()
        try:
            channel.close()
        except Exception:
            pass
        tail_channel = tail_channel_box["channel"]
        if tail_channel:
            try:
                tail_channel.close()
            except Exception:
                pass


@app.route("/status")
def status():
    return jsonify({
        "connected": state["connected"],
        "host": state["host"],
        "username": state["username"],
        "remote_path": state["remote_path"],
    })


@app.route("/disconnect", methods=["POST"])
def disconnect():
    with state["lock"]:
        ssh, sftp = _detach_ssh_handles()
        state["connected"] = False
        state["remote_path"] = None
        state["host"] = None
        state["username"] = None
        state["password"] = None
    _close_ssh_handles_async(ssh, sftp)
    return jsonify({"ok": True})


def _find_chrome_windows():
    """
    Locates chrome.exe via the registry "App Paths" key Chrome's own
    installer registers - works regardless of whether it was installed
    per-machine (HKLM) or per-user (HKCU), unlike guessing at fixed
    Program Files paths (which also differ between the 32-bit and 64-bit
    install locations). Returns the full path, or None if Chrome isn't
    installed/registered there.
    """
    import winreg
    key_path = r"SOFTWARE\Microsoft\Windows\CurrentVersion\App Paths\chrome.exe"
    for hive in (winreg.HKEY_CURRENT_USER, winreg.HKEY_LOCAL_MACHINE):
        try:
            with winreg.OpenKey(hive, key_path) as key:
                path, _ = winreg.QueryValueEx(key, None)
        except OSError:
            continue
        if path and os.path.isfile(path):
            return path
    return None


def _open_in_chrome():
    """
    Opens http://127.0.0.1:5050 in a new Chrome tab specifically (not
    just whatever the default browser is). Falls back to the system
    default browser if Chrome isn't installed/registered/available.

    macOS: `tell application "chrome" to open location ...` (via
    webbrowser's own AppleScript backend) rather than `open -a
    "Google Chrome" url` - the AppleScript "open location" Apple Event
    is what makes Chrome open a fresh new tab in the frontmost window
    each time, instead of just re-focusing an existing matching tab.

    Windows: launching chrome.exe directly with the URL as an argument
    has the same effect - Chrome opens it as a new tab in its existing
    window rather than a new window, when one is already running.
    """
    url = "http://127.0.0.1:5050"

    if sys.platform == "win32":
        chrome_path = _find_chrome_windows()
        if chrome_path:
            try:
                subprocess.Popen([chrome_path, url])
                return
            except Exception:
                pass
        webbrowser.open(url)
        return

    try:
        webbrowser.get("chrome").open(url)
    except webbrowser.Error:
        webbrowser.open(url)


if __name__ == "__main__":
    # Delay slightly so the browser doesn't try to connect before the
    # Flask dev server is actually listening.
    threading.Timer(1.0, _open_in_chrome).start()
    # threaded=True matters now that /terminal_ws can hold a connection
    # open indefinitely (an interactive shell session) - without it, the
    # single-threaded dev server would serialize every request behind
    # whichever terminal happens to be open, freezing notes/pi_time/runall
    # polling for as long as that session lasts.
    app.run(host="127.0.0.1", port=5050, debug=False, threaded=True)
