"""
SSH Dashboard
-------------
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
error.)

Can also be packaged as a double-clickable macOS .app with PyInstaller;
see build_app.sh / app.spec in the same project for the build steps.
"""

import csv
import io
import json
import math
import os
import plistlib
import re
import shlex
import stat
import subprocess
import sys
import threading
import time
import webbrowser
from datetime import datetime

import paramiko
from flask import Flask, render_template, request, jsonify
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


def _mark_disconnected():
    """
    Tears down the SSH/SFTP handles and marks the session as no longer
    connected. Call this from a route's `except` block whenever a remote
    command throws - on this kind of unattended field connection, that
    almost always means the physical link actually died (cable pulled, Pi
    lost power, out of Wi-Fi range), not a one-off command hiccup. Without
    this, state["connected"] just stays True forever after the transport is
    already dead, and the dashboard's status dot keeps lying about it.

    Callers must already hold state["lock"] - this doesn't acquire it
    itself, since most call sites are already inside a `with state["lock"]:`
    block and the lock is a plain non-reentrant threading.Lock.
    """
    if state["ssh"]:
        try:
            state["sftp"].close()
        except Exception:
            pass
        try:
            state["ssh"].close()
        except Exception:
            pass
    state["ssh"] = None
    state["sftp"] = None
    state["connected"] = False


def build_remote_path():
    ts = datetime.now().strftime("%Y%m%d")
    filename = f"remote_pc_notes_{ts}.csv"
    return f"{REMOTE_DIR}/{NOTES_SUBFOLDER}/{filename}", filename


@app.route("/")
def index():
    return render_template(
        "index.html",
        connected=state["connected"],
        host=state["host"],
        username=state["username"],
        remote_path=state["remote_path"],
    )


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
        "timedatectl show --property=NTP --property=NTPSynchronized; "
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
    "pops": {
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
    "miniaeth": {
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

    with state["lock"]:
        if not state["connected"] or not state["sftp"]:
            return jsonify({"ok": False, "error": "Not connected to remote host."}), 400

        sftp = state["sftp"]
        run_dir_name = None

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
            with sftp.open(csv_path, "r") as f:
                raw = f.read().decode("utf-8", errors="replace")
        except Exception as e:
            _mark_disconnected()
            return jsonify({"ok": False, "error": f"Could not read {csv_path}: {e}"}), 500

    reader = csv.DictReader(io.StringIO(raw))
    value_column = spec["value_column"]
    time_column = spec.get("time_column", "Timestamp")
    scale = spec["scale"]
    points = []
    for row in reader:
        ts = row.get(time_column)
        raw_val = row.get(value_column)
        if not ts or raw_val is None:
            continue
        try:
            value = float(raw_val) * scale
        except ValueError:
            continue
        points.append({"t": ts, "v": value})

    points = points[-PLOT_MAX_POINTS:]

    return jsonify({
        "ok": True,
        "sensor": sensor,
        "label": spec["label"],
        "unit": spec["unit"],
        "run_dir": run_dir_name,
        "csv_file": os.path.basename(csv_path),
        "points": points,
    })


# ---------- Data Viewer (browse every run folder on the Pi) ----------

DATA_VIEWER_PAGE_SIZE = 50

# Run folders are plain timestamps (e.g. "20260706_185920") written by
# runall.py - this is deliberately strict (not just "no dotdot") since
# both this and the file relpath validator below feed straight into an
# SFTP path built from client-controlled query params.
_RUN_NAME_RE = re.compile(r"^[A-Za-z0-9_\-]+$")


def _valid_run_name(run_name):
    return bool(run_name) and bool(_RUN_NAME_RE.match(run_name))


def _valid_csv_relpath(relpath):
    """
    Only allows the two shapes real files actually live at: a bare
    "<name>.csv" in the run folder's root (vitals_summary/merged_data),
    or "csv/<name>.csv" in its csv/ subfolder (per-sensor logs) - see
    _find_latest_sensor_csv above, which reads from these same two
    locations. Anything else (nested paths, "..", backslashes) is
    rejected before it ever reaches sftp.open().
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


def _read_remote_csv(run_name, relpath):
    """
    Shared by the table and plot endpoints: validates run/file, reads
    the CSV over SFTP, and returns its raw decoded text. Returns
    (raw_text, None) on success or (None, (json_response, status)) on
    any failure, so callers can just `return err` and stop.
    """
    if not _valid_run_name(run_name):
        return None, (jsonify({"ok": False, "error": "Invalid run name."}), 400)
    if not _valid_csv_relpath(relpath):
        return None, (jsonify({"ok": False, "error": "Invalid file path."}), 400)

    with state["lock"]:
        if not state["connected"] or not state["sftp"]:
            return None, (jsonify({"ok": False, "error": "Not connected to remote host."}), 400)
        csv_path = f"{REMOTE_DIR}/{RUN_ALL_SUBDIR}/output/{run_name}/{relpath}"
        try:
            with state["sftp"].open(csv_path, "r") as f:
                raw = f.read().decode("utf-8", errors="replace")
        except Exception as e:
            _mark_disconnected()
            return None, (jsonify({"ok": False, "error": f"Could not read {relpath}: {e}"}), 500)

    return raw, None


@app.route("/data_viewer/table")
def data_viewer_table():
    """
    Returns one page (DATA_VIEWER_PAGE_SIZE rows) of the requested CSV,
    plus its full column list and, for each column, whether it's
    numeric - computed over every row (not just this page), since the
    whole file is already in memory to compute total_rows/total_pages
    anyway. The frontend uses that flag to decide which columns are
    worth offering in the plot dropdown.
    """
    run_name = (request.args.get("run") or "").strip()
    relpath = (request.args.get("file") or "").strip()
    try:
        page = max(1, int(request.args.get("page", "1")))
    except ValueError:
        page = 1

    raw, err = _read_remote_csv(run_name, relpath)
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

    # A column counts as numeric (and so gets offered in the plot's
    # column dropdown) if at least half of its non-empty values parse as
    # a float. Checked against the whole file rather than just this
    # page - many of these CSVs (especially merged_data's per-sensor
    # columns) are mostly blank except when that specific sensor
    # happened to report on that particular merged row.
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
                float(val)
                numeric += 1
            except ValueError:
                pass
        column_numeric.append(seen > 0 and (numeric / seen) >= 0.5)

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

    raw, err = _read_remote_csv(run_name, relpath)
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


def _list_external_volumes():
    """
    Returns [{"name": ..., "path": ...}, ...] for every currently-mounted
    external (non-internal, non-network) volume on this Mac - i.e. the
    plausible targets for "Backup to External Drive". Scans /Volumes and
    asks `diskutil info` about each entry rather than parsing the full
    disk list, since that's the simplest way to get a definite yes/no on
    "is this actually external" (Internal/NetworkVolume flags) plus its
    mount point in one call. Skips disk images (BusProtocol "Disk Image")
    too - a mounted .dmg isn't a real external drive worth offering here.

    Best-effort: any single volume that fails to inspect is just skipped
    rather than failing the whole list, and this returns [] outright on
    any platform/environment where /Volumes or diskutil aren't there
    (non-macOS) instead of raising.
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

    return sorted(volumes, key=lambda v: v["name"].lower())


@app.route("/external_volumes")
def external_volumes():
    return jsonify({"ok": True, "volumes": _list_external_volumes()})


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
    return jsonify({"ok": True})


def _open_in_chrome():
    """
    Opens http://127.0.0.1:5050 in a new Chrome tab specifically (not
    just whatever the default browser is). Falls back to the system
    default browser if Chrome isn't registered/available.

    Uses `tell application "chrome" to open location ...` (via
    webbrowser's macOS AppleScript backend) rather than `open -a
    "Google Chrome" url` - the AppleScript "open location" Apple Event
    is what makes Chrome open a fresh new tab in the frontmost window
    each time, instead of just re-focusing an existing matching tab.
    """
    url = "http://127.0.0.1:5050"
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
