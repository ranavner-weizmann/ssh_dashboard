"""
SSH Dashboard
-------------
Connects to a remote machine over SSH (password auth), creates a timestamped
CSV file in /home/rsp/drone_air_system/, and lets you write notes from a
local web dashboard. Each note is appended live to the remote CSV over SFTP.
Also supports downloading the full data set (logger output, drone data,
and notes) from the remote machine into a local folder.

Run with:  python app.py
Then open: http://127.0.0.1:5000

Can also be packaged as a double-clickable macOS .app with PyInstaller;
see build_app.sh / app.spec in the same project for the build steps.
"""

import csv
import io
import os
import shlex
import stat
import sys
import threading
import webbrowser
from datetime import datetime

import paramiko
from flask import Flask, render_template, request, jsonify

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

app = Flask(__name__, template_folder=os.path.join(APP_DIR, "templates"))
app.secret_key = "drone-notes-local-secret"  # only used for local Flask session cookie

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

# Progress tracking for the "Download All Data" background job.
# Only one download job runs at a time (single-user local app).
download_state = {
    "running": False,
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


def _get_pi_time_info(ssh):
    """
    Reads the Pi's current date/time and whether NTP sync is active via
    timedatectl. Returns a dict; raises on SSH/command failure so callers
    can report a clear connection-level error.

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
    # wrong wall-clock time for the Pi.
    stdin, stdout, stderr = ssh.exec_command(
        "timedatectl show --property=NTP --property=NTPSynchronized; "
        "date '+%Y-%m-%d %H:%M:%S %Z'",
        timeout=10,
    )
    exit_status = stdout.channel.recv_exit_status()
    out_text = stdout.read().decode("utf-8", errors="replace")
    err_text = stderr.read().decode("utf-8", errors="replace").strip()

    if exit_status != 0:
        raise RuntimeError(err_text or "timedatectl/date command failed on the remote host.")

    info = {}
    pi_datetime_str = None
    for line in out_text.splitlines():
        line = line.strip()
        if not line:
            continue
        if "=" in line and not line[0].isdigit():
            key, _, value = line.partition("=")
            info[key.strip()] = value.strip()
        else:
            # The `date` output line: "2026-06-26 20:46:58 IDT"
            pi_datetime_str = line

    ntp_active = info.get("NTP") == "yes"
    ntp_synced = info.get("NTPSynchronized") == "yes"

    return {
        "pi_time": pi_datetime_str,
        "ntp_active": ntp_active,
        "ntp_synchronized": ntp_synced,
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

        pid = state.get("runall_pid")
        if not pid:
            return jsonify({"ok": False, "error": "No runall.py process is currently tracked."}), 400

        ssh = state["ssh"]

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
}


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


def _find_latest_sensor_csv(sftp, remote_dir, run_dir_name, csv_prefix):
    csv_dir = f"{remote_dir}/output/{run_dir_name}/csv"
    try:
        entries = sftp.listdir(csv_dir)
    except IOError:
        return None
    matches = sorted(e for e in entries if e.startswith(csv_prefix) and e.endswith(".csv"))
    return f"{csv_dir}/{matches[-1]}" if matches else None


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

        remote_dir = f"{REMOTE_DIR}/{RUN_ALL_SUBDIR}"
        sftp = state["sftp"]

        run_dir_name = _find_latest_run_dir(sftp, remote_dir)
        if not run_dir_name:
            return jsonify({"ok": False, "error": "No run folder found yet - start Run All Sensors first."}), 404

        csv_path = _find_latest_sensor_csv(sftp, remote_dir, run_dir_name, spec["csv_prefix"])
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
    scale = spec["scale"]
    points = []
    for row in reader:
        ts = row.get("Timestamp")
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


def sftp_count_files(sftp, remote_dir):
    """
    Recursively count files (not directories) under remote_dir.
    Returns 0 if remote_dir doesn't exist (missing sources don't error here;
    that's handled the same way during the actual download).
    """
    count = 0
    try:
        entries = sftp.listdir_attr(remote_dir)
    except (FileNotFoundError, IOError):
        return 0

    for entry in entries:
        remote_path = f"{remote_dir}/{entry.filename}"
        if stat.S_ISDIR(entry.st_mode):
            count += sftp_count_files(sftp, remote_path)
        else:
            count += 1
    return count


def sftp_download_dir(sftp, remote_dir, local_dir, on_file_done=None):
    """
    Recursively download remote_dir (and all its contents) into local_dir.
    Returns (file_count, error_or_none). Skips silently if remote_dir
    doesn't exist (treated as 0 files, no error) so a missing source
    doesn't block the others. Calls on_file_done() after each successful
    file download, for progress tracking.
    """
    file_count = 0

    try:
        entries = sftp.listdir_attr(remote_dir)
    except FileNotFoundError:
        return 0, None
    except IOError:
        return 0, None

    os.makedirs(local_dir, exist_ok=True)

    for entry in entries:
        remote_path = f"{remote_dir}/{entry.filename}"
        local_path = os.path.join(local_dir, entry.filename)

        if stat.S_ISDIR(entry.st_mode):
            sub_count, err = sftp_download_dir(sftp, remote_path, local_path, on_file_done)
            if err:
                return file_count, err
            file_count += sub_count
        else:
            try:
                sftp.get(remote_path, local_path)
                file_count += 1
                if on_file_done:
                    on_file_done()
            except Exception as e:
                return file_count, f"Failed downloading {remote_path}: {e}"

    return file_count, None


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


def _run_download_job(sftp, target_root, name):
    """
    Runs in a background thread. Updates download_state as it goes.
    Holds state['lock'] for the whole job since the SFTP channel is shared
    with the notes feature and isn't safe to use from two threads at once.
    """
    try:
        with state["lock"]:
            # Phase 1: count total files across all sources (for the progress bar denominator)
            total = 0
            for remote_subpath, _ in DOWNLOAD_SOURCES:
                remote_full = f"{REMOTE_DIR}/{remote_subpath}"
                total += sftp_count_files(sftp, remote_full)

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

                count, err = sftp_download_dir(sftp, remote_full, local_full, on_file_done)
                results.append({
                    "source": remote_subpath,
                    "local_folder": local_name,
                    "file_count": count,
                    "error": err,
                })

        with download_state["lock"]:
            download_state["results"] = results
            download_state["current_source"] = None
            download_state["running"] = False

    except Exception as e:
        with download_state["lock"]:
            download_state["error"] = f"Download job failed: {e}"
            download_state["running"] = False


@app.route("/download_all", methods=["POST"])
def download_all():
    data = request.get_json()
    name = (data.get("name") or "").strip()

    if not name:
        return jsonify({"ok": False, "error": "Folder name is required."}), 400
    if any(c in name for c in ("/", "\\", "..")):
        return jsonify({"ok": False, "error": "Folder name contains invalid characters."}), 400

    with download_state["lock"]:
        if download_state["running"]:
            return jsonify({"ok": False, "error": "A download is already in progress."}), 409

    with state["lock"]:
        if not state["connected"] or not state["sftp"]:
            return jsonify({"ok": False, "error": "Not connected to remote host."}), 400

        sftp = state["sftp"]
        target_root = os.path.join(DOWNLOADS_ROOT, name)

        try:
            os.makedirs(target_root, exist_ok=True)
        except Exception as e:
            return jsonify({"ok": False, "error": f"Could not create local folder: {e}"}), 500

        with download_state["lock"]:
            download_state["running"] = True
            download_state["total_files"] = 0
            download_state["done_files"] = 0
            download_state["current_source"] = None
            download_state["results"] = None
            download_state["target_root"] = target_root
            download_state["error"] = None

        thread = threading.Thread(target=_run_download_job, args=(sftp, target_root, name), daemon=True)
        thread.start()

        return jsonify({"ok": True, "started": True})


@app.route("/download_progress")
def download_progress():
    with download_state["lock"]:
        return jsonify({
            "running": download_state["running"],
            "total_files": download_state["total_files"],
            "done_files": download_state["done_files"],
            "current_source": download_state["current_source"],
            "results": download_state["results"],
            "target_root": download_state["target_root"],
            "error": download_state["error"],
        })



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


def _open_in_safari():
    """
    Opens http://127.0.0.1:5000 in Safari specifically (not just whatever
    the default browser is). Falls back to the system default browser if
    Safari isn't registered/available (e.g. running this on non-macOS).
    """
    url = "http://127.0.0.1:5000"
    try:
        webbrowser.get("safari").open(url)
    except webbrowser.Error:
        webbrowser.open(url)


if __name__ == "__main__":
    # Delay slightly so the browser doesn't try to connect before the
    # Flask dev server is actually listening.
    threading.Timer(1.0, _open_in_safari).start()
    app.run(host="127.0.0.1", port=5000, debug=False)
