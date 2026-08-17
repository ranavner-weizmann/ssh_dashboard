# MAMP Dashboard — Drone Air System

MAMP stands for Mobile Atmospheric Measurement Platform.

A local web dashboard that connects to a remote machine over SSH, creates a
timestamped notes CSV in `/home/rsp/drone_air_system/`, and appends each note
you write directly to that remote file in real time.

## Setup

1. Install dependencies:

```sh
pip install paramiko flask
```

2. Run the app:

```sh
python app.py
```

3. Open your browser to:

```sh
http://127.0.0.1:5050
```

## Usage

1. Enter the remote machine's **IP address**, **username**, and **password**.
2. Click **Connect & create notes file**. This will:
   - Open an SSH/SFTP connection to the remote host.
   - Confirm `/home/rsp/drone_air_system/` exists.
   - Confirm `/home/rsp/drone_air_system/remote_ssh_notes/` exists, creating
      it if it doesn't.
   - Create a new file there named `remote_pc_notes_YYYYMMDD_HHMMSS.csv`
      with a `timestamp,note` header row.

3. Type a note and click **Save note**. Each note is immediately appended
   as a new row (`timestamp,note`) to the CSV file on the remote machine
   over the open SSH connection — no need to re-upload or re-save the
   whole file.
4. Click **Run All Sensors** to start the remote `drone_air_system/uri_aplogger/runall.py`
   script using the remote virtual environment.
5. Click **Run C App** to start the remote `/home/rsp/Payload-SDK/build/bin/dji_sdk_demo_on_pi`
   binary.
6. Click **Disconnect** when you're done. This closes the SSH session.
   Reconnecting creates a *new* timestamped CSV file (so each session gets
   its own log).

## Download All Data

The dashboard has a **Download All Data** button that pulls three folders
from the remote machine down to your local computer:

- `drone_air_system/uri_aplogger/output`
- `drone_air_system/data_from_drone`
- `drone_air_system/remote_ssh_notes`

When you click it, you'll be asked to name a subfolder. Files are saved to:

```ini
downloaded_outputs/<your subfolder name>/
├── output/                 (from uri_aplogger/output)
├── data_from_drone/
└── remote_ssh_notes/
```

`downloaded_outputs/` is created automatically at the same level as the
`ssh_dashboard` app folder (i.e. one level up from `app.py`), the first time
you use this feature.

- If the subfolder name you choose already exists, the dashboard asks
   whether to **continue** into it or **choose a different name**.
- Downloads are incremental: a file already present locally with the same
   size as the remote copy is skipped, so re-downloading into the same
   subfolder only pulls new or changed files instead of starting over.
- A progress bar shows real progress: the app first counts the files that
   still need downloading (excluding ones already matched locally), then
   updates the bar as each one finishes (the browser polls the local app
   for status roughly twice a second).
- If one of the three remote source folders doesn't exist, it's skipped
   silently (0 files) rather than failing the whole download — check the
   per-folder results shown after the download finishes to confirm what
   actually came through.
- Only one download can run at a time; starting a second one while the
   first is still in progress is rejected until the first finishes.

## Notes on behavior

- **Authentication**: password-based, prompted fresh each time you connect.
   Nothing is stored to disk.
- **Single active session**: this app is built for one local user driving
   one remote connection at a time. Opening multiple browser tabs share the
   same backend connection.
- **Connection drops**: if the SSH session drops mid-session, saving a note
   will return an error in the dashboard rather than silently failing. You'll
   need to reconnect (which will start a fresh CSV file).
- __Directory check__: if `/home/rsp/drone_air_system/` doesn't exist on the
   remote machine, connecting will fail with a clear error instead of trying
   to create it — adjust `REMOTE_DIR` in `app.py` if your path differs.

## Files

- `app.py` — Flask server + SSH/SFTP logic (Paramiko)
- `templates/index.html` — the dashboard UI
