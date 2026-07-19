# Partector 2 Pro — debugging notes (2026-07-19)

How the "Partector logs no data on the Pi" problem was diagnosed and fixed.
Keep this for future reference; the same techniques apply to any serial
sensor on the drone Pi.

## TL;DR — the fix

The Pi-side driver (`uri_aplogger/sensor_implementations.py`,
`Partector2ProSensor`) opened the port at **115200 baud. The Partector only
streams when the line coding is 9600 baud** — at anything else it
enumerates fine, may even answer identity queries, but never sends
measurement data. No error anywhere; it just stays silent. Because of this,
the sensor had **never** produced a data row (every historical
`partector2pro_data_*.csv` contained only the header line).

Changed to 9600 (matching naneos' own `naneos-devices` Python library) and
data flowed within seconds: one mode-6 size-distribution row every ~6 s.

## Device facts worth knowing

- Enumerates as USB id `ffff:0005`, manufacturer `LPCUSB`, product
  `LDSAmeter`, serial `DOSEMet` — nothing says "partector" or "naneos".
  - macOS: `/dev/cu.usbmodemDOSEMet_1`
  - Pi: `/dev/ttyACM1` (by-id: `usb-LPCUSB_LDSAmeter_DOSEMet-if00`)
- Protocol: ASCII commands terminated by `!` or `?` — **no `\r\n` needed**.
  - `N?` → serial number (8701), `f?` → firmware (396), `v?` → HW version
  - `X0000!` stop streaming, `X0002!` standard streaming,
    `X0006!` + `M0004!` pro mode / size distribution, `A0002!` antispikes
- It sends **nothing** until told to stream. Identity queries work even
  when streaming is off, which separates "device dead" from "streaming not
  enabled".
- It is battery powered: unplugging USB (or forcing re-enumeration in
  software) does **not** reboot it. Only its own power button does.

## Failure modes seen today, and how to recognize each

1. **Wrong baud (the root cause).** Port opens, init "succeeds", zero data
   forever. Check: does a manual test at 9600 stream? (see script below)
2. **Device firmware wedge.** After a lot of messy port access (two hosts,
   overlapping opens at different bauds) the instrument stopped answering
   even `N?` with the port completely free, and survived a software USB
   re-enumeration (`echo 0 > /sys/bus/usb/devices/<dev>/authorized`, then
   `1`). Only power-cycling the instrument revived it.
3. **Port contention.** Linux serial ports are NOT exclusive: a second
   process can open the port and silently steal incoming bytes, or send
   `X0000!` and stop the stream for everyone. On macOS a hung reader
   process blocks data the same way. Check: `lsof /dev/ttyACM1`
   (macOS: `lsof /dev/cu.usbmodemDOSEMet_1`).
4. **ModemManager.** Runs on the Pi and probes newly plugged ttyACM
   devices, eating data for up to ~1 min after plug-in. Wait, or disable:
   `sudo systemctl disable --now ModemManager`.

## Diagnostic techniques that worked

- **Is the runner even receiving bytes?** Without touching the port:
  `PID=$(pgrep -f 'sensor_runner.py partector2pro' | head -1); cat /proc/$PID/io`
  — sample `rchar` twice, 10 s apart. Flat = nothing arriving.
- **Has the sensor EVER worked?** Check historical outputs:
  `for f in output/*/csv/partector2pro*; do echo "$(wc -l < $f) $f"; done`
  All `1` = header-only = it never worked; stop blaming today's change.
- **Talk to the device directly** (stop the sensor first — note the
  bracket trick so pkill doesn't match its own shell):
  `pkill -f '[s]ensor_runner.py partector2pro'`

  ```python
  import serial, time
  s = serial.Serial("/dev/ttyACM1", 9600, timeout=2)
  time.sleep(0.5); s.reset_input_buffer()
  s.write(b"N?"); time.sleep(0.5)
  print("N? ->", s.read(100))          # identity: is the device alive?
  s.write(b"X0006!"); time.sleep(0.2)  # pro streaming
  s.write(b"M0004!")                   # size distribution
  for _ in range(10):
      print(s.readline())              # a data line within ~6 s = healthy
  s.close()
  ```
  The sensor controller respawns the runner automatically
  (`restart_delay: 5`), and the runner re-inits the device itself.
- **Force USB re-enumeration without touching hardware** (root):
  `echo 0 > /sys/bus/usb/devices/1-1.1.3/authorized; sleep 2; echo 1 > ...`
  (find the device path via `dmesg | grep -i acm`). Fixes kernel-side
  wedges; does not reboot the instrument itself.

## Changes made to the Pi code (backup: sensor_implementations.py.bak_20260719)

In `Partector2ProSensor`:

1. `baudrate = 9600` (was 115200) — the actual fix.
2. Commands sent bare, no `\r\n`.
3. `M0004!` sent explicitly at init — the device may not remember
   size-distribution mode across a power cycle.
4. `_did_startup` is reset whenever the connection drops, so a reconnect
   redoes the full `X0000!`/`X0006!`/`M0004!` startup instead of skipping
   it (it used to reconnect and then read nothing forever).
5. 60 s silence watchdog: if the port is open but no data arrives for a
   minute, log a warning, close, and re-init from scratch. This self-heals
   the wedge/contention/ModemManager cases above.

`sensor_config.json`: `partector2pro.baudrate` updated 115200 → 9600 to
match (the class value is what's actually used).

## Reading it on the Mac (for bench tests)

`naneos-devices` is installed in `air_pollution_collector/.venv`:

```python
from naneos.partector import partector2_pro
p = partector2_pro.Partector2Pro(port="/dev/cu.usbmodemDOSEMet_1")
# wait >= 20 s; pro mode emits roughly every 6-20 s
points = p.get_data()
p.close(blocking=True)   # NOTE: sends X0000! — stops streaming for everyone
```
