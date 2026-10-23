# Video memory checkpoint demo

Build with the main guide, section 8.2. This is a custom demo based on the official
python:3.11-slim-bookworm image, not a prepublished third-party video service.

Prepare JPEG frames at /data/video/frames (5 fps, sorted filenames) on the PVC.
GET / serves the browser UI; GET /state exposes boot_id/frame/paused/marker;
GET /frame/N serves a frame; POST /pause and POST /resume control server RAM.
The app never saves playback position or identity. A new process starts at zero.
Requests drive the clock; gaps longer than one second do not advance playback.
This is a single-viewer, silent frame-preview demo, not a production streaming server.

Pause, record state, disconnect browser polling and port-forward, then checkpoint.
After restore verify identical boot_id and frame before resuming. A fresh browser
must see the same server state. A cached image in the old tab is not proof.
No authentication is included; use localhost port-forward plus an SSH tunnel.

Sample media: Big Buck Bunny (c) 2008 Blender Foundation / www.bigbuckbunny.org,
CC BY 3.0, https://peach.blender.org/about/. Keep attribution for derived frames.
No movie binary is included. Tests: python test_app.py. Actual CRIU restore is
not covered by the local tests.
