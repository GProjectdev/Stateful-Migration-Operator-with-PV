"""Playback position exists only in RAM. Frames and attribution live on the PVC."""
import json
import os
import time
import uuid
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path


class Playback:
    def __init__(self, count, fps=5, clock=time.monotonic):
        self.count, self.fps, self.clock = count, fps, clock
        self.boot_id = str(uuid.uuid4())
        self.position = 0.0
        self.paused = False
        self.last = clock()

    def state(self):
        now = self.clock()
        delta = now - self.last
        # Do not skip ahead after a long disconnect or a restore clock jump.
        if not self.paused and 0 <= delta <= 1:
            self.position = min(self.count - 1, self.position + delta * self.fps)
        self.last = now
        frame = min(self.count - 1, int(self.position + 1e-8))
        return dict(boot_id=self.boot_id, frame=frame, seconds=frame / self.fps,
                    paused=self.paused, frame_count=self.count, fps=self.fps)

    def pause(self, value):
        self.state()
        self.paused = value
        return self.state()


HTML = '''<!doctype html><meta charset="utf-8"><title>Checkpoint Video Demo</title>
<style>body{background:#101827;color:#eee;font:18px sans-serif;margin:30px}img{width:640px;max-width:100%;background:#000}button{padding:12px;margin:8px}pre{white-space:pre-wrap}</style>
<h1>Checkpoint Video Demo</h1><p id="status">Connecting...</p>
<img id="screen" alt="Video frame"><br>
<button onclick="control('resume')">Play / Resume</button>
<button onclick="control('pause')">Pause for checkpoint</button>
<button onclick="connected=false">Disconnect for checkpoint</button>
<button onclick="connected=true">Reconnect</button>
<pre id="state"></pre><p>Big Buck Bunny (c) 2008 Blender Foundation / www.bigbuckbunny.org — CC BY 3.0. Silent 5 fps preview.</p>
<script>
let connected=true;
async function control(action){await fetch('/'+action,{method:'POST'});}
async function poll(){
 if(connected){try{
 const r=await fetch('/state',{cache:'no-store',signal:AbortSignal.timeout(2000)});
 if(!r.ok)throw Error(r.status);const s=await r.json();
 document.getElementById('state').textContent=JSON.stringify(s,null,2);
 document.getElementById('screen').src='/frame/'+s.frame+'?id='+s.boot_id;
 document.getElementById('status').textContent=s.paused?'PAUSED — server RAM state':'PLAYING — server RAM state';
 }catch(e){document.getElementById('status').textContent='DISCONNECTED — last displayed frame only (not recovery evidence)';}}
 else document.getElementById('status').textContent='Polling stopped for checkpoint';
 setTimeout(poll,200);
}poll();
</script>'''


def make_server(root, host='0.0.0.0', port=8080):
    root = Path(root)
    frames = sorted((root / 'frames').glob('*.jpg'))
    if not frames:
        raise RuntimeError('No JPEG frames in /data/video/frames; prepare video first')
    playback = Playback(len(frames))

    class Handler(BaseHTTPRequestHandler):
        protocol_version = 'HTTP/1.0'

        def send(self, status, content, content_type):
            self.send_response(status)
            self.send_header('Content-Type', content_type)
            self.send_header('Content-Length', str(len(content)))
            self.send_header('Cache-Control', 'no-store')
            self.end_headers()
            self.wfile.write(content)

        def do_GET(self):
            path = self.path.split('?', 1)[0]
            if path == '/':
                self.send(200, HTML.encode(), 'text/html; charset=utf-8')
            elif path == '/state':
                state = playback.state()
                state['marker'] = (root.parent / 'marker.txt').read_text() if (root.parent / 'marker.txt').exists() else ''
                self.send(200, json.dumps(state).encode(), 'application/json')
            elif path.startswith('/frame/'):
                try:
                    index = int(path.rsplit('/', 1)[1])
                    if not 0 <= index < len(frames):
                        raise ValueError()
                    data = frames[index].read_bytes()
                except (ValueError, FileNotFoundError):
                    self.send(404, b'Frame not found', 'text/plain')
                    return
                self.send(200, data, 'image/jpeg')
            else:
                self.send(404, b'Not found', 'text/plain')

        def do_POST(self):
            if self.path not in ('/pause', '/resume'):
                self.send(404, b'Not found', 'text/plain')
                return
            self.send(200, json.dumps(playback.pause(self.path == '/pause')).encode(), 'application/json')

        def log_message(self, *_):
            pass

    return HTTPServer((host, port), Handler)


if __name__ == '__main__':
    make_server(os.environ.get('VIDEO_ROOT', '/data/video')).serve_forever()
