import importlib.util
import unittest
import tempfile
import threading
import urllib.request
import json
from pathlib import Path
spec = importlib.util.spec_from_file_location('video_app', Path(__file__).with_name('app.py'))
app = importlib.util.module_from_spec(spec)
spec.loader.exec_module(app)

class PlaybackTests(unittest.TestCase):
    def test_pause_restore_gap_and_resume(self):
        now = [0.0]
        p = app.Playback(100, clock=lambda: now[0])
        now[0] = .4
        self.assertEqual(p.state()['frame'], 2)
        before = p.pause(True)
        now[0] = 900
        self.assertEqual(p.state()['frame'], before['frame'])
        p.pause(False)
        now[0] += .4
        self.assertEqual(p.state()['frame'], 4)

    def test_fresh_process_resets_identity_and_frame(self):
        a, b = app.Playback(100), app.Playback(100)
        a.position = 50
        self.assertEqual(b.state()['frame'], 0)
        self.assertNotEqual(a.boot_id, b.boot_id)

    def test_clock_jump_does_not_skip_and_end_is_bounded(self):
        now = [0.0]
        p = app.Playback(3, clock=lambda: now[0])
        now[0] = 1000
        self.assertEqual(p.state()['frame'], 0)
        now[0] += .8
        self.assertEqual(p.state()['frame'], 2)

class HttpTests(unittest.TestCase):
    def test_http_state_pause_and_fresh_server(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / 'video'
            (root / 'frames').mkdir(parents=True)
            for i in range(3):
                (root / 'frames' / f'{i:06d}.jpg').write_bytes(b'fake-jpeg')
            (Path(tmp) / 'marker.txt').write_text('pv-marker')
            before = {str(p.relative_to(tmp)): p.read_bytes() for p in Path(tmp).rglob('*') if p.is_file()}
            identities = []
            for _ in range(2):
                server = app.make_server(root, '127.0.0.1', 0)
                thread = threading.Thread(target=server.serve_forever, daemon=True)
                thread.start()
                url = f'http://127.0.0.1:{server.server_port}'
                try:
                    with urllib.request.urlopen(urllib.request.Request(url + '/pause', method='POST')) as r:
                        self.assertTrue(json.load(r)['paused'])
                    with urllib.request.urlopen(url + '/state') as r:
                        state = json.load(r)
                    identities.append(state['boot_id'])
                    self.assertEqual(state['frame'], 0)
                    self.assertEqual(state['marker'], 'pv-marker')
                    with urllib.request.urlopen(url + '/frame/0') as r:
                        self.assertEqual(r.read(), b'fake-jpeg')
                    with urllib.request.urlopen(url) as r:
                        self.assertIn(b'Disconnect for checkpoint', r.read())
                finally:
                    server.shutdown()
                    server.server_close()
                    thread.join()
            self.assertNotEqual(*identities)
            after = {str(p.relative_to(tmp)): p.read_bytes() for p in Path(tmp).rglob('*') if p.is_file()}
            self.assertEqual(before, after, 'Playback must never be saved on the PVC')

if __name__ == '__main__':
    unittest.main()
