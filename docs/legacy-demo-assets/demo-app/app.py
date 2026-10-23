"""Memory identity is deliberately never saved to the PVC."""
import json
import uuid
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
boot_id = str(uuid.uuid4())
counter = 0
class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        global counter
        counter += 1
        marker = Path("/data/marker.txt")
        payload = json.dumps({"boot_id": boot_id, "counter": counter,
            "pv_marker": marker.read_text() if marker.exists() else "missing"}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)
HTTPServer(("0.0.0.0", 8080), Handler).serve_forever()
