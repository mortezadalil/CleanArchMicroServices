"""
Minimal local runner for infrastructure faults (Development only).

POST /stop/<name> | /start/<name> | /restart/<name>   -> runs `docker <action> <container>`
GET  /ping                                            -> 200 "ok"

Only container names listed in ALLOWED_CONTAINERS are accepted, only those three actions exist, and
the server listens inside the compose network only.  The browser never reaches it: the Web calls it
from the server side during scenario S04 / S09.
"""
import os
import subprocess
from http.server import BaseHTTPRequestHandler, HTTPServer

ALLOWED = {c.strip() for c in os.environ.get("ALLOWED_CONTAINERS", "").split(",") if c.strip()}
ACTIONS = {"stop", "start", "restart"}
PREFIX = "minishop-"


class Handler(BaseHTTPRequestHandler):
    def _reply(self, code, body):
        self.send_response(code)
        self.send_header("Content-Type", "text/plain")
        self.end_headers()
        self.wfile.write(body.encode())

    def do_GET(self):
        if self.path == "/ping":
            return self._reply(200, "ok")
        return self._reply(404, "not found")

    def do_POST(self):
        parts = [p for p in self.path.split("/") if p]
        if len(parts) != 2 or parts[0] not in ACTIONS:
            return self._reply(400, "usage: POST /stop|start|restart/<container>")
        action, name = parts
        container = name if name.startswith(PREFIX) else PREFIX + name
        if container not in ALLOWED:
            return self._reply(403, f"{container} is not in the allowlist")
        result = subprocess.run(["docker", action, container], capture_output=True, text=True)
        if result.returncode != 0:
            return self._reply(500, result.stderr)
        return self._reply(200, f"{action} {container}: ok")

    def log_message(self, fmt, *args):
        print("runner:", fmt % args, flush=True)


if __name__ == "__main__":
    print(f"runner listening on :9000, allowlist={sorted(ALLOWED)}", flush=True)
    HTTPServer(("0.0.0.0", 9000), Handler).serve_forever()
