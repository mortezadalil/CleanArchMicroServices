"""
Minimal local runner for infrastructure faults (Development only).

POST /stop/<name> | /start/<name> | /restart/<name>   -> runs `docker <action> <container>`
GET  /ping                                            -> 200 "ok"

Only container names listed in ALLOWED_CONTAINERS are accepted, only those three actions exist, and
the server listens inside the compose network only.  The browser never reaches it: the Web calls it
from the server side during scenario S04 / S09.

Where it sits in the architecture:
    Scenario Lab (in the Web container) --HTTP--> runner:9000 --docker CLI--> /var/run/docker.sock
    --> host Docker daemon --> e.g. `docker stop minishop-rabbitmq` (S04: the broker goes away while
    orders keep being accepted; their messages wait in the outbox and are delivered once it is back).

Why a separate process? Access to the Docker socket is as powerful as root on the host. Isolating it in
this ~50-line, allow-listed, optional container (compose profile "faults") keeps that power away from
the Web and from the browser.

Read next: infra/compose.yaml (the "runner" service), src/MiniShop.Web/Services/ServiceClients.cs.
"""
# os: read the ALLOWED_CONTAINERS environment variable.
import os
# subprocess: run the docker CLI.
import subprocess
# The standard library's tiny HTTP server: no third-party dependencies to install or audit.
from http.server import BaseHTTPRequestHandler, HTTPServer

# Allowlist of container names, from a comma-separated environment variable set in compose.yaml.
# A set comprehension: split on ",", trim spaces, drop empty entries. Empty variable -> empty set ->
# every request is refused (safe default).
ALLOWED = {c.strip() for c in os.environ.get("ALLOWED_CONTAINERS", "").split(",") if c.strip()}
# The only docker sub-commands we will ever run. Anything else (rm, exec, run...) is impossible.
ACTIONS = {"stop", "start", "restart"}
# Callers may say "rabbitmq" instead of "minishop-rabbitmq"; the prefix is added automatically.
PREFIX = "minishop-"


class Handler(BaseHTTPRequestHandler):
    """Handles one HTTP request. HTTPServer creates a new instance for every request."""

    def _reply(self, code, body):
        """Send a plain-text response with the given status code and body."""
        # Status line, one header, end of headers, then the body as UTF-8 bytes.
        self.send_response(code)
        self.send_header("Content-Type", "text/plain")
        self.end_headers()
        self.wfile.write(body.encode())

    def do_GET(self):
        """GET /ping is a simple liveness check (e.g. `wget -qO- http://runner:9000/ping` from another container)."""
        if self.path == "/ping":
            return self._reply(200, "ok")
        # Every other GET path is unknown.
        return self._reply(404, "not found")

    def do_POST(self):
        """POST /<action>/<container>: validate strictly, then run `docker <action> <container>`."""
        # "/stop/rabbitmq" -> ["stop", "rabbitmq"]; empty segments (leading "/") are dropped.
        parts = [p for p in self.path.split("/") if p]
        # Exactly two segments, and the first must be an allowed action; otherwise 400 Bad Request.
        if len(parts) != 2 or parts[0] not in ACTIONS:
            return self._reply(400, "usage: POST /stop|start|restart/<container>")
        action, name = parts
        # Normalise "rabbitmq" -> "minishop-rabbitmq".
        container = name if name.startswith(PREFIX) else PREFIX + name
        # The allowlist check: 403 Forbidden for anything not explicitly permitted.
        if container not in ALLOWED:
            return self._reply(403, f"{container} is not in the allowlist")
        # Run the docker CLI with an argument LIST (no shell), so a crafted name cannot inject commands.
        # capture_output + text: collect stdout/stderr as strings for the response.
        result = subprocess.run(["docker", action, container], capture_output=True, text=True)
        # Non-zero exit code: report docker's error message as 500 Internal Server Error.
        if result.returncode != 0:
            return self._reply(500, result.stderr)
        return self._reply(200, f"{action} {container}: ok")

    def log_message(self, fmt, *args):
        """Override the default access log (stderr) so lines go to stdout, prefixed and flushed at once for `docker logs`."""
        print("runner:", fmt % args, flush=True)


# Only start the server when the file is executed directly (not when imported, e.g. by a test).
if __name__ == "__main__":
    # Print the effective allowlist at startup: easy to verify in `docker compose logs runner`.
    print(f"runner listening on :9000, allowlist={sorted(ALLOWED)}", flush=True)
    # 0.0.0.0 = all interfaces of the container. Safe because compose does not publish port 9000 to the host.
    # serve_forever handles one request at a time, which is plenty for a demo.
    HTTPServer(("0.0.0.0", 9000), Handler).serve_forever()
