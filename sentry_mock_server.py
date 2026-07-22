import json
import sys
import uuid
from http.server import BaseHTTPRequestHandler, HTTPServer


class SentryMockHandler(BaseHTTPRequestHandler):
    def do_POST(self):
        n = int(self.headers.get("Content-Length", 0))
        self.rfile.read(n)
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps({"id": str(uuid.uuid4())}).encode())
        sys.stderr.write("envelope received\n")
        sys.stderr.flush()

    def log_message(self, *args):
        pass


if __name__ == "__main__":
    server = HTTPServer(("0.0.0.0", 9000), SentryMockHandler)
    sys.stderr.write("Sentry mock listening on :9000\n")
    sys.stderr.flush()
    server.serve_forever()
