import time
import os
from http.server import HTTPServer, BaseHTTPRequestHandler

class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b'OK')

port = int(os.environ.get('PORT', 8080))
server = HTTPServer(('0.0.0.0', port), Handler)
print(f"Server running on port {port}")

while True:
    server.handle_request()
