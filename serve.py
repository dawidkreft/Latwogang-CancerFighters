"""
serve.py — uruchamia lokalny serwer HTTP i otwiera dashboard w przeglądarce.

Użycie:
    python3 serve.py
    python3 serve.py 8080      # inny port
"""

import http.server
import os
import sys
import webbrowser
import threading
import socketserver

PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 8080
DIR  = os.path.dirname(os.path.abspath(__file__))

class Handler(http.server.SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=DIR, **kwargs)
    def log_message(self, fmt, *args):
        # Pokaż tylko błędy, nie każdy request
        if args[1] not in ('200', '304'):
            super().log_message(fmt, *args)

def open_browser():
    import time; time.sleep(0.5)
    webbrowser.open(f"http://localhost:{PORT}")

print(f"🎮 Łatwogang Dashboard")
print(f"   Serwer: http://localhost:{PORT}")
print(f"   Katalog: {DIR}")
print(f"   Zatrzymaj: Ctrl+C\n")

threading.Thread(target=open_browser, daemon=True).start()

with socketserver.TCPServer(("", PORT), Handler) as httpd:
    httpd.allow_reuse_address = True
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\nSerwer zatrzymany.")
