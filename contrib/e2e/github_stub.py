"""A stub of GitHub for `ants login` in the e2e fixture (docs/e2e-scenarios.md, B6 to B9): only what the signer asks
of GitHub during a registration (crates/ants/src/signing/github.rs), over plain HTTP on the hub's loopback.

Usage: python3 github_stub.py [PORT]   (default 8765)

The device flow: POST /login/device/code hands out a device code and a user code, its sign-in page
https://github.e2e.example/login/device, which `ants login --github-url https://github.e2e.example` shows and nothing
fetches. A scenario plays the person entering the user code at that page: POST /e2e/authorize with a JSON body
{"user_code": ..., "login": ..., "id": N, "emails": [...], "orgs": [...], "teams": ["org/slug", ...]}. Until then the
token poll answers authorization_pending; after it, a token standing for that user, whose /user, /user/emails and
membership reads answer for the user. Every request is logged to stdout.
"""

import json
import secrets
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs

PAGE = "https://github.e2e.example/login/device"
lock = threading.Lock()
flows = {}  # device code -> {"user_code": ..., "user": None or the user}
tokens = {}  # token -> user
users = {}  # login -> user, for the lookups with the signer's token


class Stub(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, format, *args):
        sys.stdout.write("%s %s\n" % (self.log_date_time_string(), format % args))
        sys.stdout.flush()

    def answer(self, status, body=None):
        data = b"" if body is None else json.dumps(body).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def body(self):
        return self.rfile.read(int(self.headers.get("Content-Length") or 0))

    def user(self):
        auth = self.headers.get("Authorization") or ""
        with lock:
            return tokens.get(auth.removeprefix("Bearer ").strip())

    def do_POST(self):
        body = self.body()
        if self.path == "/login/device/code":
            device, user_code = secrets.token_hex(16), secrets.token_hex(4).upper()
            with lock:
                flows[device] = {"user_code": user_code, "user": None}
            return self.answer(200, {"device_code": device, "user_code": user_code, "verification_uri": PAGE,
                                     "expires_in": 900, "interval": 1})
        if self.path == "/login/oauth/access_token":
            form = parse_qs(body.decode())
            device = (form.get("device_code") or [""])[0]
            with lock:
                flow = flows.get(device)
                if flow is None:
                    return self.answer(200, {"error": "expired_token"})
                if flow["user"] is None:
                    return self.answer(200, {"error": "authorization_pending"})
                token = "gho_" + secrets.token_hex(16)
                tokens[token] = flow["user"]
                del flows[device]
            return self.answer(200, {"access_token": token, "token_type": "bearer", "scope": "read:org,user:email"})
        if self.path == "/e2e/authorize":
            user = json.loads(body)
            with lock:
                for flow in flows.values():
                    if flow["user_code"] == user["user_code"]:
                        flow["user"] = user
                        users[user["login"].lower()] = user
                        return self.answer(200, {"authorized": user["login"]})
            return self.answer(404, {"message": "no sign-in waits for that code"})
        self.answer(404, {"message": "Not Found"})

    def do_GET(self):
        path = self.path.split("?")[0]
        parts = path.strip("/").split("/")
        user = self.user()
        account = lambda u: {"login": u["login"], "id": u["id"], "type": "User"}
        if path == "/user":
            return self.answer(200, account(user)) if user else self.answer(401, {"message": "Bad credentials"})
        if path == "/user/emails":
            if not user:
                return self.answer(401, {"message": "Bad credentials"})
            return self.answer(200, [{"email": e, "verified": True, "primary": i == 0, "visibility": None}
                                     for i, e in enumerate(user.get("emails", []))])
        if parts[:3] == ["user", "memberships", "orgs"] and len(parts) == 4:
            if user and parts[3].lower() in [o.lower() for o in user.get("orgs", [])]:
                return self.answer(200, {"state": "active", "role": "member"})
            return self.answer(404, {"message": "Not Found"})
        if parts[0] == "orgs" and len(parts) == 6 and parts[2] == "teams" and parts[4] == "memberships":
            with lock:
                member = users.get(parts[5].lower())
            if member and "%s/%s" % (parts[1], parts[3]) in member.get("teams", []):
                return self.answer(200, {"state": "active", "role": "member"})
            return self.answer(404, {"message": "Not Found"})
        if parts[0] == "orgs" and len(parts) == 4 and parts[2] == "members":
            with lock:
                member = users.get(parts[3].lower())
            ok = member and parts[1].lower() in [o.lower() for o in member.get("orgs", [])]
            return self.answer(204 if ok else 404)
        if parts[0] == "orgs" and len(parts) in (2, 4):
            return self.answer(200, {"login": parts[1]})
        if parts[0] in ("user", "users") and len(parts) == 2:
            with lock:
                found = [u for u in users.values() if str(u["id"]) == parts[1] or u["login"].lower() == parts[1].lower()]
            return self.answer(200, account(found[0])) if found else self.answer(404, {"message": "Not Found"})
        self.answer(404, {"message": "Not Found"})


if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8765
    ThreadingHTTPServer(("127.0.0.1", port), Stub).serve_forever()
