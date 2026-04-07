"""One-time OAuth2 authorization flow for QuickBooks Online.

Usage:
    python -m pipelines.quickbooks.auth

Spins up a local server on port 8080 to catch the OAuth callback automatically.
Requires http://localhost:8080 to be listed as a Redirect URI in your Intuit app.
"""

import base64
import threading
import urllib.parse
import webbrowser
from http.server import BaseHTTPRequestHandler, HTTPServer

import requests

from ..config import QUICKBOOKS_CLIENT_ID, QUICKBOOKS_CLIENT_SECRET

_auth_result: dict = {}


class _CallbackHandler(BaseHTTPRequestHandler):
    def do_GET(self):  # noqa: N802
        parsed = urllib.parse.urlparse(self.path)
        params = urllib.parse.parse_qs(parsed.query)
        if "code" in params:
            _auth_result["code"] = params["code"][0]
            _auth_result["realm_id"] = params.get("realmId", [None])[0]
            self.send_response(200)
            self.send_header("Content-Type", "text/html")
            self.end_headers()
            self.wfile.write(b"<h2>Authorized! You can close this tab.</h2>")
        else:
            _auth_result["error"] = params.get("error", ["unknown"])[0]
            self.send_response(400)
            self.send_header("Content-Type", "text/html")
            self.end_headers()
            self.wfile.write(b"<h2>Authorization failed. Check terminal.</h2>")
        threading.Thread(target=self.server.shutdown, daemon=True).start()

    def log_message(self, format, *args):
        pass

AUTHORIZATION_URL = "https://appcenter.intuit.com/connect/oauth2"
TOKEN_URL = "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer"
REDIRECT_URI = "http://localhost:8080"
SCOPES = "com.intuit.quickbooks.accounting"


def _exchange_code(code: str) -> dict:
    """Exchange authorization code for access + refresh tokens."""
    credentials = base64.b64encode(
        f"{QUICKBOOKS_CLIENT_ID}:{QUICKBOOKS_CLIENT_SECRET}".encode()
    ).decode()

    resp = requests.post(
        TOKEN_URL,
        headers={
            "Authorization": f"Basic {credentials}",
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "application/json",
        },
        data={
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": REDIRECT_URI,
        },
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()


def authorize():
    """Run the full OAuth2 authorization flow."""
    if not QUICKBOOKS_CLIENT_ID or not QUICKBOOKS_CLIENT_SECRET:
        print("ERROR: Set QUICKBOOKS_CLIENT_ID and QUICKBOOKS_CLIENT_SECRET in .env first.")
        return

    params = urllib.parse.urlencode({
        "client_id": QUICKBOOKS_CLIENT_ID,
        "scope": SCOPES,
        "redirect_uri": REDIRECT_URI,
        "response_type": "code",
        "state": "pickleball",
    })
    auth_url = f"{AUTHORIZATION_URL}?{params}"

    print("Opening browser for QuickBooks authorization...")
    print(f"\nIf the browser doesn't open, visit:\n{auth_url}\n")
    webbrowser.open(auth_url)

    print("Waiting for authorization callback on http://localhost:8080 ...")
    server = HTTPServer(("localhost", 8080), _CallbackHandler)
    server.serve_forever()

    if "error" in _auth_result:
        print(f"\nAuthorization failed: {_auth_result['error']}")
        return
    if "code" not in _auth_result:
        print("\nNo authorization code received.")
        return

    code = _auth_result["code"]
    realm_id = _auth_result.get("realm_id")
    print("\nExchanging authorization code for tokens...")
    tokens = _exchange_code(code)

    print("\n" + "=" * 60)
    print("SUCCESS! Add these to your .env file:")
    print("=" * 60)
    print(f"\nQUICKBOOKS_REFRESH_TOKEN={tokens['refresh_token']}")
    if realm_id:
        print(f"QUICKBOOKS_REALM_ID={realm_id}")
    print(f"\nRefresh token valid for: {tokens.get('x_refresh_token_expires_in', '?')} seconds (~100 days)")
    print("It auto-renews on each pipeline run so it won't expire as long as the pipeline runs daily.")


if __name__ == "__main__":
    authorize()
