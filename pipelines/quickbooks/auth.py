"""One-time OAuth2 authorization flow for QuickBooks Online.

Usage:
    python -m pipelines.quickbooks.auth

Uses the Intuit OAuth2 Playground redirect URI. After authorizing in the
browser, paste the full redirect URL from your address bar into the terminal.
"""

import base64
import urllib.parse
import webbrowser

import requests

from ..config import QUICKBOOKS_CLIENT_ID, QUICKBOOKS_CLIENT_SECRET

AUTHORIZATION_URL = "https://appcenter.intuit.com/connect/oauth2"
TOKEN_URL = "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer"
REDIRECT_URI = "https://developer.intuit.com/v2/OAuth2Playground/RedirectUrl"
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

    print("After authorizing, you'll land on the Intuit OAuth Playground page.")
    print("Copy the FULL URL from your browser's address bar and paste it here.\n")
    redirect_url = input("Paste the full redirect URL: ").strip()

    parsed = urllib.parse.urlparse(redirect_url)
    params_out = urllib.parse.parse_qs(parsed.query)

    if "error" in params_out:
        print(f"\nAuthorization failed: {params_out['error'][0]}")
        return

    code = params_out.get("code", [None])[0]
    realm_id = params_out.get("realmId", [None])[0]

    if not code:
        print("\nNo authorization code found in the URL. Make sure you copied the full URL.")
        return

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
