#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = [
#   "google-auth>=2.30",
#   "google-auth-oauthlib>=1.2",
#   "google-cloud-secret-manager>=2.20",
#   "requests>=2.32",
#   "rich>=13.7",
# ]
# ///
"""Guided credential rotation for Pickleball Effect data pipelines.

Rotates Google Ads OAuth (refresh token + client + dev token), Shopify Custom
App credentials, or Klaviyo API key. Atomically writes a new version of the
`pipeline-env` Secret Manager secret and re-triggers the affected Cloud Run
job to validate.

Usage:
    uv run scripts/rotate_credentials.py google-ads
    uv run scripts/rotate_credentials.py shopify
    uv run scripts/rotate_credentials.py klaviyo
    uv run scripts/rotate_credentials.py validate-all

The rotation flows that require human-in-browser steps (regenerating dev
tokens, OAuth client IDs in console, etc.) print clear instructions and
prompt for paste. The OAuth refresh-token exchange and Secret Manager
update are fully automated.
"""

from __future__ import annotations

import argparse
import http.server
import json
import os
import secrets
import subprocess
import sys
import threading
import urllib.parse
import webbrowser
from dataclasses import dataclass
from typing import Callable

import requests
from google.cloud import secretmanager
from rich.console import Console
from rich.panel import Panel
from rich.prompt import Confirm, Prompt

PROJECT_ID = "practical-gecko-373320"
SECRET_NAME = "pipeline-env"
REGION = "us-west1"

console = Console()


# ── Secret Manager I/O ────────────────────────────────────────────────────────


def _sm_client() -> secretmanager.SecretManagerServiceClient:
    return secretmanager.SecretManagerServiceClient()


def read_pipeline_env() -> dict[str, str]:
    """Pull latest pipeline-env, parse KEY=VALUE lines into a dict."""
    client = _sm_client()
    name = f"projects/{PROJECT_ID}/secrets/{SECRET_NAME}/versions/latest"
    payload = client.access_secret_version(name=name).payload.data.decode("utf-8")
    out: dict[str, str] = {}
    for line in payload.splitlines():
        line = line.rstrip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue
        k, _, v = line.partition("=")
        out[k.strip()] = v
    return out


def write_pipeline_env(env: dict[str, str], reason: str) -> str:
    """Write a new version of pipeline-env. Returns the new version name."""
    payload = "\n".join(f"{k}={v}" for k, v in env.items()) + "\n"
    client = _sm_client()
    parent = f"projects/{PROJECT_ID}/secrets/{SECRET_NAME}"
    response = client.add_secret_version(
        parent=parent, payload={"data": payload.encode("utf-8")}
    )
    console.print(f"[green]✓[/] wrote {response.name} ({reason})")
    return response.name


# ── Cloud Run job validation ──────────────────────────────────────────────────


def trigger_and_wait(job_name: str) -> bool:
    """Trigger a Cloud Run job and wait for it. Return True on success."""
    console.print(f"[cyan]▶[/] triggering [b]{job_name}[/] to validate…")
    result = subprocess.run(
        [
            "gcloud", "run", "jobs", "execute", job_name,
            f"--project={PROJECT_ID}", f"--region={REGION}", "--wait",
        ],
        capture_output=True, text=True,
    )
    sys.stderr.write(result.stderr)
    sys.stdout.write(result.stdout)
    if result.returncode == 0:
        console.print(f"[green]✓[/] {job_name} succeeded")
        return True
    console.print(f"[red]✗[/] {job_name} failed (exit {result.returncode})")
    return False


# ── OAuth callback server (Google Ads) ────────────────────────────────────────


@dataclass
class _CallbackResult:
    code: str | None = None
    state: str | None = None
    error: str | None = None


def _run_oauth_callback(expected_state: str, port: int = 8765) -> _CallbackResult:
    """Spin up a one-shot HTTP server to capture OAuth redirect."""
    result = _CallbackResult()
    done = threading.Event()

    class Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            qs = urllib.parse.urlparse(self.path).query
            params = urllib.parse.parse_qs(qs)
            result.code = (params.get("code") or [None])[0]
            result.state = (params.get("state") or [None])[0]
            result.error = (params.get("error") or [None])[0]
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.end_headers()
            body = (
                b"<html><body style='font-family:system-ui;padding:40px'>"
                b"<h2>You can close this tab.</h2>"
                b"<p>Auth code captured. Return to the terminal.</p>"
                b"</body></html>"
            )
            self.wfile.write(body)
            done.set()

        def log_message(self, *_):  # silence access logs
            pass

    server = http.server.HTTPServer(("127.0.0.1", port), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    done.wait(timeout=300)
    server.shutdown()

    if result.error:
        raise RuntimeError(f"OAuth error: {result.error}")
    if not result.code:
        raise RuntimeError("OAuth callback timed out (5 min)")
    if result.state != expected_state:
        raise RuntimeError("OAuth state mismatch — possible CSRF, abort")
    return result


# ── Google Ads rotation ───────────────────────────────────────────────────────

GADS_SCOPE = "https://www.googleapis.com/auth/adwords"
GADS_AUTH_URL = "https://accounts.google.com/o/oauth2/v2/auth"
GADS_TOKEN_URL = "https://oauth2.googleapis.com/token"
GADS_REDIRECT = "http://127.0.0.1:8765/oauth/callback"


def _step(n: int, total: int, title: str) -> None:
    console.print(Panel(f"[b]Step {n}/{total}:[/] {title}", style="cyan"))


def rotate_google_ads() -> None:
    env = read_pipeline_env()
    console.print(f"[dim]current Google Ads keys present: "
                  f"{[k for k in env if k.startswith('GOOGLE_ADS_')]}[/]")

    total = 6
    _step(1, total, "Regenerate Developer Token")
    console.print(
        "Open [u]https://ads.google.com[/] → Tools & Settings → API Center.\n"
        "Click [b]Regenerate developer token[/]. Copy the new value."
    )
    if Confirm.ask("Open Ads UI in browser now?", default=True):
        webbrowser.open("https://ads.google.com/aw/apicenter")
    new_dev_token = Prompt.ask("Paste new GOOGLE_ADS_DEVELOPER_TOKEN",
                               password=True).strip()
    if not new_dev_token or len(new_dev_token) < 20:
        console.print("[red]✗ that doesn't look right; aborting[/]")
        return

    _step(2, total, "Regenerate OAuth Client ID + Secret")
    console.print(
        "Open [u]https://console.cloud.google.com/apis/credentials?project="
        f"{PROJECT_ID}[/].\n"
        "Find the existing OAuth client used for Google Ads pipelines.\n"
        "Either [b]Reset Secret[/] (keeps client_id, new secret) or create a\n"
        "new OAuth 2.0 Client ID (Desktop / Web — Web here, since we use\n"
        "127.0.0.1 redirect)."
    )
    if Confirm.ask("Open GCP credentials page now?", default=True):
        webbrowser.open(
            f"https://console.cloud.google.com/apis/credentials?project={PROJECT_ID}"
        )
    new_client_id = Prompt.ask("Paste new GOOGLE_ADS_CLIENT_ID").strip()
    new_client_secret = Prompt.ask("Paste new GOOGLE_ADS_CLIENT_SECRET",
                                   password=True).strip()
    console.print(
        f"\n[yellow]Important:[/] In the OAuth client config, ensure\n"
        f"[b]{GADS_REDIRECT}[/] is listed under Authorized redirect URIs."
    )
    Confirm.ask("Confirmed redirect URI is set?", default=True)

    _step(3, total, "Run OAuth consent → capture refresh token")
    state = secrets.token_urlsafe(24)
    auth_qs = urllib.parse.urlencode({
        "client_id": new_client_id,
        "redirect_uri": GADS_REDIRECT,
        "response_type": "code",
        "scope": GADS_SCOPE,
        "access_type": "offline",
        "prompt": "consent",  # force a refresh_token even on re-auth
        "state": state,
    })
    auth_url = f"{GADS_AUTH_URL}?{auth_qs}"
    console.print(f"Opening consent URL…\n[dim]{auth_url}[/]")
    webbrowser.open(auth_url)
    console.print("Waiting on local callback (port 8765)… up to 5 min.")
    cb = _run_oauth_callback(expected_state=state)

    token_resp = requests.post(GADS_TOKEN_URL, data={
        "code": cb.code,
        "client_id": new_client_id,
        "client_secret": new_client_secret,
        "redirect_uri": GADS_REDIRECT,
        "grant_type": "authorization_code",
    }, timeout=30)
    token_resp.raise_for_status()
    tokens = token_resp.json()
    new_refresh = tokens.get("refresh_token")
    if not new_refresh:
        console.print(
            "[red]✗ no refresh_token returned. Common cause: this Google\n"
            "account already has a non-expired token. Revoke at\n"
            "https://myaccount.google.com/permissions and retry.[/]"
        )
        return
    new_access = tokens["access_token"]
    console.print("[green]✓[/] got refresh_token + access_token")

    _step(4, total, "Validate token against Google Ads API")
    login_cust = env.get("GOOGLE_ADS_LOGIN_CUSTOMER_ID", "").replace("-", "")
    headers = {
        "Authorization": f"Bearer {new_access}",
        "developer-token": new_dev_token,
    }
    if login_cust:
        headers["login-customer-id"] = login_cust
    resp = requests.get(
        "https://googleads.googleapis.com/v18/customers:listAccessibleCustomers",
        headers=headers, timeout=30,
    )
    if resp.status_code != 200:
        console.print(f"[red]✗ validation failed ({resp.status_code}):[/]\n{resp.text}")
        if not Confirm.ask("Write to Secret Manager anyway?", default=False):
            return
    else:
        accessible = resp.json().get("resourceNames", [])
        console.print(f"[green]✓[/] token works — {len(accessible)} accessible customer(s)")

    _step(5, total, "Write new pipeline-env version")
    env["GOOGLE_ADS_DEVELOPER_TOKEN"] = new_dev_token
    env["GOOGLE_ADS_CLIENT_ID"] = new_client_id
    env["GOOGLE_ADS_CLIENT_SECRET"] = new_client_secret
    env["GOOGLE_ADS_REFRESH_TOKEN"] = new_refresh
    write_pipeline_env(env, reason="rotate google-ads")

    _step(6, total, "Trigger pipeline-google-ads to confirm end-to-end")
    if Confirm.ask("Trigger pipeline-google-ads job now?", default=True):
        trigger_and_wait("pipeline-google-ads")


# ── Shopify rotation ──────────────────────────────────────────────────────────


def rotate_shopify() -> None:
    env = read_pipeline_env()
    shop_domain = env.get("SHOPIFY_SHOP_DOMAIN", "your-shop.myshopify.com")
    total = 4

    _step(1, total, "Regenerate Shopify Custom App credentials")
    console.print(
        f"Open [u]https://{shop_domain}/admin/settings/apps/development[/].\n"
        "Find the Custom App used by the data pipeline.\n"
        "Click [b]API credentials[/] → [b]Rotate API credentials[/].\n"
        "Copy the new Client ID and Client Secret (Client Credentials Grant —\n"
        "tokens auto-refresh, so no separate refresh-token step needed)."
    )
    if Confirm.ask("Open Shopify admin now?", default=True):
        webbrowser.open(f"https://{shop_domain}/admin/settings/apps/development")
    new_id = Prompt.ask("Paste new SHOPIFY_CLIENT_ID").strip()
    new_secret = Prompt.ask("Paste new SHOPIFY_CLIENT_SECRET", password=True).strip()

    _step(2, total, "Validate by minting an access token")
    api_version = env.get("SHOPIFY_API_VERSION", "2024-10")
    resp = requests.post(
        f"https://{shop_domain}/admin/oauth/access_token",
        json={
            "client_id": new_id,
            "client_secret": new_secret,
            "grant_type": "client_credentials",
        },
        timeout=30,
    )
    if resp.status_code == 200:
        console.print("[green]✓[/] Shopify creds valid")
        token = resp.json().get("access_token")
        if token:
            shop_resp = requests.get(
                f"https://{shop_domain}/admin/api/{api_version}/shop.json",
                headers={"X-Shopify-Access-Token": token}, timeout=30,
            )
            if shop_resp.status_code == 200:
                shop_name = shop_resp.json().get("shop", {}).get("name", "?")
                console.print(f"  → connected to shop: [b]{shop_name}[/]")
    else:
        console.print(f"[red]✗ validation failed ({resp.status_code})[/]: {resp.text}")
        if not Confirm.ask("Write anyway?", default=False):
            return

    _step(3, total, "Write new pipeline-env version")
    env["SHOPIFY_CLIENT_ID"] = new_id
    env["SHOPIFY_CLIENT_SECRET"] = new_secret
    write_pipeline_env(env, reason="rotate shopify")

    _step(4, total, "Trigger pipeline-shopify")
    if Confirm.ask("Trigger pipeline-shopify job now?", default=True):
        trigger_and_wait("pipeline-shopify")


# ── Klaviyo rotation ──────────────────────────────────────────────────────────


def rotate_klaviyo() -> None:
    env = read_pipeline_env()
    total = 3

    _step(1, total, "Regenerate Klaviyo Private API Key")
    console.print(
        "Open [u]https://www.klaviyo.com/account#api-keys-tab[/].\n"
        "Create a new Private API Key. Required scopes: read access to\n"
        "Profiles, Events, Campaigns, Flows, Lists, Segments, Metrics.\n"
        "Revoke the old key AFTER this rotation completes successfully."
    )
    if Confirm.ask("Open Klaviyo API keys page now?", default=True):
        webbrowser.open("https://www.klaviyo.com/account#api-keys-tab")
    new_key = Prompt.ask("Paste new KLAVIYO_API_KEY", password=True).strip()
    if not new_key.startswith("pk_"):
        console.print("[yellow]⚠ Klaviyo private keys typically start with 'pk_'[/]")

    _step(2, total, "Validate against Klaviyo API")
    resp = requests.get(
        "https://a.klaviyo.com/api/accounts/",
        headers={
            "Authorization": f"Klaviyo-API-Key {new_key}",
            "revision": "2024-10-15",
            "accept": "application/vnd.api+json",
        },
        timeout=30,
    )
    if resp.status_code == 200:
        accounts = resp.json().get("data", [])
        if accounts:
            name = accounts[0].get("attributes", {}).get("contact_information", {}).get("organization_name", "?")
            console.print(f"[green]✓[/] Klaviyo creds valid — account: [b]{name}[/]")
    else:
        console.print(f"[red]✗ validation failed ({resp.status_code})[/]: {resp.text}")
        if not Confirm.ask("Write anyway?", default=False):
            return

    _step(3, total, "Write new pipeline-env + trigger")
    env["KLAVIYO_API_KEY"] = new_key
    write_pipeline_env(env, reason="rotate klaviyo")
    if Confirm.ask("Trigger pipeline-klaviyo job now?", default=True):
        trigger_and_wait("pipeline-klaviyo")


# ── Validate all (read-only health check) ─────────────────────────────────────


def validate_all() -> None:
    """Read-only check: do current credentials actually work?"""
    env = read_pipeline_env()
    results: list[tuple[str, bool, str]] = []

    # Google Ads
    try:
        token = requests.post(GADS_TOKEN_URL, data={
            "client_id": env["GOOGLE_ADS_CLIENT_ID"],
            "client_secret": env["GOOGLE_ADS_CLIENT_SECRET"],
            "refresh_token": env["GOOGLE_ADS_REFRESH_TOKEN"],
            "grant_type": "refresh_token",
        }, timeout=30).json()
        if "access_token" in token:
            login = env.get("GOOGLE_ADS_LOGIN_CUSTOMER_ID", "").replace("-", "")
            r = requests.get(
                "https://googleads.googleapis.com/v18/customers:listAccessibleCustomers",
                headers={
                    "Authorization": f"Bearer {token['access_token']}",
                    "developer-token": env["GOOGLE_ADS_DEVELOPER_TOKEN"],
                    **({"login-customer-id": login} if login else {}),
                }, timeout=30,
            )
            ok = r.status_code == 200
            msg = f"{len(r.json().get('resourceNames', []))} accessible customers" if ok else r.text[:120]
            results.append(("google-ads", ok, msg))
        else:
            results.append(("google-ads", False, str(token)[:200]))
    except Exception as e:
        results.append(("google-ads", False, str(e)[:200]))

    # Shopify
    try:
        domain = env["SHOPIFY_SHOP_DOMAIN"]
        r = requests.post(
            f"https://{domain}/admin/oauth/access_token",
            json={
                "client_id": env["SHOPIFY_CLIENT_ID"],
                "client_secret": env["SHOPIFY_CLIENT_SECRET"],
                "grant_type": "client_credentials",
            }, timeout=30,
        )
        ok = r.status_code == 200
        results.append(("shopify", ok, "minted access token" if ok else r.text[:120]))
    except Exception as e:
        results.append(("shopify", False, str(e)[:200]))

    # Klaviyo
    try:
        r = requests.get(
            "https://a.klaviyo.com/api/accounts/",
            headers={
                "Authorization": f"Klaviyo-API-Key {env['KLAVIYO_API_KEY']}",
                "revision": "2024-10-15",
                "accept": "application/vnd.api+json",
            }, timeout=30,
        )
        ok = r.status_code == 200
        results.append(("klaviyo", ok, "account fetched" if ok else r.text[:120]))
    except Exception as e:
        results.append(("klaviyo", False, str(e)[:200]))

    # Meta — debug_token (sanity check we don't expire silently again)
    try:
        token = env["META_ACCESS_TOKEN"]
        app_token = f"{env['META_APP_ID']}|{env['META_APP_SECRET']}"
        r = requests.get(
            "https://graph.facebook.com/v21.0/debug_token",
            params={"input_token": token, "access_token": app_token},
            timeout=30,
        )
        d = r.json().get("data", {})
        ok = bool(d.get("is_valid"))
        exp = d.get("expires_at")
        msg = "never expires" if not exp else f"valid, expires_at={exp}"
        if not ok:
            msg = d.get("error", {}).get("message", "invalid")
        results.append(("meta", ok, msg))
    except Exception as e:
        results.append(("meta", False, str(e)[:200]))

    console.rule("[b]validation results[/]")
    for name, ok, msg in results:
        icon = "[green]✓[/]" if ok else "[red]✗[/]"
        console.print(f"{icon} [b]{name:12}[/] {msg}")


# ── CLI ───────────────────────────────────────────────────────────────────────


COMMANDS: dict[str, Callable[[], None]] = {
    "google-ads": rotate_google_ads,
    "shopify": rotate_shopify,
    "klaviyo": rotate_klaviyo,
    "validate-all": validate_all,
}


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("command", choices=sorted(COMMANDS.keys()))
    args = p.parse_args()

    # Prereq: gcloud auth + project
    who = subprocess.run(
        ["gcloud", "config", "get-value", "project"],
        capture_output=True, text=True,
    ).stdout.strip()
    if who != PROJECT_ID:
        console.print(f"[red]gcloud project is '{who}', expected {PROJECT_ID}[/]")
        if not Confirm.ask(f"Switch project to {PROJECT_ID}?", default=True):
            sys.exit(1)
        subprocess.run(["gcloud", "config", "set", "project", PROJECT_ID], check=True)

    COMMANDS[args.command]()


if __name__ == "__main__":
    main()
