# Credential Rotation

This document tracks rotation procedures for every credential in the
`pipeline-env` Secret Manager secret (project `practical-gecko-373320`).

> **Token-handling rule:** never paste a token into chat or commit one to
> disk. Use the rotation flows in [`scripts/rotate_credentials.py`](scripts/rotate_credentials.py),
> which prompt via Rich `password=True` and write directly to Secret Manager.

## Status

All 9 production pipelines are currently green. Items below marked "later"
are precautionary (potential exposure on the old Windows machine) rather
than required to restore service.

**Done**
| Credential | Last rotated | Notes |
|---|---|---|
| `META_ACCESS_TOKEN` | 2026-04-30 | System User token, no expiry |
| `SHOPIFY_ACCESS_TOKEN` | 2026-05-12 | |
| `QUICKBOOKS_REFRESH_TOKEN` | 2026-05-12 | |
| `GOOGLE_ADS_REFRESH_TOKEN` | 2026-05-12 | Dev token + OAuth client also rotated |

**Later (not urgent — green pipelines, precautionary only)**
| Credential | Effort | Notes |
|---|---|---|
| `KLAVIYO_API_KEY` | easy | Single key, paste-and-go |
| `ANTHROPIC_API_KEY` | easy | Lives in `analysis-env` (ai-marketing) |
| `SLACK_WEBHOOK_URL` | easy | Regenerate webhook in Slack app config |
| `PAYPAL_CLIENT_ID` / `_SECRET` | medium | Developer dashboard pair |
| `GOOGLE_SEARCH_CONSOLE_REFRESH_TOKEN` | medium | OAuth flow with local redirect |
| `AMAZON_REFRESH_TOKEN` | medium | LWA OAuth (Amazon Ads scope) |
| `SP_API_REFRESH_TOKEN` | hard | Seller Central self-authorize, multi-step |
| `META_APP_ID` / `META_APP_SECRET` | skip unless needed | App credential — only rotate if compromise suspected to reach app secret itself |

## Meta — `META_ACCESS_TOKEN`

The Meta token is a **System User access token** with no expiration. This
replaced the earlier long-lived user token, which expired every 60 days and
caused recurring pipeline outages.

System User: `data-pipeline` (ID `61589091960127`) on the `Pickleball Effect`
business portfolio.

Granted assets:
- Ad account `Pickleball Effect` — Full control
- App `data_ingestion` — Develop app, View insights, Test app

Token scopes: `ads_read`, `ads_management`, `business_management`.

### To rotate

If the token is ever lost or compromised:

1. Go to <https://business.facebook.com/latest/settings/system_users> for the
   Pickleball Effect business portfolio.
2. Select the `data-pipeline` system user.
3. Click **Generate token** and walk the wizard:
   - App: `data_ingestion`
   - Expiration: **Never**
   - Scopes: `ads_read`, `ads_management`, `business_management`
4. Copy the token. **Do not paste it into a chat, terminal history, or file.**
5. Run the rotation flow in your terminal:
   ```bash
   uv run scripts/rotate_credentials.py meta
   ```
   Paste the token at the hidden prompt. The script validates against the
   Meta `debug_token` endpoint, writes a new version of `pipeline-env`, and
   triggers `pipeline-meta-ads` to smoke-test.
6. Optionally revoke prior tokens via the **Revoke tokens** button on the
   same system user — this invalidates *every* outstanding token for that
   user, so do this only after the new one is verified working.

## Adding a new rotation flow

When adding a flow for another credential, add it as a subcommand in
[`scripts/rotate_credentials.py`](scripts/rotate_credentials.py) following
the existing pattern (`rotate_klaviyo` is the simplest reference):

- Use Rich `Prompt.ask(..., password=True)` to capture secret values.
- Validate against the issuing API before writing (early failure beats
  a broken pipeline at 6 AM).
- Update via `write_pipeline_env(env, reason="...")` — atomic single-version
  bump that preserves other keys.
- Offer to trigger the relevant Cloud Run job to confirm end-to-end.
