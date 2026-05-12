# Shopify SEO Scripts

Two automation scripts that pair with the SEO audit and rewrites at
`pe_strategy/brand/shopify-seo-2026-05/`.

## Scripts

| Script | Purpose | Scope required |
|---|---|---|
| `update_alt_text.py` | Bulk-update product image alt text across the catalog | `read_products`, `write_products` |
| `create_comparison_page.py` | Create the Tungsten vs Lead Tape page as a draft in Shopify | `read_content`, `write_content` |
| `render_comparison_page.py` | Render the same page to paste-ready HTML files (no API write) | none |
| `comparison_page_content.py` | The page body content (single source of truth) | n/a |

## Two Shopify apps — read vs write

There are two custom apps for the same store, with different scopes:

| App | Scopes | Where credentials live | Used by |
|---|---|---|---|
| **Read-only** (data platform) | `read_products`, `read_orders`, etc. | `pipeline-env` Secret Manager + local `.env` (`SHOPIFY_ACCESS_TOKEN`) | dlt ingestion pipelines |
| **Write-scoped** (writer) | `read_products`, `write_products`, `read_content`, `write_content` | `writer-env` Secret Manager (`SHOPIFY_WRITER_CLIENT_ID`, `SHOPIFY_WRITER_CLIENT_SECRET`) | These SEO scripts |

The SEO scripts pull writer credentials from `writer-env` at runtime via `gcloud secrets versions access` — no local copy required. See `_writer_client.py`.

**Risk note:** `write_products` and `write_content` grant broad mutation access. The scripts only touch image alts and pages, but the scope itself is broader. The writer app is dedicated to authorized content automation, separate from ingestion.

## Usage — comparison page (paste-ready, no scope needed)

```bash
uv run python -m scripts.seo.render_comparison_page
open out/seo/tungsten-tape-vs-lead-tape-preview.html
```

Then in Shopify admin → Online Store → Pages → Add page:
- Title: `Tungsten Tape vs Lead Tape for Pickleball Paddles`
- Content: paste the contents of `out/seo/tungsten-tape-vs-lead-tape-body.html`
- Visibility: leave **Hidden** (this is a draft)
- Search engine listing → Edit → use the values in `out/seo/tungsten-tape-vs-lead-tape-meta.txt`
- Save

## Usage — comparison page (after scope is added)

```bash
uv run python -m scripts.seo.create_comparison_page --check    # see if exists
uv run python -m scripts.seo.create_comparison_page            # create draft
uv run python -m scripts.seo.create_comparison_page --update   # refresh content
```

Page is created with `published: false` — appears in Shopify admin but is not publicly visible.

## Usage — alt text (dry-run always works)

```bash
# Generate change set (no API writes)
uv run python -m scripts.seo.update_alt_text

# Filter to a single product
uv run python -m scripts.seo.update_alt_text --product "Tungsten Weighted Tape"

# Apply (uses writer-env Secret Manager creds)
uv run python -m scripts.seo.update_alt_text --apply

# Apply to one product only — recommended first run
uv run python -m scripts.seo.update_alt_text --apply --product "Tungsten Weighted Tape"

# Re-flow alts even on images that already have text (use to correct bad past runs)
uv run python -m scripts.seo.update_alt_text --apply --overwrite --product "Tungsten Weighted Tape"
```

Dry-run output:
- `out/seo/alt-text-changes.csv` — full change set with current_alt and proposed_alt for each image

The script is idempotent — running it again only proposes changes for images that *still* have empty alt text. Already-updated alts are left alone.

## How alt text is generated

For top-revenue products (`tungsten-weighted-tape`, `tuning-clamps`, `soft-ultra-tac-overgrip-4-pack`, etc.) there are hand-curated patterns in `update_alt_text.py::ALT_PATTERNS` — one per image position. These are the alts that matter most.

For everything else, a generic but descriptive fallback: `Pickleball Effect <Product Title> — image N`.

If you want to improve alts for a specific product, edit `ALT_PATTERNS` and re-run with `--apply --product "Product Title"`. Already-updated alts won't be touched (the script only writes to empty-alt images), so you'll need to first clear the alt manually if you want to overwrite curated text.

## Idempotency and safety

- **Reads are always safe** — script only reads products via the API.
- **Dry-runs are always safe** — write a CSV, no API writes.
- **`--apply` only updates images with empty alts** — won't overwrite anything you've manually set.
- **Rate-limited** — 0.6s pacing between calls + 429 retry handling.
- **Per-image granular** — failures on one image don't block subsequent ones (except 403, which exits).

## Output directory

All generated files land in `out/seo/` (gitignored — these are artifacts, not source). The directory is created automatically.
