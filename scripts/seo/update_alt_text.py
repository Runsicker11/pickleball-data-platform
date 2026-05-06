"""Bulk-update Shopify product image alt text.

Strategy:
  - Pull all active products via the Admin REST API (read-only).
  - For each image with empty/missing alt, generate alt from product title +
    image position + variant context.
  - Dry-run by default: writes a CSV of proposed changes to out/seo/.
  - --apply commits the changes via PUT /products/{pid}/images/{iid}.json.

Required scopes:
  - read_products  (already present)
  - write_products (NOT present today — token returns 403 on PUT). Adding the
    scope requires updating the custom app config in Shopify admin.

Usage:
    uv run python -m scripts.seo.update_alt_text                 # dry-run
    uv run python -m scripts.seo.update_alt_text --product TITLE # filter
    uv run python -m scripts.seo.update_alt_text --apply         # commit
    uv run python -m scripts.seo.update_alt_text --apply --product "Tungsten Weighted Tape"
"""

import argparse
import csv
import logging
import re
import sys
import time
from pathlib import Path

from pipelines import config
from pipelines.shopify.client import ShopifyClient
from scripts.seo._writer_client import get_writer_client

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

OUT_DIR = Path(__file__).resolve().parents[2] / "out" / "seo"
OUT_DIR.mkdir(parents=True, exist_ok=True)


# Hand-curated alt patterns for top-traffic products. The default fallback is a
# generic but descriptive pattern; these override for the products that matter.
# Map current (live) handles to the curated-pattern key. The keys on the right
# are the post-migration clean handles, but products still use the old handles
# until the URL migration goes live.
HANDLE_ALIASES: dict[str, str] = {
    "pickleball-effect-weighted-tungsten-tape-1-gram-per-inch-safe-alternative-to-lead-tape": "tungsten-weighted-tape",
    "tungsten-tape-strips-ten-3-gram-strips-for-pickleball-paddle-customization-toxic-free-alternative-to-lead-tape": "tungsten-tape-strips",
    "pickleball-effect-soft-ultra-tac-overgrip-premium-comfort-extra-tacky-hot-pink-4-pack": "soft-ultra-tac-overgrip-4-pack",
    "pickleball-effect-comfort-tac-overgrip-high-performance-overgrip-black-4-pack": "comfort-tac-overgrip-4-pack",
    "pickleball-effect-edge-guard-tape-narrow-width-no-wrinkling-protects-full-edge-of-2-paddles-designed-by-pickleball-will-pink": "edge-guard-tape",
    "pickleball-effect-tuning-clamps-adjustable-clamp-on-pickleball-paddle-tuning-weights": "tuning-clamps",
}


ALT_PATTERNS: dict[str, list[str]] = {
    "tungsten-weighted-tape": [
        "Pickleball Effect tungsten weighted tape, 1 gram per inch",
        "Tungsten weighted tape applied to pickleball paddle edge",
        "Tungsten tape cut to length for paddle weighting",
        "Pickleball paddle with tungsten weighted tape at 3 and 9 oclock",
        "Tungsten weighted tape adhesive backing detail",
        "Tungsten tape on pickleball paddle ready for play",
        "Pickleball Effect tungsten weighted tape product packaging",
    ],
    "tungsten-tape-strips": [
        "Ten 3 gram tungsten tape strips for pickleball paddles",
        "Tungsten tape strip applied at 3 oclock position on pickleball paddle",
        "Pre-cut tungsten weighted strips lined up",
        "Tungsten tape strip pulled from adhesive backing",
        "Tungsten tape strips packaging Pickleball Effect",
        "Tungsten tape strips with paddle for size reference",
        "Tungsten strips on edge guard for symmetric weighting",
    ],
    "tuning-clamps": [
        "Pickleball Effect tuning clamps mounted on paddle edge guard",
        "Tuning clamp 6061 aircraft aluminum body close-up",
        "Pair of tuning clamps with hex key install tool",
        "Tuning clamps positioned at 3 and 9 oclock for forgiveness",
        "Tuning clamps slid toward paddle head for power setup",
        "Tuning clamp adjustable position detail on edge guard",
        "Pickleball Effect tuning clamps in package",
        "Tuning clamps and tools laid out for installation",
        "Tuning clamps on multiple paddles for setup comparison",
        "Pickleball paddle with tuning clamps tournament-ready",
        "Tuning clamps profile shot showing low edge protrusion",
        "Tuning clamps mid-installation on pickleball paddle",
    ],
    "soft-ultra-tac-overgrip-4-pack": [
        "Soft Ultra Tac extra-soft pickleball overgrip 4-pack",
        "Soft Ultra Tac overgrip wrapped on pickleball paddle handle",
        "Soft Ultra Tac extra-soft overgrip surface texture detail",
        "Soft Ultra Tac overgrip pack contents shown opened",
        "Soft Ultra Tac overgrip installation start at butt cap",
        "Soft Ultra Tac wrap technique on pickleball paddle handle",
        "Soft Ultra Tac extra-soft overgrip cushion thickness detail",
        "Soft Ultra Tac overgrip 4-pack packaging Pickleball Effect",
        "Pickleball paddle with Soft Ultra Tac overgrip ready for play",
        "Soft Ultra Tac extra-soft overgrip individual roll",
        "Soft Ultra Tac overgrip color options 4-pack",
        "Soft Ultra Tac extra-soft overgrip wrap tension detail",
        "Soft Ultra Tac overgrip on heavier pickleball paddle setup",
        "Soft Ultra Tac extra-soft overgrip side profile",
        "Soft Ultra Tac overgrip end-of-handle finish",
        "Soft Ultra Tac extra-soft overgrip installation complete",
        "Soft Ultra Tac overgrip backing strip removal",
        "Soft Ultra Tac overgrip pack stacked",
        "Soft Ultra Tac extra-soft overgrip lifestyle shot",
    ],
    "comfort-tac-overgrip-4-pack": [
        "Comfort Tac thin tacky pickleball overgrip 4-pack",
        "Comfort Tac overgrip wrapped on paddle handle",
        "Comfort Tac overgrip surface texture close-up showing tack",
        "Comfort Tac overgrip 4-pack contents",
        "Comfort Tac overgrip installation on pickleball paddle",
        "Comfort Tac thin tacky overgrip individual roll",
        "Comfort Tac overgrip wrap tension detail",
        "Comfort Tac overgrip color options",
        "Comfort Tac thin tacky overgrip on pickleball paddle ready for play",
        "Comfort Tac overgrip backing strip removal",
        "Comfort Tac overgrip end-of-handle finish",
        "Comfort Tac overgrip 4-pack stacked",
    ],
    "edge-guard-tape": [
        "Pickleball Effect edge guard tape three width options",
        "Edge guard tape applied to pickleball paddle without wrinkling",
        "Narrow edge guard tape on thin profile pickleball paddle",
        "Wide edge guard tape on thick edge guard paddle",
        "Edge guard tape installation on pickleball paddle rim",
        "Edge guard tape Pickleball Will pink color option",
        "Edge guard tape product packaging Pickleball Effect",
        "Edge guard tape mid-wrap technique demonstration",
        "Edge guard tape detail showing clean edge wrap",
        "Edge guard tape on pickleball paddle full coverage",
    ],
    "paddle-tuning-tape": [
        "Pickleball Effect paddle tuning tape, 60 inch lead-free roll",
        "Paddle tuning tape applied along pickleball paddle edge guard",
        "Paddle tuning tape cut to length for symmetric placement",
        "Paddle tuning tape and tungsten tape side by side",
        "Paddle tuning tape adhesive backing detail",
        "Paddle tuning tape on curved edge guard",
        "Paddle tuning tape product packaging",
        "Paddle tuning tape on pickleball paddle ready for play",
        "Paddle tuning tape on widebody pickleball paddle",
        "Paddle tuning tape variant options",
    ],
    "drymax-grip": [
        "Pickleball Effect DryMax replacement grip in package",
        "DryMax grip wrapped on pickleball paddle handle",
        "DryMax grip dry-feel surface texture detail",
        "DryMax replacement grip installation",
        "DryMax grip on pickleball paddle for humid play",
        "DryMax grip individual roll",
        "DryMax replacement grip end finish",
        "DryMax grip color options",
        "DryMax grip wrap tension detail",
        "DryMax replacement grip backing strip",
    ],
    "cap-coins": [
        "Pickleball Effect cap coins in 6g 9g 12g sizes",
        "Cap coin applied to pickleball paddle end cap",
        "Cap coin adhesive detail close-up",
        "Cap coins on multiple pickleball paddles",
        "Cap coin lowering balance point on pickleball paddle",
        "Cap coin installation on butt cap",
        "Cap coins product packaging",
    ],
    "cap-coins-3-pack": [
        "Cap coins 3 pack 6g 9g 12g all weights shown",
        "Cap coin 3 pack lined up by weight",
        "Cap coin from 3 pack applied to pickleball paddle end cap",
        "Cap coin 3 pack packaging",
        "Cap coins 3 pack on multiple paddles for testing",
        "Cap coin 3 pack adhesive backing detail",
        "Cap coin 3 pack with paddle for size reference",
    ],
}


def slugify_title(title: str) -> str:
    """Make a filesystem-safe slug from a product title."""
    s = title.lower()
    s = re.sub(r"[^a-z0-9]+", "-", s)
    return s.strip("-")


def _strip_brand_prefix(title: str) -> str:
    """Avoid 'Pickleball Effect Pickleball Effect ...' doubling when titles already
    include the brand. Returns the title with leading 'Pickleball Effect' removed."""
    t = title.strip()
    for prefix in ("Pickleball Effect Shop ", "Pickleball Effect "):
        if t.startswith(prefix):
            return t[len(prefix):].strip()
    return t


def generic_alt(product_title: str, position: int, variant_label: str = "") -> str:
    """Fallback alt for products without a hand-curated pattern."""
    base = _strip_brand_prefix(product_title.replace("|", "—"))
    if variant_label:
        return f"Pickleball Effect {base} — {variant_label} — image {position}"
    return f"Pickleball Effect {base} — image {position}"


def alt_for_image(handle: str, product_title: str, image_index: int, variant_label: str = "") -> str:
    """Pick the best alt text for a given image."""
    pattern_key = HANDLE_ALIASES.get(handle, handle)
    patterns = ALT_PATTERNS.get(pattern_key)
    if patterns and image_index < len(patterns):
        return patterns[image_index]
    if patterns:
        # Beyond the curated list — extend with a position-based variant
        return f"{patterns[0]} — additional view {image_index + 1}"
    return generic_alt(product_title, image_index + 1, variant_label)


def fetch_all_products(client: ShopifyClient) -> list[dict]:
    """Fetch all products with images and variants."""
    products = client.get_paginated(
        "/products.json",
        params={"limit": 250, "status": "active", "fields": "id,title,handle,status,images,variants"},
        key="products",
    )
    return products


def build_changes(
    products: list[dict],
    handle_filter: str | None = None,
    overwrite: bool = False,
) -> list[dict]:
    """For every image with empty or missing alt, build a proposed change row.

    If overwrite=True, also re-flow images that already have alt text (used to
    correct previously-applied bad alts). Default behavior preserves any
    existing alt to avoid stomping manual edits.
    """
    rows = []
    needle = (handle_filter or "").lower()
    for p in products:
        if needle and needle not in p["handle"].lower() and needle not in p["title"].lower():
            continue
        # Map variant_id -> variant title for variant context
        variant_lookup = {v["id"]: v.get("title", "") for v in p.get("variants", [])}
        for idx, img in enumerate(p.get("images", [])):
            existing = (img.get("alt") or "").strip()
            if existing and not overwrite:
                continue  # skip non-empty unless overwrite mode
            # Try to pull a variant label from associated variant_ids
            variant_label = ""
            for vid in img.get("variant_ids", []) or []:
                if vid in variant_lookup:
                    label = variant_lookup[vid]
                    if label and label.lower() != "default title":
                        variant_label = label
                        break
            new_alt = alt_for_image(p["handle"], p["title"], idx, variant_label)
            rows.append({
                "product_id": p["id"],
                "product_handle": p["handle"],
                "product_title": p["title"],
                "image_id": img["id"],
                "image_position": idx + 1,
                "image_src": img.get("src", ""),
                "current_alt": existing,
                "proposed_alt": new_alt,
            })
    return rows


def write_csv(rows: list[dict], path: Path) -> None:
    if not rows:
        path.write_text("(no changes)\n")
        return
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def apply_changes(client: ShopifyClient, rows: list[dict]) -> tuple[int, int]:
    """PUT each image to update its alt. Returns (success, fail)."""
    success = 0
    fail = 0
    for i, r in enumerate(rows, 1):
        url = f"{client.base_url}/products/{r['product_id']}/images/{r['image_id']}.json"
        payload = {"image": {"id": r["image_id"], "alt": r["proposed_alt"]}}
        for attempt in range(3):
            resp = client._session.put(url, json=payload, timeout=30)
            if resp.status_code == 429:
                wait = int(float(resp.headers.get("Retry-After", 5)))
                logger.warning(f"  [{i}/{len(rows)}] 429 rate-limited, waiting {wait}s")
                time.sleep(wait)
                continue
            if 200 <= resp.status_code < 300:
                success += 1
                logger.info(f"  [{i}/{len(rows)}] {r['product_handle']} img {r['image_position']} ✓")
                break
            elif resp.status_code == 403:
                logger.error(
                    f"  [{i}/{len(rows)}] 403 forbidden — token lacks write_products scope. "
                    "Update the Shopify app config and re-issue the token."
                )
                return success, fail + (len(rows) - i + 1)
            else:
                logger.error(f"  [{i}/{len(rows)}] {resp.status_code}: {resp.text[:200]}")
                fail += 1
                break
        # Light pacing — Shopify REST allows ~2 req/sec sustained
        time.sleep(0.6)
    return success, fail


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="Commit changes (default: dry-run).")
    parser.add_argument("--product", default=None, help="Filter by handle or title substring.")
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Re-flow alts even if they already have text. Default skips non-empty alts.",
    )
    args = parser.parse_args()

    # Use the writer-scoped app for both reads and writes (scopes include read_products).
    client = get_writer_client()

    logger.info("Fetching all active products...")
    products = fetch_all_products(client)
    logger.info(f"Got {len(products)} products. Building change set...")

    rows = build_changes(products, handle_filter=args.product, overwrite=args.overwrite)
    mode = "to (re)flow" if args.overwrite else "with missing/empty alt"
    logger.info(f"Identified {len(rows)} images {mode}.")

    csv_path = OUT_DIR / "alt-text-changes.csv"
    write_csv(rows, csv_path)
    logger.info(f"Wrote change set to {csv_path}")

    # Print sample
    for r in rows[:8]:
        logger.info(f"  {r['product_handle'][:40]:<42} img {r['image_position']:>2}: '{r['proposed_alt']}'")
    if len(rows) > 8:
        logger.info(f"  ... and {len(rows) - 8} more — see CSV")

    if not args.apply:
        print()
        print("DRY-RUN. Review the CSV, then re-run with --apply to commit.")
        return 0

    if not rows:
        logger.info("Nothing to apply.")
        return 0

    print()
    print(f"About to apply {len(rows)} alt-text updates. Press Ctrl-C in 5s to abort.")
    time.sleep(5)

    success, fail = apply_changes(client, rows)
    print()
    logger.info(f"Done. Success: {success} | Fail: {fail}")
    return 0 if fail == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
