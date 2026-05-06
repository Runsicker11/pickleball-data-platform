"""Render the comparison page to a paste-ready HTML file.

Use when the Shopify Admin token doesn't have write_content scope.
The output file can be opened in a browser preview and the body
pasted directly into Shopify admin (Online Store -> Pages -> Add page).
"""

import json
from pathlib import Path

from scripts.seo.comparison_page_content import (
    FAQ_SCHEMA,
    META_DESCRIPTION,
    PAGE_BODY_HTML,
    PAGE_HANDLE,
    PAGE_TITLE,
    SEO_TITLE,
)

OUT_DIR = Path(__file__).resolve().parents[2] / "out" / "seo"
OUT_DIR.mkdir(parents=True, exist_ok=True)


def render() -> Path:
    schema_block = (
        f'<script type="application/ld+json">\n'
        f"{json.dumps(FAQ_SCHEMA, indent=2)}\n"
        f"</script>\n"
    )
    body_with_schema = schema_block + PAGE_BODY_HTML

    # Standalone preview HTML (with light styles for readability)
    preview = f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>PREVIEW: {PAGE_TITLE}</title>
<meta name="description" content="{META_DESCRIPTION}">
<style>
body {{ max-width: 760px; margin: 2em auto; font-family: -apple-system, system-ui, sans-serif; line-height: 1.6; padding: 0 1em; color: #222; }}
h1 {{ margin-bottom: .25em; }}
h2 {{ margin-top: 2em; border-bottom: 1px solid #eee; padding-bottom: .25em; }}
table {{ border-collapse: collapse; margin: 1em 0; width: 100%; }}
th, td {{ border: 1px solid #ddd; padding: .5em .75em; text-align: left; }}
th {{ background: #f7f7f7; }}
.preview-banner {{ background: #fffbcc; border: 1px solid #f0d000; padding: .75em 1em; margin-bottom: 2em; }}
em {{ color: #555; }}
</style>
</head>
<body>
<div class="preview-banner">
<strong>PREVIEW</strong> — paste from this file into Shopify admin. SEO meta below.
<br>Page title: <code>{SEO_TITLE}</code>
<br>Meta desc: <code>{META_DESCRIPTION}</code>
<br>URL handle: <code>{PAGE_HANDLE}</code>
</div>
<h1>{PAGE_TITLE}</h1>
{body_with_schema}
</body>
</html>"""

    preview_path = OUT_DIR / f"{PAGE_HANDLE}-preview.html"
    preview_path.write_text(preview, encoding="utf-8")

    # Body-only paste-ready file (this is what goes in Shopify Page body)
    paste_path = OUT_DIR / f"{PAGE_HANDLE}-body.html"
    paste_path.write_text(body_with_schema, encoding="utf-8")

    # Quick reference of meta fields
    ref_path = OUT_DIR / f"{PAGE_HANDLE}-meta.txt"
    ref_path.write_text(
        f"PAGE TITLE (in body):  {PAGE_TITLE}\n"
        f"URL HANDLE:            {PAGE_HANDLE}\n"
        f"SEO PAGE TITLE TAG:    {SEO_TITLE}  ({len(SEO_TITLE)} chars)\n"
        f"META DESCRIPTION:      {META_DESCRIPTION}  ({len(META_DESCRIPTION)} chars)\n"
        f"\n"
        f"To create in Shopify admin:\n"
        f"  1. Online Store -> Pages -> Add page\n"
        f"  2. Title: {PAGE_TITLE}\n"
        f"  3. Content: paste contents of {paste_path.name}\n"
        f"  4. Visibility: leave 'Hidden' (this is a draft)\n"
        f"  5. Search engine listing -> Edit:\n"
        f"     Page title: {SEO_TITLE}\n"
        f"     Description: {META_DESCRIPTION}\n"
        f"     URL handle: {PAGE_HANDLE}\n"
        f"  6. Save\n",
        encoding="utf-8",
    )

    print(f"Wrote:\n  {preview_path}\n  {paste_path}\n  {ref_path}\n")
    print(f"Open the preview in a browser:\n  open {preview_path}\n")
    return preview_path


if __name__ == "__main__":
    render()
