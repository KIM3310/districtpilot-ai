from __future__ import annotations

import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_cloudflare_adsense_static_surface_is_ready() -> None:
    adsense_client = "ca-pub-4973160293737562"
    ads_txt = "google.com, pub-4973160293737562, DIRECT, f08c47fec0942fa0"
    canonical = "https://districtpilot-ai.pages.dev/"

    assert (ROOT / "site" / "ads.txt").read_text(encoding="utf-8").strip() == ads_txt

    index = (ROOT / "site" / "index.html").read_text(encoding="utf-8")
    assert f'name="google-adsense-account" content="{adsense_client}"' in index
    loader = f"adsbygoogle.js?client={adsense_client}"
    assert loader not in index

    for filename in ("guide.html", "architecture.html", "verification.html"):
        assert loader in (ROOT / "site" / filename).read_text(encoding="utf-8")
    for filename in ("publisher.html", "privacy.html", "terms.html"):
        html = (ROOT / "site" / filename).read_text(encoding="utf-8")
        assert loader not in html

    sitemap = (ROOT / "site" / "sitemap.xml").read_text(encoding="utf-8")
    for route in (
        "guide",
        "architecture",
        "verification",
        "publisher",
        "privacy",
        "terms",
    ):
        assert f"https://districtpilot-ai.pages.dev/{route}" in sitemap

    llms = (ROOT / "site" / "llms.txt").read_text(encoding="utf-8")
    assert f"Canonical URL: {canonical}" in llms

    offer = json.loads((ROOT / "site" / "service-offer.json").read_text(encoding="utf-8"))
    assert offer["canonical_url"] == canonical
    assert offer["structured_data"]["url"] == canonical
    assert offer["structured_data"]["offers"][0]["url"] == canonical

    wrangler = json.loads((ROOT / "wrangler.jsonc").read_text(encoding="utf-8"))
    assert wrangler["name"] == "districtpilot-ai"
    assert wrangler["pages_build_output_dir"] == "site"
