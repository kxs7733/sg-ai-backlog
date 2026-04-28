#!/usr/bin/env python3
"""Daily SG real-estate deal recommender.

Pulls live listings from PropertyGuru (private condo resale, EC, landed),
computes per-project and per-district median asking PSF from the listing pool
itself, and pushes the top 3 listings (under SGD 2M) with the largest PSF
discount vs their peer-group median to Telegram.

Caveat: comps are asking-prices, not transacted prices. A listing flagged as
"-15% vs project" means it's 15% below the median ask of comparable PG
listings, not 15% below the last URA caveat. Useful for spotting underpriced
listings, but worth sanity-checking before action.

Required env vars:
  TELEGRAM_BOT_TOKEN
  TELEGRAM_CHAT_ID

Optional env vars:
  MAX_PRICE_SGD       - default 2_000_000 (ranking cap)
  COMP_PRICE_CAP_SGD  - default 1.5 * MAX_PRICE (comp pool search cap)
  TOP_N               - default 3
  DRY_RUN             - "1" prints to stdout instead of Telegram
"""

import json
import os
import re
import sys
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from statistics import median

import requests

try:
    import cloudscraper  # type: ignore
except ImportError:
    cloudscraper = None

MAX_PRICE = int(os.environ.get('MAX_PRICE_SGD', '2000000'))
COMP_PRICE_CAP = int(os.environ.get('COMP_PRICE_CAP_SGD', str(int(MAX_PRICE * 1.5))))
TOP_N = int(os.environ.get('TOP_N', '3'))
DRY_RUN = os.environ.get('DRY_RUN') == '1'

UA = (
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 '
    '(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36'
)

# ---------- Comps from PG listing pool ------------------------------------
# Asking-price-vs-asking-price comps. For each target listing, median PSF of
# OTHER PG listings (under COMP_PRICE_CAP) in the same project (>=3 comps),
# else same district (>=5 comps). Self always excluded from the pool.


def comps_index(listings: list[dict]) -> dict:
    by_project: dict = defaultdict(list)  # (project_upper, district) -> [(id, psf)]
    by_district: dict = defaultdict(list)  # district -> [(id, psf)]
    for L in listings:
        proj = (L.get('project') or '').strip().upper()
        dist = L.get('district') or ''
        psf = L.get('psf')
        if not psf:
            continue
        if proj and dist:
            by_project[(proj, dist)].append((L['id'], psf))
        if dist:
            by_district[dist].append((L['id'], psf))
    return {'project': dict(by_project), 'district': dict(by_district)}


def comp_psf(
    idx: dict, project: str, district: str, self_id
) -> tuple[float | None, int, str]:
    """Return (median_psf, n_comps, scope). Excludes self by id."""
    proj = (project or '').strip().upper()
    pkey = (proj, district)
    proj_pool = [psf for lid, psf in idx['project'].get(pkey, []) if lid != self_id]
    if len(proj_pool) >= 3:
        return median(proj_pool), len(proj_pool), 'project'
    dist_pool = [psf for lid, psf in idx['district'].get(district, []) if lid != self_id]
    if len(dist_pool) >= 5:
        return median(dist_pool), len(dist_pool), 'district'
    return None, 0, 'none'


# ---------- PropertyGuru scraping ----------------------------------------
# PG embeds listing data in a <script id="__NEXT_DATA__"> JSON blob on most
# pages. We hit the search URL for each segment and parse that blob. Cloudflare
# may block; we use cloudscraper as a fallback and retry with backoff.

PG_BASE = 'https://www.propertyguru.com.sg'

# (label, path)  - landed includes terraced/semi-d/detached/cluster
PG_SEGMENTS = [
    ('Condo (resale)',
     '/property-for-sale?property_type=N&property_type_code[]=CONDO&'
     'property_type_code[]=APT&property_type_code[]=WALK&listing_type=sale&'
     'maxprice={max_price}&isCommercial=0'),
    ('Executive Condo',
     '/property-for-sale?property_type=N&property_type_code[]=EXCON&'
     'listing_type=sale&maxprice={max_price}&isCommercial=0'),
    ('Landed',
     '/property-for-sale?property_type=L&property_type_code[]=TERRA&'
     'property_type_code[]=SEMI&property_type_code[]=BUNG&'
     'property_type_code[]=CLUS&listing_type=sale&maxprice={max_price}&'
     'isCommercial=0'),
]

# Listing JSON path inside __NEXT_DATA__ has shifted historically; we walk the
# tree looking for objects that look like listings.
LISTING_KEYS_HINT = {'price', 'priceFormatted', 'floorArea', 'address'}


def pg_fetch_html(url: str) -> str | None:
    headers = {
        'User-Agent': UA,
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-SG,en;q=0.9',
    }
    for attempt in range(3):
        try:
            r = requests.get(url, headers=headers, timeout=30)
            if r.status_code == 200 and 'cf-chl' not in r.text[:2000].lower():
                return r.text
        except requests.RequestException as e:
            print(f'  requests attempt {attempt+1} failed: {e}')
        time.sleep(2 ** attempt)
    if cloudscraper is None:
        print('  cloudscraper not installed; cannot bypass Cloudflare')
        return None
    try:
        scraper = cloudscraper.create_scraper(
            browser={'browser': 'chrome', 'platform': 'darwin', 'desktop': True}
        )
        r = scraper.get(url, timeout=45)
        if r.status_code == 200:
            return r.text
        print(f'  cloudscraper status {r.status_code}')
    except Exception as e:
        print(f'  cloudscraper failed: {e}')
    return None


def pg_extract_next_data(html: str) -> dict | None:
    m = re.search(
        r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>',
        html,
        re.DOTALL,
    )
    if not m:
        return None
    try:
        return json.loads(m.group(1))
    except json.JSONDecodeError:
        return None


def _walk(node, found: list):
    """DFS for listing-shaped dicts."""
    if isinstance(node, dict):
        keys = set(node.keys())
        # PG listings typically carry these keys
        if {'id', 'listingType'}.issubset(keys) and (
            'price' in keys or 'priceFormatted' in keys
        ):
            found.append(node)
        for v in node.values():
            _walk(v, found)
    elif isinstance(node, list):
        for v in node:
            _walk(v, found)


def pg_parse_listings(blob: dict, segment: str) -> list[dict]:
    found: list[dict] = []
    _walk(blob, found)
    out = []
    for L in found:
        try:
            price = L.get('price') or L.get('priceValue') or 0
            if isinstance(price, str):
                price = float(re.sub(r'[^\d.]', '', price) or 0)
            price = float(price)
            if price <= 0 or price > COMP_PRICE_CAP:
                continue
            sqft = (
                L.get('floorArea')
                or L.get('size')
                or L.get('sizeSqft')
                or 0
            )
            if isinstance(sqft, dict):
                sqft = sqft.get('value', 0)
            if isinstance(sqft, str):
                sqft = float(re.sub(r'[^\d.]', '', sqft) or 0)
            sqft = float(sqft)
            if sqft <= 0:
                continue
            psf = price / sqft
            project = (
                L.get('projectName')
                or L.get('developmentName')
                or L.get('localizedTitle')
                or L.get('title')
                or ''
            )
            address = L.get('address') or {}
            district = ''
            if isinstance(address, dict):
                district = (
                    address.get('district')
                    or address.get('districtCode')
                    or ''
                )
            district = str(district).strip()
            # PG district is "D15" or "15"; normalize to zero-padded "15"
            dm = re.search(r'(\d{1,2})', district)
            district = f'{int(dm.group(1)):02d}' if dm else ''
            url_path = L.get('url') or L.get('listingUrl') or ''
            if url_path and not url_path.startswith('http'):
                url_path = PG_BASE + url_path
            out.append({
                'segment': segment,
                'id': L.get('id'),
                'project': str(project).strip(),
                'district': district,
                'price': price,
                'sqft': sqft,
                'psf': psf,
                'beds': L.get('bedrooms') or L.get('beds') or '',
                'url': url_path,
                'title': L.get('localizedTitle') or L.get('title') or '',
            })
        except (ValueError, TypeError):
            continue
    # Dedupe by id
    seen = set()
    deduped = []
    for L in out:
        if L['id'] in seen:
            continue
        seen.add(L['id'])
        deduped.append(L)
    return deduped


def pg_listings(search_price_cap: int) -> tuple[list[dict], list[str]]:
    listings: list[dict] = []
    errors: list[str] = []
    for label, path in PG_SEGMENTS:
        url = PG_BASE + path.format(max_price=search_price_cap)
        print(f'Scraping {label}: {url}')
        html = pg_fetch_html(url)
        if not html:
            errors.append(f'{label}: fetch blocked')
            continue
        blob = pg_extract_next_data(html)
        if not blob:
            errors.append(f'{label}: no __NEXT_DATA__ blob')
            continue
        seg_listings = pg_parse_listings(blob, label)
        print(f'  parsed {len(seg_listings)} listings')
        listings.extend(seg_listings)
    return listings, errors


# ---------- Scoring -------------------------------------------------------

def score_listings(listings: list[dict], idx: dict, max_price: int) -> list[dict]:
    scored = []
    for L in listings:
        if L['price'] > max_price:
            continue
        med, n, scope = comp_psf(idx, L['project'], L['district'], L['id'])
        if med is None or med <= 0:
            continue
        discount_pct = (med - L['psf']) / med * 100.0
        scored.append({
            **L,
            'comp_psf': med,
            'comp_n': n,
            'comp_scope': scope,
            'discount_pct': discount_pct,
        })
    scored.sort(key=lambda x: x['discount_pct'], reverse=True)
    return scored


# ---------- Telegram ------------------------------------------------------

def fmt_money(v: float) -> str:
    if v >= 1_000_000:
        return f'${v/1_000_000:.2f}M'
    return f'${v:,.0f}'


def render_message(top: list[dict], errors: list[str]) -> str:
    sgt = datetime.now(timezone(timedelta(hours=8)))
    lines = [f'*SG Deal Recs — {sgt.strftime("%a %d %b %Y")}*', '']
    if not top:
        lines.append('_No qualifying listings with comps today._')
    for i, L in enumerate(top, 1):
        beds = f' · {L["beds"]}br' if L['beds'] else ''
        lines += [
            f'*{i}. {L["project"] or L["title"] or "Listing"}* (D{L["district"] or "?"}, {L["segment"]})',
            f'   {fmt_money(L["price"])} · {int(L["sqft"])} sqft · ${L["psf"]:.0f} psf{beds}',
            f'   Comp: ${L["comp_psf"]:.0f} psf ({L["comp_scope"]}, n={L["comp_n"]}) → '
            f'*{L["discount_pct"]:+.1f}%*',
            f'   {L["url"]}',
            '',
        ]
    if errors:
        lines += ['_Notes:_'] + [f'• {e}' for e in errors]
    return '\n'.join(lines)


def send_telegram(text: str) -> None:
    token = os.environ['TELEGRAM_BOT_TOKEN']
    chat_id = os.environ['TELEGRAM_CHAT_ID']
    r = requests.post(
        f'https://api.telegram.org/bot{token}/sendMessage',
        json={
            'chat_id': chat_id,
            'text': text,
            'parse_mode': 'Markdown',
            'disable_web_page_preview': False,
        },
        timeout=30,
    )
    if r.status_code != 200:
        # Markdown can fail on weird chars; retry plain
        r = requests.post(
            f'https://api.telegram.org/bot{token}/sendMessage',
            json={'chat_id': chat_id, 'text': text},
            timeout=30,
        )
    r.raise_for_status()


# ---------- Main ----------------------------------------------------------

def main() -> int:
    errors: list[str] = []

    print(
        f'Scraping PropertyGuru up to {fmt_money(COMP_PRICE_CAP)} '
        f'(ranking cap {fmt_money(MAX_PRICE)})...'
    )
    listings, scrape_errs = pg_listings(COMP_PRICE_CAP)
    errors.extend(scrape_errs)
    print(f'  {len(listings)} total listings in comp pool')

    idx = comps_index(listings)
    print(
        f'  {len(idx["project"])} (project, district) keys, '
        f'{len(idx["district"])} district keys'
    )

    scored = score_listings(listings, idx, MAX_PRICE)
    print(f'  {len(scored)} listings under {fmt_money(MAX_PRICE)} have usable comps')
    top = scored[:TOP_N]

    msg = render_message(top, errors)
    print('\n--- message ---\n' + msg + '\n--- end ---')

    if DRY_RUN:
        print('DRY_RUN=1, not sending')
        return 0
    if not os.environ.get('TELEGRAM_BOT_TOKEN'):
        print('TELEGRAM_BOT_TOKEN missing, skipping send')
        return 2
    send_telegram(msg)
    print('Sent.')
    return 0


if __name__ == '__main__':
    sys.exit(main())
