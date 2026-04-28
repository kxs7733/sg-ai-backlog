#!/usr/bin/env python3
"""Daily SG real-estate deal recommender.

Pulls live listings from PropertyGuru (private condo resale, EC, landed) under
SGD 2M, scores each against URA private-residential caveats from the last 12
months in the same project / district, and pushes the top 3 PSF discounts to
Telegram.

Required env vars:
  URA_ACCESS_KEY    - registered on https://www.ura.gov.sg/maps/api/
  TELEGRAM_BOT_TOKEN
  TELEGRAM_CHAT_ID

Optional env vars:
  MAX_PRICE_SGD     - default 2_000_000
  TOP_N             - default 3
  LOOKBACK_MONTHS   - default 12
  DRY_RUN           - "1" prints to stdout instead of Telegram
"""

import json
import os
import re
import sys
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from statistics import median
from urllib.parse import quote

import requests

try:
    import cloudscraper  # type: ignore
except ImportError:
    cloudscraper = None

MAX_PRICE = int(os.environ.get('MAX_PRICE_SGD', '2000000'))
TOP_N = int(os.environ.get('TOP_N', '3'))
LOOKBACK_MONTHS = int(os.environ.get('LOOKBACK_MONTHS', '12'))
DRY_RUN = os.environ.get('DRY_RUN') == '1'

UA = (
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 '
    '(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36'
)

# ---------- URA caveats ---------------------------------------------------

URA_TOKEN_URL = 'https://eservice.ura.gov.sg/uraDataService/insertNewToken/v1'
URA_TXN_URL = (
    'https://eservice.ura.gov.sg/uraDataService/invokeUraDS/v1'
    '?service=PMI_Resi_Transaction&batch={batch}'
)


def ura_token(access_key: str) -> str:
    r = requests.get(
        URA_TOKEN_URL,
        headers={'AccessKey': access_key, 'User-Agent': UA},
        timeout=30,
    )
    r.raise_for_status()
    data = r.json()
    if data.get('Status') != 'Success':
        raise RuntimeError(f'URA token error: {data}')
    return data['Result']


def ura_caveats(access_key: str, token: str) -> list[dict]:
    """Pull all 4 batches of URA private resi transactions (~last 5 yrs)."""
    rows: list[dict] = []
    for batch in (1, 2, 3, 4):
        r = requests.get(
            URA_TXN_URL.format(batch=batch),
            headers={
                'AccessKey': access_key,
                'Token': token,
                'User-Agent': UA,
            },
            timeout=60,
        )
        r.raise_for_status()
        data = r.json()
        if data.get('Status') != 'Success':
            print(f'  URA batch {batch} non-success: {data.get("Message")}')
            continue
        for project in data.get('Result', []):
            project_name = (project.get('project') or '').strip().upper()
            street = (project.get('street') or '').strip()
            district = (project.get('district') or '').strip()
            market = project.get('marketSegment', '')  # CCR/RCR/OCR
            for tx in project.get('transaction', []):
                try:
                    psf = float(tx.get('price', 0)) / float(tx.get('area', 0)) / 10.7639
                except (ValueError, ZeroDivisionError, TypeError):
                    continue
                rows.append({
                    'project': project_name,
                    'street': street,
                    'district': district,
                    'market': market,
                    'price': float(tx.get('price', 0)),
                    'area_sqm': float(tx.get('area', 0)),
                    'psf_sgd': psf,
                    'contract_date': tx.get('contractDate', ''),  # MMYY
                    'property_type': tx.get('propertyType', ''),
                    'tenure': tx.get('tenure', ''),
                    'type_of_sale': tx.get('typeOfSale', ''),
                })
    return rows


def caveat_date(mmyy: str) -> datetime | None:
    if not mmyy or len(mmyy) != 4:
        return None
    try:
        m, y = int(mmyy[:2]), int(mmyy[2:])
        return datetime(2000 + y, m, 1)
    except ValueError:
        return None


def comps_index(caveats: list[dict]) -> dict:
    """Build (project, district) -> recent PSF list within lookback window."""
    cutoff = datetime.now() - timedelta(days=LOOKBACK_MONTHS * 30)
    idx: dict = defaultdict(list)
    by_district: dict = defaultdict(list)
    for c in caveats:
        d = caveat_date(c['contract_date'])
        if not d or d < cutoff:
            continue
        idx[(c['project'], c['district'])].append(c['psf_sgd'])
        by_district[c['district']].append(c['psf_sgd'])
    return {'project': dict(idx), 'district': dict(by_district)}


def comp_psf(idx: dict, project: str, district: str) -> tuple[float | None, int, str]:
    """Return (median_psf, n_comps, scope). Project-level if >=3 comps, else district."""
    proj = (project or '').strip().upper()
    pkey = (proj, district)
    proj_psfs = idx['project'].get(pkey, [])
    if len(proj_psfs) >= 3:
        return median(proj_psfs), len(proj_psfs), 'project'
    dist_psfs = idx['district'].get(district, [])
    if len(dist_psfs) >= 5:
        return median(dist_psfs), len(dist_psfs), 'district'
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
            if price <= 0 or price > MAX_PRICE:
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


def pg_listings(max_price: int) -> tuple[list[dict], list[str]]:
    listings: list[dict] = []
    errors: list[str] = []
    for label, path in PG_SEGMENTS:
        url = PG_BASE + path.format(max_price=max_price)
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

def score_listings(listings: list[dict], idx: dict) -> list[dict]:
    scored = []
    for L in listings:
        med, n, scope = comp_psf(idx, L['project'], L['district'])
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
    # Best deals = biggest positive discount
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

    access_key = os.environ.get('URA_ACCESS_KEY')
    if not access_key:
        print('URA_ACCESS_KEY missing', file=sys.stderr)
        return 2

    # Diagnostic only - never log full key
    masked = (
        f'{access_key[:4]}...{access_key[-4:]}' if len(access_key) >= 8 else '<short>'
    )
    print(
        f'URA_ACCESS_KEY loaded: len={len(access_key)} masked={masked}'
    )

    print('Fetching URA token...')
    token = ura_token(access_key)
    print('Pulling URA caveats...')
    caveats = ura_caveats(access_key, token)
    print(f'  {len(caveats)} caveat rows')
    idx = comps_index(caveats)
    print(
        f'  {len(idx["project"])} project comp keys, '
        f'{len(idx["district"])} district keys'
    )

    print('Scraping PropertyGuru...')
    listings, scrape_errs = pg_listings(MAX_PRICE)
    errors.extend(scrape_errs)
    print(f'  {len(listings)} total listings under {fmt_money(MAX_PRICE)}')

    scored = score_listings(listings, idx)
    print(f'  {len(scored)} listings have usable comps')
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
