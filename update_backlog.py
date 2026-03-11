#!/usr/bin/env python3
"""Fetch from Google Sheets and regenerate index.html for Railway deployment.
Runs in GitHub Actions with credentials from environment variables."""

import json
import os
import re
from collections import defaultdict
from datetime import datetime, timezone, timedelta

import requests

# Config
SPREADSHEET_ID = '1gRPcXtBQcSayTvJ1Gp9Z02Zls_HaiN5bDYNvTw9n9RQ'
SHEET_RANGE = 'SG Project Overview!A2:V200'
OUTPUT_FILE = 'index.html'

DOMAIN_COLORS = {
    'OPS': '#3b82f6', 'BD': '#10b981', 'MKT': '#8b5cf6',
    'SPX': '#f97316', 'WH': '#6366f1', 'BI': '#14b8a6', 'Product': '#ec4899',
}
STATUS_ORDER = ['To Start', 'POC', 'Design & Build', 'Parallel Run', 'Live', 'Cancelled']
STATUS_SLUGS = {
    'To Start': 'to-start', 'POC': 'poc', 'Design & Build': 'design-and-build',
    'Parallel Run': 'parallel-run', 'Live': 'live', 'Cancelled': 'cancelled',
}
STATUS_BG = {
    'to-start': '#f3f4f6', 'poc': '#fef3c7', 'design-and-build': '#dbeafe',
    'parallel-run': '#d1fae5', 'live': '#bbf7d0', 'cancelled': '#fecaca',
}

# Column indices (A=0 ... V=21)
COL_ID, COL_DOMAIN, COL_NAME, COL_DESC = 0, 1, 2, 3
COL_IMPACT, COL_PIC, COL_STATUS, COL_ETA = 18, 19, 20, 21


def get_access_token():
    """Exchange refresh token for access token."""
    resp = requests.post('https://oauth2.googleapis.com/token', data={
        'client_id': os.environ['GOOGLE_CLIENT_ID'],
        'client_secret': os.environ['GOOGLE_CLIENT_SECRET'],
        'refresh_token': os.environ['GOOGLE_REFRESH_TOKEN'],
        'grant_type': 'refresh_token',
    })
    resp.raise_for_status()
    return resp.json()['access_token']


def fetch_from_sheets(token):
    url = f'https://sheets.googleapis.com/v4/spreadsheets/{SPREADSHEET_ID}/values/{SHEET_RANGE}'
    resp = requests.get(url, headers={'Authorization': f'Bearer {token}'})
    resp.raise_for_status()
    return resp.json().get('values', [])


def get_cell(row, idx):
    return row[idx] if idx < len(row) else ''


def parse_impact(val):
    if not val:
        return 0
    val = val.replace(',', '').replace('$', '').lower()
    match = re.search(r'([\d.]+)\s*k?\s*(usd|sgd)?', val)
    if match:
        num = float(match.group(1))
        if 'k' in val and num < 1000:
            num *= 1000
        return int(num)
    return 0


def format_pic(pic_str):
    if not pic_str or pic_str.strip() == '':
        return 'Not assigned'
    names = re.split(r'[\n,;]+', pic_str)
    formatted = []
    for name in names:
        name = name.strip()
        if '@' in name:
            name = name.split('@')[0].replace('.', ' ').title()
        if name:
            formatted.append(name.title())
    return ', '.join(formatted[:2]) if formatted else 'Not assigned'


def format_eta(eta_str):
    if not eta_str or not eta_str.strip():
        return 'ETA tbc'
    for fmt in ('%d %B %Y', '%d %b %Y', '%d/%m/%y', '%d/%m/%Y'):
        try:
            dt = datetime.strptime(eta_str.strip(), fmt)
            return 'ETA:' + dt.strftime('%-d %b %y')
        except ValueError:
            continue
    return 'ETA tbc'


def load_projects(rows):
    projects = []
    for row in rows:
        proj_id = get_cell(row, COL_ID).strip()
        if not proj_id or not proj_id.startswith('SGLLM'):
            continue
        domain = get_cell(row, COL_DOMAIN).strip()
        if not domain:
            continue
        project = {
            'id': proj_id, 'domain': domain,
            'name': get_cell(row, COL_NAME),
            'description': get_cell(row, COL_DESC),
            'status': get_cell(row, COL_STATUS).strip(),
            'pic': get_cell(row, COL_PIC),
            'eta': get_cell(row, COL_ETA).strip(),
            'impact': parse_impact(get_cell(row, COL_IMPACT)),
        }
        if project['status'] not in STATUS_ORDER:
            continue
        projects.append(project)
    return projects


def generate_html(projects):
    by_domain_status = defaultdict(lambda: defaultdict(list))
    domain_impacts = defaultdict(int)
    domain_impacts_live = defaultdict(int)
    domain_counts = defaultdict(int)
    status_counts = defaultdict(int)

    for p in projects:
        domain, status = p['domain'], p['status']
        by_domain_status[domain][status].append(p)
        domain_impacts[domain] += p['impact']
        if status == 'Live':
            domain_impacts_live[domain] += p['impact']
        domain_counts[domain] += 1
        status_counts[status] += 1

    total_impact_all = sum(domain_impacts.values())
    total_impact_live = sum(domain_impacts_live.values())

    sorted_domains = sorted(domain_impacts.keys(), key=lambda d: domain_impacts[d], reverse=True)
    for d in DOMAIN_COLORS:
        if d not in sorted_domains:
            sorted_domains.append(d)
    num_domains = len(sorted_domains)

    # SGT timestamp
    sgt = datetime.now(timezone(timedelta(hours=8)))
    timestamp = sgt.strftime("%d%m%Y %H:%M")

    html = '''<!DOCTYPE html>
<html>
<head>
    <title>SG AI Projects Backlog</title>
    <style>
        * { box-sizing: border-box; margin: 0; padding: 0; }
        body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; padding: 20px; background: #f9fafb; }
        h1 { text-align: center; margin-bottom: 10px; color: #111827; }
        .subtitle { text-align: center; color: #6b7280; margin-bottom: 20px; }
        .filter-bar { display: flex; justify-content: center; align-items: center; gap: 10px; margin-bottom: 20px; }
        .filter-bar label { font-size: 14px; font-weight: 600; color: #374151; }
        .filter-bar select { padding: 8px 14px; border: 2px solid #d1d5db; border-radius: 8px; font-size: 14px; font-family: inherit; background: white; color: #111827; cursor: pointer; outline: none; }
        .filter-bar select:focus { border-color: #3b82f6; }
        .legend { display: flex; justify-content: center; gap: 20px; margin-bottom: 20px; flex-wrap: wrap; }
        .legend-item { display: flex; align-items: center; gap: 5px; font-size: 12px; }
        .legend-color { width: 12px; height: 12px; border-radius: 2px; }
        .board { display: grid; gap: 10px; overflow-x: auto; transition: all 0.3s ease; }
        .header-row { display: contents; }
        .header-cell { background: #1f2937; color: white; padding: 12px 8px; text-align: center; font-weight: 600; font-size: 13px; border-radius: 6px 6px 0 0; transition: all 0.3s ease; }
        .header-cell .impact { font-size: 10px; font-weight: normal; opacity: 0.8; margin-top: 4px; }
        .status-label { background: #374151; color: white; padding: 10px 8px; font-weight: 600; font-size: 12px; display: flex; align-items: center; justify-content: center; border-radius: 6px 0 0 6px; }
        .cell { padding: 8px; min-height: 80px; border-radius: 4px; }
        .project {
            background: white; border-radius: 6px; padding: 8px; margin-bottom: 6px;
            box-shadow: 0 1px 3px rgba(0,0,0,0.1); font-size: 11px; border-left: 3px solid;
            transition: all 0.2s; cursor: pointer; position: relative;
        }
        .project:hover { box-shadow: 0 4px 12px rgba(0,0,0,0.15); transform: translateY(-2px); z-index: 100; }
        .project.highlighted { border: 2px solid #facc15; background: #fefce8; box-shadow: 0 0 8px rgba(250,204,21,0.5); }
        .highlight-impact { color: #eab308; font-size: 10px; font-weight: 600; margin-top: 4px; display: none; }
        .highlight-impact.visible { display: block; }
        .subtitle .highlight-total { color: #eab308; font-weight: 600; display: none; }
        .subtitle .highlight-total.visible { display: inline; }
        .project[data-tooltip]:hover::after {
            content: attr(data-tooltip); position: absolute; left: 0; top: 100%;
            background: #1f2937; color: white; padding: 8px 10px; border-radius: 6px;
            font-size: 11px; line-height: 1.4; max-width: 280px; width: max-content;
            z-index: 1000; margin-top: 4px; box-shadow: 0 4px 12px rgba(0,0,0,0.3); white-space: pre-wrap;
        }
        .project-id { font-weight: 600; color: #6b7280; font-size: 9px; }
        .project-name { color: #111827; margin-top: 2px; line-height: 1.3; }
        .project-pic { font-size: 9px; color: #3b82f6; margin-top: 4px; font-weight: 500; }
        .project-impact { font-size: 9px; color: #059669; margin-top: 2px; font-weight: 500; }
        .project-eta { color: #e67e22; font-weight: 500; }
        .count-badge { background: #e5e7eb; color: #374151; padding: 2px 6px; border-radius: 10px; font-size: 10px; font-weight: 600; }
        #filtered-view { display: none; }
        #filtered-view.active { display: block; }
        .fv-status-section { margin-bottom: 20px; }
        .fv-status-header { background: #374151; color: white; padding: 10px 16px; font-weight: 600; font-size: 13px; border-radius: 8px 8px 0 0; display: flex; align-items: center; gap: 8px; }
        .fv-projects-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(220px, 1fr)); gap: 8px; padding: 12px; border-radius: 0 0 8px 8px; }
        .fv-projects-grid .project { margin-bottom: 0; }
    </style>
</head>
<body>
    <h1>SG AI Projects Backlog</h1>
    <p class="subtitle">''' + f'Total Impact: ${total_impact_all:,}/yr | Live Projects: ${total_impact_live:,}/yr' + ''' <span class="highlight-total" id="highlight-total"></span></p>
    <p style="text-align:center;color:#9ca3af;font-size:11px;margin-bottom:10px;">''' + f'last updated {timestamp} hrs' + '''</p>

    <div class="filter-bar">
        <label for="domain-filter">Filter by Domain:</label>
        <select id="domain-filter" onchange="filterDomain(this.value)">
            <option value="all">All Domains</option>
'''

    for domain in sorted_domains:
        count = domain_counts[domain]
        impact = domain_impacts[domain]
        live_impact = domain_impacts_live.get(domain, 0)
        html += f'            <option value="{domain}">{domain} ({count} projects, ${impact:,}/yr, Live: ${live_impact:,})</option>\n'

    html += '''        </select>
    </div>
    <div class="legend" id="legend">
'''
    for domain in sorted_domains:
        color = DOMAIN_COLORS.get(domain, '#6b7280')
        count = domain_counts[domain]
        impact = domain_impacts[domain]
        live_imp = domain_impacts_live.get(domain, 0)
        html += f'<div class="legend-item" data-domain="{domain}"><div class="legend-color" style="background:{color}"></div>{domain} (${impact:,}/yr, Live: ${live_imp:,}, {count} projects)</div>\n'

    html += '</div>\n'
    html += f'<div class="board" id="board" style="grid-template-columns: 100px repeat({num_domains}, minmax(150px, 1fr));">\n'
    html += '<div class="header-row">\n<div class="header-cell" data-domain="status">Status</div>\n'

    for domain in sorted_domains:
        impact = domain_impacts[domain]
        live_impact = domain_impacts_live.get(domain, 0)
        html += f'<div class="header-cell" data-domain="{domain}">{domain}<div class="impact">${impact:,}/yr (Live: ${live_impact:,})</div><div class="highlight-impact" id="hi-{domain}"></div></div>\n'
    html += '</div>\n'

    for status in STATUS_ORDER:
        slug = STATUS_SLUGS[status]
        bg = STATUS_BG[slug]
        count = status_counts[status]
        html += f'<div class="status-label" data-status="{slug}">{status} <span class="count-badge" style="margin-left:5px">{count}</span></div>\n'

        for domain in sorted_domains:
            color = DOMAIN_COLORS.get(domain, '#6b7280')
            html += f'<div class="cell" data-domain="{domain}" data-status="{slug}" style="background:{bg}">\n'
            projs = by_domain_status[domain].get(status, [])
            for p in sorted(projs, key=lambda x: x['impact'], reverse=True):
                name = p['name'].split('\n')[0][:80]
                pic = format_pic(p['pic'])
                desc = p.get('description', '').replace('"', '&quot;').replace('\n', ' ').strip()[:300]
                desc_attr = f' data-tooltip="{desc}"' if desc else ''
                html += f'''<div class="project" style="border-left-color:{color}" data-impact="{p['impact']}" data-domain="{domain}"{desc_attr} onclick="toggleHighlight(this)">
                <div class="project-id">{p['id']}</div>
                <div class="project-name">{name}</div>
                <div class="project-pic">{pic}</div>
'''
                eta_formatted = format_eta(p.get('eta', ''))
                show_eta = eta_formatted and status != 'Live'
                if p['impact'] > 0 or show_eta:
                    html += '                <div class="project-impact">'
                    if p['impact'] > 0:
                        html += f'${p["impact"]:,}/yr'
                    if show_eta:
                        html += f' <span class="project-eta">[{eta_formatted}]</span>'
                    html += '</div>\n'
                html += '            </div>\n'
            html += '</div>\n'

    html += '</div>\n'

    html += '''
<div id="filtered-view"></div>
<script>
const STATUS_BG = {'to-start':'#f3f4f6','poc':'#fef3c7','design-and-build':'#dbeafe','parallel-run':'#d1fae5','live':'#bbf7d0','cancelled':'#fecaca'};
const STATUS_LABELS = {'to-start':'To Start','poc':'POC','design-and-build':'Design & Build','parallel-run':'Parallel Run','live':'Live','cancelled':'Cancelled'};
const STATUS_ORDER = ['to-start','poc','design-and-build','parallel-run','live','cancelled'];
const highlightedIds = new Set();

function toggleHighlight(el) {
    const projectId = el.querySelector('.project-id').textContent;
    el.classList.toggle('highlighted');
    if (el.classList.contains('highlighted')) { highlightedIds.add(projectId); } else { highlightedIds.delete(projectId); }
    document.querySelectorAll('.project').forEach(p => {
        const pid = p.querySelector('.project-id').textContent;
        p.classList.toggle('highlighted', highlightedIds.has(pid));
    });
    updateHighlightTotals();
}

function updateHighlightTotals() {
    const domainTotals = {}; let grandTotal = 0, count = 0;
    document.querySelectorAll('#board .project.highlighted').forEach(p => {
        const impact = parseInt(p.getAttribute('data-impact') || '0');
        const domain = p.getAttribute('data-domain');
        domainTotals[domain] = (domainTotals[domain] || 0) + impact;
        grandTotal += impact; count++;
    });
    document.querySelectorAll('.highlight-impact').forEach(el => {
        const domain = el.id.replace('hi-', ''), val = domainTotals[domain] || 0;
        if (val > 0) { el.textContent = 'Highlighted: $' + val.toLocaleString() + '/yr'; el.classList.add('visible'); }
        else { el.classList.remove('visible'); }
    });
    const totalEl = document.getElementById('highlight-total');
    if (grandTotal > 0) { totalEl.textContent = '| Highlighted: $' + grandTotal.toLocaleString() + '/yr (' + count + ' projects)'; totalEl.classList.add('visible'); }
    else { totalEl.classList.remove('visible'); }
}

function filterDomain(domain) {
    const board = document.getElementById('board'), filteredView = document.getElementById('filtered-view'), legend = document.getElementById('legend');
    if (domain === 'all') { board.style.display = 'grid'; filteredView.className = ''; filteredView.innerHTML = ''; legend.querySelectorAll('.legend-item').forEach(l => l.style.display = ''); return; }
    board.style.display = 'none'; filteredView.className = 'active';
    legend.querySelectorAll('.legend-item').forEach(l => { l.style.display = l.getAttribute('data-domain') === domain ? '' : 'none'; });
    let html = '';
    STATUS_ORDER.forEach(statusSlug => {
        const cell = board.querySelector(`.cell[data-domain="${domain}"][data-status="${statusSlug}"]`);
        if (!cell) return;
        const projects = cell.querySelectorAll('.project');
        if (projects.length === 0) return;
        html += `<div class="fv-status-section"><div class="fv-status-header">${STATUS_LABELS[statusSlug]} <span class="count-badge" style="margin-left:4px">${projects.length}</span></div>`;
        html += `<div class="fv-projects-grid" style="background:${STATUS_BG[statusSlug]}">`;
        projects.forEach(p => { html += p.outerHTML; });
        html += `</div></div>`;
    });
    filteredView.innerHTML = html;
    filteredView.querySelectorAll('.project').forEach(p => {
        const pid = p.querySelector('.project-id').textContent;
        if (highlightedIds.has(pid)) p.classList.add('highlighted');
    });
}
</script>
</body>
</html>'''

    return html


def main():
    print("Fetching access token...")
    token = get_access_token()

    print("Fetching data from Google Sheets...")
    rows = fetch_from_sheets(token)
    print(f"  Got {len(rows)} rows")

    projects = load_projects(rows)
    print(f"Found {len(projects)} projects")

    status_counts = defaultdict(int)
    for p in projects:
        status_counts[p['status']] += 1
    print("By status:", dict(status_counts))

    print(f"Generating {OUTPUT_FILE}...")
    html = generate_html(projects)
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        f.write(html)
    print("Done!")


if __name__ == '__main__':
    main()
