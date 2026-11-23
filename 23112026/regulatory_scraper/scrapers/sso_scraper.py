'''
SSO discovery: which Singapore Statutes Online items fall inside the scan period for a category.

Built on scrapers/mas_scraper.py (the master scraper) and scrapers/mha_scraper.py: same
structure, same category parameters as mas_config.yaml. Everything after discovery (item
pages, downloads, decisions, storage, the run loop) is shared and lives in
scrapers/base_scraper.py.

How SSO items are found (follows the intake tracker)
  Each category reads one Browse supplement (Homepage > Browse > ...):
    1_ACTS_SUPPLEMENT_NEW_ACTS        Acts Supplement, titles WITHOUT 'Amendment'
    2_ACTS_SUPPLEMENT_AMENDMENT_ACTS  Acts Supplement, titles WITH 'Amendment'
    3_SL_SUPPLEMENT                   Subsidiary Legislation Supplement, Tax and CPF excluded
    4_BILLS_SUPPLEMENT                Bills Supplement
  Categories 1 and 2 read the same listing and are split by title (TITLE_RULES), so every
  Act lands in exactly one of them.

No page selectors to configure
  The SSO site sits behind a bot challenge, so its HTML could not be inspected. Instead of
  CSS selectors, discovery relies on the SSO link format, which is stable:
    Browse page   /Browse/Acts-Supp            (the Entry URL)
    Item page     /Acts-Supp/<number>/Published/<yyyymmdd>?...
  An item is any link in the page whose path starts with /<supplement>/, the supplement
  being the last part of the Entry URL path (Acts-Supp, SL-Supp, Bills-Supp).
    title           the link text (longest one when a row links the same item twice)
    item_date       /Published/<yyyymmdd> in the link, else the first date in the row text
    item_reference  the <number> part of the link, for example '5-2026' or 'S 123-2026'
  Further pages are found from the page's own Next link (no page parameter to configure).
  Pages are read until there is no Next link, or (newest first) every item on a page is
  older than the scan start. Stopping on the whole page, not its oldest item, keeps items
  that are listed slightly out of date order (the SSO sort is by year).
  The supplements only filter by year and Title keyword (no month or date range), so
  filter_by_date_in_url must be false and the scan period is applied from the item date.

Zero items
  Nothing in the period is recorded as 0 items. A listing that shows no items at all is also
  recorded as 0 items, with a warning to check the Entry URL.

Safety checks that fail a category
  1. The page did not show any item link or the no results text in time (this includes the
     bot challenge not being passed).
  2. More pages than max_pages_per_category.
'''

import re
from datetime import datetime
from urllib.parse import unquote, urlsplit

from playwright.async_api import TimeoutError as PlaywrightTimeoutError

from scrapers.base_scraper import BaseScraper, PermanentError, RetryableError
from scrapers.tracking import normalize_url

LISTING_TYPES = {'table'}
DATE_SOURCES = {'listing_date'}

NO_RESULTS_TEXT = 'No record'           # text that counts as an empty listing (optional)
DATE_LABEL = 'Published Date'           # name stored for the item date

# Business rules (from the intake tracker), per category. 'include': keep only titles with
# one of the words. 'exclude': drop titles with any of the words. Case insensitive, whole
# words or phrases. Review the word lists with the business owner.
AMENDMENT_WORDS = ['Amendment', 'Amendments']
TAX_CPF_WORDS = ['Tax', 'Taxes', 'Income Tax', 'Goods and Services Tax', 'Stamp Duties',
                 'Property Tax', 'Central Provident Fund', 'CPF']
TITLE_RULES = {
    '1_ACTS_SUPPLEMENT_NEW_ACTS': {'exclude': AMENDMENT_WORDS},
    '2_ACTS_SUPPLEMENT_AMENDMENT_ACTS': {'include': AMENDMENT_WORDS},
    '3_SL_SUPPLEMENT': {'exclude': TAX_CPF_WORDS},
}
# -------------------------------------------------------------------------------------------

LISTING_EMPTY_WARNING = ('Listing showed no items at all. Recorded as 0 items. '
                         'Check that the Entry URL still works.')
LISTING_PARTIAL_WARNING = ('The Next page added no new items before reaching the scan start. '
                           'Items may be missing: check the paging of the Entry URL.')

_DATE_RE = re.compile(r'(\d{1,2}) ([A-Za-z]+) (\d{4})')
_PUBLISHED_RE = re.compile(r'/Published/(\d{8})(?:[/?#]|$)', re.IGNORECASE)


# =====================================================================
# Pure helpers (unit tested without a browser)
# =====================================================================

def parse_display_date(text):
    '''First date written like 04 September 2026 (or 4 Sep 2026) in the text, or None.'''
    match = _DATE_RE.search(text or '')
    if not match:
        return None
    for fmt in ('%d %B %Y', '%d %b %Y'):
        try:
            return datetime.strptime(' '.join(match.groups()), fmt).date()
        except ValueError:
            continue
    return None


def supplement_of(category):
    '''The supplement name from the Entry URL: the part after /Browse/. Both /Browse/Acts-Supp
    and /Browse/Acts-Supp/Published/All/0?... give Acts-Supp.'''
    parts = [p for p in urlsplit(category['listing_url']).path.split('/') if p]
    lowered = [p.lower() for p in parts]
    if 'browse' in lowered and lowered.index('browse') + 1 < len(parts):
        return parts[lowered.index('browse') + 1]
    return parts[-1]


def item_path_pattern(category):
    '''Regex (shared with the browser script) for item link paths of the category supplement.'''
    return '^/' + re.escape(supplement_of(category)) + '/[^/]+'


def published_date(url):
    '''The date in /Published/<yyyymmdd> of an item link, or None.'''
    match = _PUBLISHED_RE.search(urlsplit(url or '').path + '/')
    if not match:
        return None
    try:
        return datetime.strptime(match.group(1), '%Y%m%d').date()
    except ValueError:
        return None


def row_date(row):
    '''The date of an item: from its link, else the first date in its row text.'''
    return published_date(row.get('href')) or parse_display_date(row.get('text'))


def _keyword_pattern(keywords):
    words = [' '.join(k.split()) for k in keywords or [] if k and k.strip()]
    if not words:
        return None
    # (?<!\w) and (?!\w) instead of \b, so keywords ending in ')' still match
    return re.compile(r'(?<!\w)(?:' + '|'.join(re.escape(w) for w in words) + r')(?!\w)', re.IGNORECASE)


def title_keeps(title, category):
    '''Apply TITLE_RULES for the category. Categories without rules keep everything.'''
    rules = TITLE_RULES.get(category.get('category_id')) or {}
    title = ' '.join((title or '').split())
    include, exclude = _keyword_pattern(rules.get('include')), _keyword_pattern(rules.get('exclude'))
    if include and not include.search(title):
        return False
    if exclude and exclude.search(title):
        return False
    return True


def parse_table_row(row, category, base_url, scan_start, scan_end):
    '''Turn one listed item into an item dict, or None when it has no usable link or date,
    or falls outside the scan period.'''
    url = normalize_url(row.get('href') or '', base_url)
    if not url or urlsplit(url).netloc != urlsplit(base_url).netloc.lower():
        return None
    path = urlsplit(url).path
    if not re.match(item_path_pattern(category), path, re.IGNORECASE):
        return None
    item_date = row_date(row)
    if item_date is None or item_date < scan_start or item_date > scan_end:
        return None
    return {
        'url': url,
        'title': ' '.join((row.get('title') or '').split()) or url,
        'item_date': item_date,
        'item_date_label': DATE_LABEL,
        'item_reference': unquote(path.split('/')[2]) or None,
    }


# Scripts run inside Chromium. They receive the item path regex and no results text.

_ITEMS_READY_SCRIPT = r'''(cfg) => {
    const re = new RegExp(cfg.item_path, 'i');
    const scope = document.querySelector('main') || document.body;
    if (!scope) return false;
    for (const a of scope.querySelectorAll('a[href]')) {
        try { if (re.test(new URL(a.href).pathname)) return true; } catch (e) {}
    }
    return !!cfg.no_results && (scope.innerText || '').toLowerCase().includes(cfg.no_results.toLowerCase());
}'''

_ITEMS_SCRIPT = r'''(cfg) => {
    const clean = s => (s || '').replace(/\s+/g, ' ').trim();
    const re = new RegExp(cfg.item_path, 'i');
    const scope = document.querySelector('main') || document.body;
    const byPath = new Map();
    for (const a of scope.querySelectorAll('a[href]')) {
        let u;
        try { u = new URL(a.href); } catch (e) { continue; }
        if (!re.test(u.pathname)) continue;
        if (/viewtype=pdf/i.test(u.search) || /\.pdf$/i.test(u.pathname)) continue;   // file links, not item pages
        const row = a.closest('tr, li, article') || a.parentElement;
        const title = clean(a.textContent);
        const old = byPath.get(u.pathname);
        if (!old || title.length > old.title.length) {
            byPath.set(u.pathname, {href: a.href, title: title, text: clean(row ? row.innerText : '')});
        }
    }
    const next = [...document.querySelectorAll('a[href]')].find(a =>
        (a.getAttribute('rel') || '').toLowerCase() === 'next'
        || /^(next|next page|›|»|>)$/i.test(clean(a.textContent))
        || /next/i.test(a.getAttribute('aria-label') || '')
        || /next/i.test(a.getAttribute('title') || ''));
    const text = clean(scope.innerText).toLowerCase();
    return {rows: [...byPath.values()],
            next: next && !/^javascript:|#$/.test(next.getAttribute('href') || '') ? next.href : '',
            no_results: !!cfg.no_results && text.includes(cfg.no_results.toLowerCase())};
}'''


# =====================================================================
# SSO scraper
# =====================================================================

class SsoScraper(BaseScraper):

    @classmethod
    def validate_category(cls, category, base_host):
        problems = []
        for key in ('listing_type', 'listing_url', 'date_source'):
            if not category.get(key):
                problems.append(f'missing {key}')
        listing_type = category.get('listing_type')
        if listing_type and listing_type not in LISTING_TYPES:
            problems.append(f'listing_type must be one of {sorted(LISTING_TYPES)}')
        if category.get('date_source') and category['date_source'] not in DATE_SOURCES:
            problems.append(f'date_source must be one of {sorted(DATE_SOURCES)}')
        if category.get('filter_by_date_in_url', False):
            problems.append('filter_by_date_in_url must be false (the SSO supplements have no date filter in the URL)')
        if category.get('expected_card_types'):
            problems.append('expected_card_types must be [] (table rows have no type label)')
        if category.get('listing_url') and urlsplit(category['listing_url']).netloc.lower() != base_host:
            problems.append(f'listing_url must be on {base_host}')
        return problems

    # Reading listing pages

    async def _read_listing_page(self, page, url, category):
        '''Open one supplement page and return {rows, next, no_results}.'''
        args = {'item_path': item_path_pattern(category), 'no_results': NO_RESULTS_TEXT}

        async def action():
            await self._open(page, url)
            try:
                await page.wait_for_function(_ITEMS_READY_SCRIPT, arg=args, timeout=self.render_timeout)
            except PlaywrightTimeoutError:
                raise RetryableError(f'Listing did not show any item link in time (bot challenge not '
                                     f'passed, or the page layout changed): {url}')
            await self._settle(page)
            return await page.evaluate(_ITEMS_SCRIPT, args)
        return await self._with_retry(action, f'read listing {url}')

    async def _collect_rows(self, page, category, scan_start):
        '''All items of a supplement, following the Next link while pages add new items and
        have not yet reached dates before the scan start.
        Returns (rows, listing_empty, may_be_incomplete).'''
        max_pages = int(self.config.get('max_pages_per_category', 20))
        rows, seen_paths, visited = [], set(), set()
        url = category['listing_url']
        for _ in range(max_pages):
            visited.add(url)
            result = await self._read_listing_page(page, url, category)
            if result.get('no_results') and not rows and not result['rows']:
                return [], True, False
            new_rows = [r for r in result['rows'] if urlsplit(r['href']).path not in seen_paths]
            if not new_rows:
                # No items: listing empty or past the last page (complete).
                # Only repeated items: the Next link did not move on (may be incomplete).
                return rows, not rows, bool(rows) and bool(result['rows'])
            for row in new_rows:
                seen_paths.add(urlsplit(row['href']).path)
            rows += new_rows
            dates = [d for d in (row_date(r) for r in new_rows) if d]
            if dates and max(dates) < scan_start:
                return rows, False, False      # whole page older than the scan: later pages are older
            next_url = result.get('next') or ''
            if not next_url or next_url in visited:
                return rows, False, False      # last page
            url = next_url
        raise PermanentError(f'More than {max_pages} listing pages, raise max_pages_per_category',
                             'PAGINATION_LIMIT')

    async def discover(self, page, category, scan_start, scan_end):
        warning = None
        rows, empty, partial = await self._collect_rows(page, category, scan_start)
        if empty:
            warning = LISTING_EMPTY_WARNING
        elif partial:
            warning = LISTING_PARTIAL_WARNING
        items = [parse_table_row(r, category, self.base_url, scan_start, scan_end) for r in rows]

        unique, seen, filtered_out = [], set(), 0
        for item in items:
            if not item or item['url'] in seen:
                continue
            if not title_keeps(item['title'], category):
                filtered_out += 1
                continue
            seen.add(item['url'])
            unique.append(item)
        self.logger.info('[%s] %s items in scan period%s%s', category['category_id'], len(unique),
                         f' ({filtered_out} dropped by title rules)' if filtered_out else '',
                         ' (listing empty, recorded as 0)' if warning == LISTING_EMPTY_WARNING else '')
        return unique, warning
