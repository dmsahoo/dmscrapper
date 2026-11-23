'''
REACH discovery: which REACH public consultations fall inside the scan period for a category.

Built on scrapers/mas_scraper.py (the master scraper) and scrapers/mha_scraper.py: same
structure, same category parameters as mas_config.yaml. Everything after discovery (item
pages, downloads, decisions, storage, the run loop) is shared and lives in
scrapers/base_scraper.py.

How REACH items are found (follows the intake tracker)
  All three categories use the Public consultation search
  (Homepage > Latest happenings > Public consultation > search / filter):
    1_FINANCIAL_SECTOR_CONSULTATIONS         Category filter: ACRA, MOF and joint MOF agencies
    2_CROSS_CUTTING_CONSULTATIONS_FS_IMPACT  PLACEHOLDER (tracker: Unclear)
    3_UPCOMING_AND_ONGOING_CONSULTATIONS     PLACEHOLDER
  The category Entry URL (listing_url) is opened with page=1, 2, ... Each result card gives
  the date, title, status, agency and item link. REACH runs on the same platform (Isomer) as
  the MHA Newsroom, so the card layout is the same.

Filters in the Entry URL
  The filters parameter is a JSON list such as
    [{"id": "Category", "items": [{"id": "Ministry of Finance"}, ...]}]
  Filter ids on the site: Status, Category, year. The page first shows unfiltered results and
  applies the filters in the browser a moment later. The scraper therefore waits until every
  card matches the filters (Category = card agency, Status = card status, year = card date)
  before it reads the page, and drops any card that still does not match.
  There is no date range filter, so filter_by_date_in_url must be false and the scan period
  is applied from the card date. Pages are read until a page adds no new cards or (newest
  first) reaches dates before the scan start.

Zero items
  Nothing in the period is recorded as 0 items. A listing that shows no results at all is
  also recorded as 0 items, with a warning to check the Entry URL.

Safety checks that fail a category
  1. The page did not render cards or the no results text in time.
  2. The cards did not match the Entry URL filters in time (filters not applied).
  3. A card has a label (agency) not in expected_card_types (only when the list is not empty).
  4. More pages than max_pages_per_category.
'''

import asyncio
import json
import re
import time
from datetime import datetime
from urllib.parse import parse_qsl, quote, urlencode, urlsplit, urlunsplit

from playwright.async_api import TimeoutError as PlaywrightTimeoutError

from scrapers.base_scraper import BaseScraper, PermanentError, RetryableError
from scrapers.tracking import normalize_url

LISTING_TYPES = {'cards'}
DATE_SOURCES = {'listing_date'}

# ---- PLACEHOLDERS: REACH page details, confirm in a browser (DevTools) -------------------
# Pre-filled from the REACH Public consultation HTML. Each result is an <a> holding a date
# <p>, an <h2> title, a Status pill and the agency <span>.
CARD_SELECTOR = 'a.border-collapse[href*="/public-consultations/"]'   # PLACEHOLDER: one result card
TITLE_SELECTOR = 'h2 span.line-clamp-3'          # PLACEHOLDER: title inside a card
DATE_SELECTOR = 'p'                              # PLACEHOLDER: date inside a card (e.g. 27 July 2026)
TYPE_SELECTOR = 'div.prose-label-md span'        # PLACEHOLDER: agency inside a card
STATUS_SELECTOR = 'div.rounded-full p'           # PLACEHOLDER: status inside a card (Open, Closed, ...)
NO_RESULTS_TEXT = 'No results'                   # PLACEHOLDER: text shown when a search finds nothing
PAGE_PARAM = 'page'                              # PLACEHOLDER: page number parameter in the Entry URL
FILTERS_PARAM = 'filters'                        # JSON filter list parameter in the Entry URL
DATE_LABEL = 'Published Date'                    # PLACEHOLDER: name stored for the card date
# -------------------------------------------------------------------------------------------

LISTING_EMPTY_WARNING = ('Listing showed no results at all. Recorded as 0 items. '
                         'Check that the Entry URL still works.')
LISTING_PARTIAL_WARNING = ('Listing stopped adding results before reaching the scan start '
                           '(the page number may be ignored). Items may be missing: check PAGE_PARAM.')

_DATE_RE = re.compile(r'(\d{1,2}) ([A-Za-z]+) (\d{4})')


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


def build_listing_url(category, page_number=1):
    '''The category Entry URL with PAGE_PARAM set to page_number (any page number already in
    the Entry URL is replaced).'''
    parts = urlsplit(category['listing_url'])
    query = [(k, v) for k, v in parse_qsl(parts.query, keep_blank_values=True) if k != PAGE_PARAM]
    query.append((PAGE_PARAM, str(page_number)))
    return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(query, quote_via=quote), ''))


def url_filters(listing_url):
    '''The filters in the Entry URL as {filter id (lower case): {item ids (lower case)}}.
    Raises ValueError when the filters parameter is not valid JSON in the expected shape.'''
    raw = dict(parse_qsl(urlsplit(listing_url or '').query, keep_blank_values=True)).get(FILTERS_PARAM)
    if not raw:
        return {}
    data = json.loads(raw)
    if not isinstance(data, list):
        raise ValueError('filters must be a JSON list')
    result = {}
    for entry in data:
        if not isinstance(entry, dict) or not entry.get('id') or not isinstance(entry.get('items'), list):
            raise ValueError('each filter needs an id and a list of items')
        items = {' '.join(str(i.get('id', '')).split()).lower() for i in entry['items'] if isinstance(i, dict)}
        items.discard('')
        if items:
            result[str(entry['id']).strip().lower()] = items
    return result


def card_date(card):
    '''The date shown on a card: from the date element, else anywhere in the card text.'''
    return parse_display_date(card.get('date_text')) or parse_display_date(card.get('text'))


def card_matches_filters(card, filters):
    '''True when the card agrees with every filter of the Entry URL. Filter ids the scraper
    does not know are ignored.'''
    def norm(value):
        return ' '.join((value or '').split()).lower()
    for filter_id, items in filters.items():
        if filter_id == 'category' and norm(card.get('type')) not in items:
            return False
        if filter_id == 'status' and norm(card.get('status')) not in items:
            return False
        if filter_id == 'year':
            item_date = card_date(card)
            if item_date is not None and str(item_date.year) not in items:
                return False
    return True


def parse_card(card, category, base_url, scan_start, scan_end, filters=None):
    '''Turn one listing card into an item dict, or None when it has no usable link or date,
    does not match the Entry URL filters, or falls outside the scan period. Raises
    PermanentError when the card label is not one the category expects (only when
    expected_card_types is not empty).'''
    expected = category.get('expected_card_types') or []
    card_type = (card.get('type') or '').strip()
    if expected and card_type not in expected:
        raise PermanentError(f'Card label {card_type!r} is not in {expected}', 'LISTING_FILTER_IGNORED')

    if filters and not card_matches_filters(card, filters):
        return None

    url = normalize_url(card.get('href') or '', base_url)
    if not url or urlsplit(url).netloc != urlsplit(base_url).netloc.lower():
        return None

    item_date = card_date(card)
    if item_date is None or item_date < scan_start or item_date > scan_end:
        return None

    return {
        'url': url,
        'title': (card.get('title') or '').strip() or url,
        'item_date': item_date,
        'item_date_label': DATE_LABEL,
        'item_reference': card_type or None,
    }


# Scripts run inside Chromium. They receive the REACH selectors as one argument object.

_CARDS_READY_SCRIPT = r'''(cfg) => {
    if (document.querySelectorAll(cfg.card).length > 0) return true;
    const scope = document.querySelector('main') || document.body;
    return !!cfg.no_results && (scope.innerText || '').toLowerCase().includes(cfg.no_results.toLowerCase());
}'''

_CARDS_SCRIPT = r'''(cfg) => {
    const clean = s => (s || '').replace(/\s+/g, ' ').trim();
    const scope = document.querySelector('main') || document.body;
    const cards = [...document.querySelectorAll(cfg.card)].map(c => {
        const a = c.matches('a[href]') ? c : c.querySelector('a[href]');
        const t = cfg.title ? c.querySelector(cfg.title) : null;
        const d = cfg.date ? c.querySelector(cfg.date) : null;
        const k = cfg.type ? c.querySelector(cfg.type) : null;
        const s = cfg.status ? c.querySelector(cfg.status) : null;
        return {type: k ? clean(k.textContent) : '',
                status: s ? clean(s.textContent) : '',
                title: t ? clean(t.getAttribute('title') || t.textContent) : (a ? clean(a.textContent) : ''),
                date_text: d ? clean(d.textContent) : '',
                href: a ? a.href : '',
                text: clean(c.innerText)};
    });
    const text = clean(scope.innerText).toLowerCase();
    return {cards: cards,
            no_results: !!cfg.no_results && text.includes(cfg.no_results.toLowerCase())};
}'''

_SCRIPT_ARGS = {
    'card': CARD_SELECTOR,
    'title': TITLE_SELECTOR,
    'date': DATE_SELECTOR,
    'type': TYPE_SELECTOR,
    'status': STATUS_SELECTOR,
    'no_results': NO_RESULTS_TEXT,
}


# =====================================================================
# REACH scraper
# =====================================================================

class ReachScraper(BaseScraper):

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
            problems.append('filter_by_date_in_url must be false (the REACH search has no date range filter in the URL)')
        if category.get('listing_url'):
            if urlsplit(category['listing_url']).netloc.lower() != base_host:
                problems.append(f'listing_url must be on {base_host}')
            try:
                url_filters(category['listing_url'])
            except ValueError as exc:
                problems.append(f'listing_url {FILTERS_PARAM} parameter is not valid: {exc}')
        return problems

    # Reading listing pages

    async def _read_cards_page(self, page, url, filters):
        '''Open one card listing page and return {cards, no_results}, once the cards match the
        Entry URL filters.'''
        async def action():
            await self._open(page, url)
            try:
                await page.wait_for_function(_CARDS_READY_SCRIPT, arg=_SCRIPT_ARGS,
                                             timeout=self.render_timeout)
            except PlaywrightTimeoutError:
                raise RetryableError(f'Listing did not show cards or the no results text in time: {url}')
            await self._settle(page)
            # The page shows unfiltered results first and applies the filters a moment later
            deadline = time.monotonic() + self.render_timeout / 1000
            while True:
                result = await page.evaluate(_CARDS_SCRIPT, _SCRIPT_ARGS)
                if all(card_matches_filters(c, filters) for c in result['cards']):
                    return result
                if time.monotonic() > deadline:
                    raise RetryableError(f'Listing still shows results outside the Entry URL filters '
                                         f'(filters not applied in time): {url}')
                await asyncio.sleep(1)
        return await self._with_retry(action, f'read listing {url}')

    async def _collect_cards(self, page, category, scan_start, filters):
        '''All cards of a listing, following pages while they add new cards and have not yet
        reached dates before the scan start. Returns (cards, listing_empty, may_be_incomplete).'''
        max_pages = int(self.config.get('max_pages_per_category', 20))
        cards, seen_hrefs = [], set()
        for page_number in range(1, max_pages + 1):
            url = build_listing_url(category, page_number)
            result = await self._read_cards_page(page, url, filters)
            if result.get('no_results') and not cards:
                return [], True, False
            new_cards = [c for c in result['cards'] if c.get('href') and c['href'] not in seen_hrefs]
            if not new_cards:
                # No cards: page 1 empty or past the last page (complete).
                # Only repeated cards: the site ignored the page number (may be incomplete).
                return cards, not cards, bool(cards) and bool(result['cards'])
            for card in new_cards:
                seen_hrefs.add(card['href'])
            cards += new_cards
            dates = [d for d in (card_date(c) for c in new_cards) if d]
            if dates and min(dates) < scan_start:
                return cards, False, False     # newest first: later pages are older
        raise PermanentError(f'More than {max_pages} listing pages, raise max_pages_per_category',
                             'PAGINATION_LIMIT')

    async def discover(self, page, category, scan_start, scan_end):
        warning = None
        filters = url_filters(category['listing_url'])
        cards, empty, partial = await self._collect_cards(page, category, scan_start, filters)
        if empty:
            warning = LISTING_EMPTY_WARNING
        elif partial:
            warning = LISTING_PARTIAL_WARNING
        items = [parse_card(c, category, self.base_url, scan_start, scan_end, filters) for c in cards]

        unique, seen = [], set()
        for item in items:
            if item and item['url'] not in seen:
                seen.add(item['url'])
                unique.append(item)
        self.logger.info('[%s] %s items in scan period%s', category['category_id'], len(unique),
                         ' (listing empty, recorded as 0)' if warning == LISTING_EMPTY_WARNING else '')
        return unique, warning
