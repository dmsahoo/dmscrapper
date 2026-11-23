'''
MOM discovery: which MOM (www.mom.gov.sg) pages fall inside the scan period for a category.

Built on scrapers/mas_scraper.py (the master scraper) and scrapers/reach_scraper.py: same
structure, same category parameters as mas_config.yaml. Everything after discovery (item
pages, downloads, decisions, storage, the run loop) is shared and lives in
scrapers/base_scraper.py.

How MOM items are found (follows the intake tracker)
  1_WORK_PASS_POLICY             section_pages  /passes-and-permits/
  2_EMPLOYMENT_LEGISLATION       section_pages  /employment-practices/employment-act
  3_NEWSROOM                     cards          /newsroom/press-releases
  4_WORKPLACE_SAFETY_AND_HEALTH  section_pages  /workplace-safety-and-health
  5_OPERATIONAL_ESERVICES        section_pages  /eservices

  cards (listing_date)
    The Newsroom listing is server rendered. Each result is an <article> in
    section.item-listing with the title link (.item-title a), a <time datetime="2026-11-2">
    and its MOM categories (.article-meta a). Pages are read with page=1, 2, ... newest first,
    until a page adds no new cards or reaches dates before the scan start.

  section_pages (last_updated)
    A topic section. The items are the Entry URL page itself plus every page it links to under
    the same path (for example /passes-and-permits/s-pass). Links to other hosts, links with a
    query (Filter by / page links) and fragments are ignored. When the Entry URL page has a
    page=2 link (eServices), the next pages are read too. Each candidate page is opened and its
    'Last Updated: 3 November 2026' line (span.mom-last-updated) gives the item date. A page is
    an item when that date is inside the scan period. Pages without the line are skipped and
    counted in the category warning.

Zero items
  Nothing in the period is recorded as 0 items. A listing that shows no cards or links at all
  is also recorded as 0 items, with a warning to check the Entry URL.

Safety checks that fail a category
  1. A card has no label in expected_card_types (only when the list is not empty).
  2. More pages than max_pages_per_category, or more section links than MAX_SECTION_LINKS.
'''

import re
from datetime import date, datetime
from urllib.parse import parse_qsl, quote, urlencode, urljoin, urlsplit, urlunsplit

from scrapers.base_scraper import BaseScraper, PermanentError, RetryableError
from scrapers.tracking import normalize_url

LISTING_TYPES = {'cards', 'section_pages'}
DATE_SOURCES = {'listing_date', 'last_updated'}
DATE_SOURCE_FOR_TYPE = {'cards': 'listing_date', 'section_pages': 'last_updated'}

# ---- MOM page details (checked on the 5 Entry URLs, 2026-11-13) ---------------------------
CARD_SELECTOR = 'section.item-listing article'   # one Newsroom result
CARD_LINK_SELECTOR = '.item-title a[href]'      # title link inside a card
CARD_LABEL_SELECTOR = '.article-meta a'          # MOM categories inside a card
LAST_UPDATED_SELECTOR = '.mom-last-updated'      # 'Last Updated: 3 November 2026'
PAGE_PARAM = 'page'                              # page number parameter
MAX_SECTION_LINKS = 150                          # safety limit on candidate pages per section
CARD_DATE_LABEL = 'Published Date'
SECTION_DATE_LABEL = 'Last Updated'
SKIP_EXTENSIONS = ('.pdf', '.doc', '.docx', '.xls', '.xlsx', '.ppt', '.pptx', '.zip', '.jpg', '.png')
# -------------------------------------------------------------------------------------------

LISTING_EMPTY_WARNING = ('Listing showed no results at all. Recorded as 0 items. '
                         'Check that the Entry URL still works.')
LISTING_PARTIAL_WARNING = ('Listing stopped adding results before reaching the scan start '
                           '(the page number may be ignored). Items may be missing.')

_DATE_RE = re.compile(r'(\d{1,2}) ([A-Za-z]+) (\d{4})')
_ISO_RE = re.compile(r'^(\d{4})-(\d{1,2})-(\d{1,2})')


# =====================================================================
# Pure helpers (unit tested without a browser)
# =====================================================================

def parse_display_date(text):
    '''First date written like 3 November 2026 (or 3 Nov 2026) in the text, or None.'''
    match = _DATE_RE.search(text or '')
    if not match:
        return None
    for fmt in ('%d %B %Y', '%d %b %Y'):
        try:
            return datetime.strptime(' '.join(match.groups()), fmt).date()
        except ValueError:
            continue
    return None


def parse_iso_date(text):
    '''A datetime attribute like 2026-11-2 (MOM does not zero pad), or None.'''
    match = _ISO_RE.match((text or '').strip())
    if not match:
        return None
    try:
        return date(*(int(g) for g in match.groups()))
    except ValueError:
        return None


def build_listing_url(category, page_number=1):
    '''The category Entry URL with PAGE_PARAM set to page_number. Page 1 is the Entry URL as
    given (MOM page 1 has no page parameter).'''
    parts = urlsplit(category['listing_url'])
    query = [(k, v) for k, v in parse_qsl(parts.query, keep_blank_values=True) if k != PAGE_PARAM]
    if page_number > 1:
        query.append((PAGE_PARAM, str(page_number)))
    return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(query, quote_via=quote), ''))


def section_path(listing_url):
    '''The path of the Entry URL without a trailing slash, lower case.'''
    return urlsplit(listing_url or '').path.rstrip('/').lower()


def section_candidates(hrefs, listing_url, base_url):
    '''Page URLs (no query, no fragment) on the base host whose path is under the Entry URL
    path, in first seen order. The Entry URL page itself is not included.'''
    host = urlsplit(base_url).netloc.lower()
    root = section_path(listing_url)
    kept, seen = [], set()
    for href in hrefs:
        url = normalize_url(href or '', base_url)
        parts = urlsplit(url or '')
        if parts.netloc.lower() != host or parts.query:
            continue
        path = parts.path.rstrip('/')
        if not path.lower().startswith(root + '/') or path.lower().endswith(SKIP_EXTENSIONS):
            continue
        clean = urlunsplit((parts.scheme, parts.netloc, path, '', ''))
        if clean.lower() not in seen:
            seen.add(clean.lower())
            kept.append(clean)
    return kept


def has_next_page(hrefs, listing_url, page_number, base_url):
    '''True when the page links to page_number + 1 of the same path. (normalize_url drops the
    query, so the link is resolved with urljoin here.)'''
    root = section_path(listing_url)
    wanted = str(page_number + 1)
    for href in hrefs:
        parts = urlsplit(urljoin(base_url, (href or '').strip()))
        if parts.path.rstrip('/').lower() == root and dict(parse_qsl(parts.query)).get(PAGE_PARAM) == wanted:
            return True
    return False


def card_date(card):
    '''The date of a Newsroom card: the datetime attribute, else the shown date.'''
    return parse_iso_date(card.get('date_attr')) or parse_display_date(card.get('date_text'))


def parse_card(card, category, base_url, scan_start, scan_end):
    '''Turn one Newsroom card into an item dict, or None when it has no usable link or date or
    falls outside the scan period. Raises PermanentError when none of the card labels is one
    the category expects (only when expected_card_types is not empty).'''
    labels = [l for l in (card.get('labels') or []) if l]
    expected = category.get('expected_card_types') or []
    if expected and not any(l in expected for l in labels):
        raise PermanentError(f'Card labels {labels} are not in {expected}', 'LISTING_FILTER_IGNORED')

    url = normalize_url(card.get('href') or '', base_url)
    if not url or urlsplit(url).netloc.lower() != urlsplit(base_url).netloc.lower():
        return None
    item_date = card_date(card)
    if item_date is None or item_date < scan_start or item_date > scan_end:
        return None
    return {
        'url': url,
        'title': (card.get('title') or '').strip() or url,
        'item_date': item_date,
        'item_date_label': CARD_DATE_LABEL,
        'item_reference': '; '.join(labels) or None,
    }


def parse_section_page(info, url, scan_start, scan_end):
    '''Turn one opened section page into an item dict, or None when it has no Last Updated date
    or falls outside the scan period.'''
    item_date = parse_display_date(info.get('last_updated'))
    if item_date is None or item_date < scan_start or item_date > scan_end:
        return None
    return {
        'url': url,
        'title': (info.get('title') or '').strip() or url,
        'item_date': item_date,
        'item_date_label': SECTION_DATE_LABEL,
        'item_reference': urlsplit(url).path.rstrip('/') or None,
    }


# Scripts run inside the browser. MOM pages are server rendered, so no polling is needed.

_CARDS_SCRIPT = r'''(cfg) => {
    const clean = s => (s || '').replace(/\s+/g, ' ').trim();
    return [...document.querySelectorAll(cfg.card)].map(c => {
        const a = c.querySelector(cfg.link);
        const t = c.querySelector('time');
        return {href: a ? a.href : '',
                title: a ? clean(a.textContent) : '',
                date_attr: t ? (t.getAttribute('datetime') || '') : '',
                date_text: t ? clean(t.textContent) : '',
                labels: [...c.querySelectorAll(cfg.label)].map(x => clean(x.textContent))};
    });
}'''

_PAGE_INFO_SCRIPT = r'''(cfg) => {
    const clean = s => (s || '').replace(/\s+/g, ' ').trim();
    const lu = document.querySelector(cfg.last_updated);
    const h1 = document.querySelector('h1');
    return {hrefs: [...document.querySelectorAll('a[href]')].map(a => a.href),
            last_updated: lu ? clean(lu.textContent) : '',
            title: h1 ? clean(h1.textContent) : clean(document.title)};
}'''

_SCRIPT_ARGS = {
    'card': CARD_SELECTOR,
    'link': CARD_LINK_SELECTOR,
    'label': CARD_LABEL_SELECTOR,
    'last_updated': LAST_UPDATED_SELECTOR,
}


# =====================================================================
# MOM scraper
# =====================================================================

class MomScraper(BaseScraper):

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
        if listing_type in DATE_SOURCE_FOR_TYPE and category.get('date_source') \
                and category['date_source'] != DATE_SOURCE_FOR_TYPE[listing_type]:
            problems.append(f'listing_type {listing_type} needs date_source {DATE_SOURCE_FOR_TYPE[listing_type]}')
        if category.get('filter_by_date_in_url', False):
            problems.append('filter_by_date_in_url must be false (MOM has no date range filter in the URL)')
        if listing_type == 'section_pages' and category.get('expected_card_types'):
            problems.append('expected_card_types must be empty for section_pages (no cards)')
        if category.get('listing_url') and urlsplit(category['listing_url']).netloc.lower() != base_host:
            problems.append(f'listing_url must be on {base_host}')
        return problems

    # Reading pages

    async def _evaluate(self, page, url, script):
        '''Open url and run a script on it, with retries.'''
        async def action():
            await self._open(page, url)
            await self._settle(page)
            return await page.evaluate(script, _SCRIPT_ARGS)
        return await self._with_retry(action, f'read {url}')

    # cards

    async def _discover_cards(self, page, category, scan_start, scan_end):
        max_pages = int(self.config.get('max_pages_per_category', 20))
        cards, seen = [], set()
        for page_number in range(1, max_pages + 1):
            result = await self._evaluate(page, build_listing_url(category, page_number), _CARDS_SCRIPT)
            new_cards = [c for c in result if c.get('href') and c['href'] not in seen]
            if not new_cards:
                warning = None
                if not cards:
                    warning = LISTING_EMPTY_WARNING
                elif result:
                    warning = LISTING_PARTIAL_WARNING   # only repeated cards: page number ignored
                return cards, warning
            for card in new_cards:
                seen.add(card['href'])
            cards += new_cards
            dates = [d for d in (card_date(c) for c in new_cards) if d]
            if dates and min(dates) < scan_start:
                return cards, None                      # newest first: later pages are older
        raise PermanentError(f'More than {max_pages} listing pages, raise max_pages_per_category',
                             'PAGINATION_LIMIT')

    # section_pages

    async def _discover_section(self, page, category, scan_start, scan_end):
        max_pages = int(self.config.get('max_pages_per_category', 20))
        listing_url = category['listing_url']
        entry_url = build_listing_url(category, 1)
        entry_info = None
        candidates, seen = [], set()
        for page_number in range(1, max_pages + 1):
            info = await self._evaluate(page, build_listing_url(category, page_number), _PAGE_INFO_SCRIPT)
            if entry_info is None:
                entry_info = info
            for url in section_candidates(info['hrefs'], listing_url, self.base_url):
                if url.lower() not in seen:
                    seen.add(url.lower())
                    candidates.append(url)
            if not has_next_page(info['hrefs'], listing_url, page_number, self.base_url):
                break
        else:
            raise PermanentError(f'More than {max_pages} listing pages, raise max_pages_per_category',
                                 'PAGINATION_LIMIT')
        if len(candidates) > MAX_SECTION_LINKS:
            raise PermanentError(f'{len(candidates)} section links, more than MAX_SECTION_LINKS '
                                 f'({MAX_SECTION_LINKS}). Check the Entry URL.', 'PAGINATION_LIMIT')

        items, undated = [], 0
        entry_item = parse_section_page(entry_info, entry_url, scan_start, scan_end)
        if entry_item:
            items.append(entry_item)
        if not parse_display_date(entry_info.get('last_updated')):
            undated += 1
        for url in candidates:
            try:
                info = await self._evaluate(page, url, _PAGE_INFO_SCRIPT)
            except (RetryableError, PermanentError) as error:
                self.logger.warning('[%s] Could not read the date of %s: %s',
                                    category['category_id'], url, error)
                undated += 1
                continue
            if not parse_display_date(info.get('last_updated')):
                undated += 1
                continue
            item = parse_section_page(info, url, scan_start, scan_end)
            if item:
                items.append(item)

        warning = None
        if not candidates and not entry_info.get('last_updated'):
            warning = LISTING_EMPTY_WARNING
        elif undated:
            warning = (f'{undated} of {len(candidates) + 1} section pages had no readable '
                       f'Last Updated date and were skipped.')
        self.logger.info('[%s] %s section pages checked', category['category_id'], len(candidates) + 1)
        return items, warning

    async def discover(self, page, category, scan_start, scan_end):
        if category['listing_type'] == 'cards':
            cards, warning = await self._discover_cards(page, category, scan_start, scan_end)
            items = [parse_card(c, category, self.base_url, scan_start, scan_end) for c in cards]
        else:
            items, warning = await self._discover_section(page, category, scan_start, scan_end)

        unique, seen = [], set()
        for item in items:
            if item and item['url'] not in seen:
                seen.add(item['url'])
                unique.append(item)
        self.logger.info('[%s] %s items in scan period%s', category['category_id'], len(unique),
                         ' (listing empty, recorded as 0)' if warning == LISTING_EMPTY_WARNING else '')
        return unique, warning
