'''
ACRA discovery: which ACRA (www.acra.gov.sg) items fall inside the scan period for a category.

Built on scrapers/mas_scraper.py (the master scraper), scrapers/mom_scraper.py (section_pages)
and scrapers/reach_scraper.py (Isomer cards): same structure, same category parameters as
mas_config.yaml. Everything after discovery (downloads, decisions, storage, the run loop) is
shared and lives in scrapers/base_scraper.py.

How ACRA items are found (follows the intake tracker)
  1A_PRACTICE_DIRECTIONS         section_pages  /regulations/practice-directions-.../practice-directions/
  1B_REGISTRARS_INTERPRETATIONS  section_pages  /regulations/practice-directions-.../registrars-interpretations/
  1C_AUDIT_PRACTICE_BULLETINS    section_pages  /regulations/practice-directions-.../audit-practice-bulletins/
  2_NOTICES_AND_CIRCULARS        section_pages  /manage/companies/overview/ (links under /manage/companies/)
  3_LEGISLATION_UPDATES          section_pages  /regulations/legislation/
  4_POLICY_AND_CONSULTATIONS     cards          /public-consultations/ (Category filter)
  5_NEWS_ANNOUNCEMENTS           cards          /news-events/news-announcements/ (Category filter)

  section_pages (last_updated)
    The Entry URL page itself plus every page it links to under the section root (the Entry
    URL path, or SECTION_ROOTS for the category). Links in nav, header and footer, links with
    a query and links to other hosts are ignored. Each page is opened and its 'Last updated on
    15 September 2026' line gives the item date. A page is an item when that date is inside
    the scan period. Pages without the line are skipped and counted in the category warning.
    The accordions on the Practice Directions pages are <details> blocks: their links are in
    the page already, so nothing has to be expanded.

  cards (listing_date)
    Isomer result cards (same layout as REACH): each card is an <a> with a date <p>, an <h2>
    title and label groups (Audience, News Topic, Category, Status, Topic). The page and the
    filters parameter of the Entry URL are applied in the browser a moment after the page
    loads (the server always sends page 1 unfiltered). The scraper polls until the pagination
    shows the asked page and every card matches the filters, then reads the cards. Pages are
    read page=1, 2, ... until a page reaches dates before the scan start, the Next button is
    disabled, or a page adds no new cards.

PDFs
  ACRA PDFs are stored on isomer-user-content.by.gov.sg. The shared PDF filter only keeps
  links on base_url, so _process_item here uses the same flow as base_scraper with the PDF
  hosts in PDF_HOSTS. Keyword filters work as for every regulator.

Zero items
  Nothing in the period is recorded as 0 items. A listing that shows no cards or links at all
  is also recorded as 0 items, with a warning to check the Entry URL.

Safety checks that fail a category
  1. The listing did not show cards (or the no results text) for the asked page and filters
     in time.
  2. More pages than max_pages_per_category, or more section links than MAX_SECTION_LINKS.
'''

import asyncio
import json
import re
import time
from dataclasses import asdict
from datetime import datetime
from urllib.parse import parse_qsl, quote, quote_plus, urlencode, urlsplit, urlunsplit

from playwright.async_api import Error as PlaywrightError

from scrapers.base_scraper import BaseScraper, Counters, PermanentError, RetryableError
from scrapers.tracking import FAILED, PARTIAL_SUCCESS, SUCCESS, normalize_url

LISTING_TYPES = {'cards', 'section_pages'}
DATE_SOURCES = {'listing_date', 'last_updated'}
DATE_SOURCE_FOR_TYPE = {'cards': 'listing_date', 'section_pages': 'last_updated'}

# ---- ACRA page details (checked on the 7 Entry URLs with plain requests, 2026-11-17) -------
CARD_SELECTOR = 'main a.border-collapse[href]'   # one Isomer result card
TITLE_SELECTOR = 'h2 span.line-clamp-3'          # title inside a card (full title in title=)
DATE_SELECTOR = 'p'                              # first <p> in a card: 11 November 2026
GROUP_SELECTOR = 'p.prose-label-sm'              # label group name: Audience, Category, Status, ...
GROUP_VALUE_SELECTOR = 'p.prose-label-sm-medium' # label values in that group
FOOTER_LABEL_SELECTOR = 'div.prose-label-md span'   # consultations: Category at the card bottom
CURRENT_PAGE_SELECTOR = 'nav[aria-label=pagination] [aria-current=page]'
NEXT_PAGE_SELECTOR = 'nav[aria-label=pagination] button[aria-label="Go to next page"]'
NO_RESULTS_TEXT = 'No results'                   # PLACEHOLDER: text shown when a filter finds nothing
LAST_UPDATED_PREFIX = 'Last updated on'          # 'Last updated on 15 September 2026'
PAGE_PARAM = 'page'
FILTERS_PARAM = 'filters'
SITE_HOSTS = {'www.acra.gov.sg', 'acra.gov.sg'}  # some ACRA links leave out www
PDF_HOSTS = {'www.acra.gov.sg', 'isomer-user-content.by.gov.sg'}
# Section root per category when it is not the Entry URL path
SECTION_ROOTS = {'2_NOTICES_AND_CIRCULARS': '/manage/companies'}
MAX_SECTION_LINKS = 150                          # safety limit on candidate pages per section
CARD_DATE_LABEL = 'Published Date'
SECTION_DATE_LABEL = 'Last Updated'
SKIP_EXTENSIONS = ('.pdf', '.doc', '.docx', '.xls', '.xlsx', '.ppt', '.pptx', '.zip', '.jpg', '.png')
POLL_SECONDS = 1.0
# -------------------------------------------------------------------------------------------

LISTING_EMPTY_WARNING = ('Listing showed no results at all. Recorded as 0 items. '
                         'Check that the Entry URL still works.')
LISTING_PARTIAL_WARNING = ('Listing stopped adding results before reaching the scan start '
                           '(the page number may be ignored). Items may be missing.')

_DATE_RE = re.compile(r'(\d{1,2}) ([A-Za-z]+) (\d{4})')


# =====================================================================
# Pure helpers (unit tested without a browser)
# =====================================================================

def parse_display_date(text):
    '''First date written like 15 September 2026 (or 15 Sep 2026) in the text, or None.'''
    for match in _DATE_RE.finditer(text or ''):
        for fmt in ('%d %B %Y', '%d %b %Y'):
            try:
                return datetime.strptime(' '.join(match.groups()), fmt).date()
            except ValueError:
                continue
    return None


def build_listing_url(category, page_number=1):
    '''The category Entry URL with PAGE_PARAM set to page_number. An Entry URL without a page
    parameter is returned unchanged for page 1.'''
    parts = urlsplit(category['listing_url'])
    query = parse_qsl(parts.query, keep_blank_values=True)
    if page_number == 1 and PAGE_PARAM not in dict(query):
        return category['listing_url']
    query = [(k, v) for k, v in query if k != PAGE_PARAM] + [(PAGE_PARAM, str(page_number))]
    # quote_plus keeps the site's own form: filters=%5B%7B%22id%22%3A%22News+Topic%22...
    return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(query, quote_via=quote_plus), ''))


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
        items = {_norm(i.get('id')) for i in entry['items'] if isinstance(i, dict)}
        items.discard('')
        if items:
            result[_norm(entry['id'])] = items
    return result


def _norm(value):
    return ' '.join(str(value or '').split()).lower()


def card_date(card):
    '''The date shown on a card: from the date element, else anywhere in the card text.'''
    return parse_display_date(card.get('date_text')) or parse_display_date(card.get('text'))


def card_matches_filters(card, filters):
    '''True when the card agrees with every filter of the Entry URL: for each filter id the card
    has a label in that group with one of the filter values (Year: the card date year).'''
    groups = {_norm(k): {_norm(v) for v in vs} for k, vs in (card.get('groups') or {}).items()}
    for filter_id, items in filters.items():
        if filter_id == 'year':
            item_date = card_date(card)
            if item_date is None or str(item_date.year) not in items:
                return False
        elif not groups.get(filter_id, set()) & items:
            return False
    return True


def parse_card(card, category, base_url, scan_start, scan_end, filters=None):
    '''Turn one listing card into an item dict, or None when it has no usable link or date,
    does not match the Entry URL filters, or falls outside the scan period. Raises
    PermanentError when no card label is one the category expects (only when
    expected_card_types is not empty).'''
    labels = [v for vs in (card.get('groups') or {}).values() for v in vs]
    expected = category.get('expected_card_types') or []
    if expected and not any(l in expected for l in labels):
        raise PermanentError(f'Card labels {labels} are not in {expected}', 'LISTING_FILTER_IGNORED')
    if filters and not card_matches_filters(card, filters):
        return None
    url = site_url(card.get('href'), base_url)
    if not url:
        return None
    item_date = card_date(card)
    if item_date is None or item_date < scan_start or item_date > scan_end:
        return None
    category_labels = (card.get('groups') or {}).get('Category') or []
    return {
        'url': url,
        'title': (card.get('title') or '').strip() or url,
        'item_date': item_date,
        'item_date_label': CARD_DATE_LABEL,
        'item_reference': '; '.join(category_labels) or urlsplit(url).path.rstrip('/') or None,
    }


def site_url(href, base_url):
    '''The link as an ACRA page URL on the base host (acra.gov.sg without www is rewritten),
    no query, no fragment; None for other hosts.'''
    url = normalize_url(href or '', base_url)
    parts = urlsplit(url or '')
    if parts.netloc.lower() not in SITE_HOSTS:
        return None
    return urlunsplit((parts.scheme, urlsplit(base_url).netloc.lower(), parts.path, '', ''))


def section_root(category):
    '''The path whose sub pages belong to the category, lower case, no trailing slash.'''
    root = SECTION_ROOTS.get(category.get('category_id')) or urlsplit(category.get('listing_url') or '').path
    return root.rstrip('/').lower()


def section_candidates(hrefs, category, base_url):
    '''ACRA page URLs (no query, no fragment) whose path is under the section root, in first
    seen order. The Entry URL page itself is not included.'''
    root = section_root(category)
    entry = urlsplit(category['listing_url']).path.rstrip('/').lower()
    kept, seen = [], set()
    for href in hrefs:
        if urlsplit(href or '').query:
            continue                                    # filter / paging links
        url = site_url(href, base_url)
        if not url:
            continue
        key = urlsplit(url).path.rstrip('/').lower()
        if not key.startswith(root + '/') or key == entry or key.endswith(SKIP_EXTENSIONS):
            continue
        if key not in seen:
            seen.add(key)
            kept.append(url)
    return kept


def parse_section_page(info, url, scan_start, scan_end):
    '''Turn one opened section page into an item dict, or None when it has no Last updated
    date or falls outside the scan period.'''
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


def filter_pdf_links(links, category, base_url):
    '''Same as base_scraper.filter_pdf_links, but PDFs may be on any host in PDF_HOSTS. Spaces
    and other unsafe characters in the path are percent encoded (Isomer file names have them).'''
    include = [k.lower() for k in category.get('pdf_include_keywords') or []]
    exclude = [k.lower() for k in category.get('pdf_exclude_keywords') or []]
    kept, seen = [], set()
    for link in links:
        parts = urlsplit(normalize_url(link.get('href') or '', base_url))
        if parts.netloc not in PDF_HOSTS or not parts.path.lower().endswith('.pdf'):
            continue
        url = urlunsplit((parts.scheme, parts.netloc, quote(parts.path, safe="/%'()!*,;=:@&+$-._~"), '', ''))
        haystack = (url + ' ' + parts.path + ' ' + (link.get('text') or '')).lower()
        if include and not any(k in haystack for k in include):
            continue
        if exclude and any(k in haystack for k in exclude):
            continue
        if url in seen:
            continue
        seen.add(url)
        kept.append({'url': url, 'text': ' '.join((link.get('text') or '').split())[:500]})
    return kept


# Scripts run inside the browser.

_CARDS_SCRIPT = r'''(cfg) => {
    const clean = s => (s || '').replace(/\s+/g, ' ').trim();
    const scope = document.querySelector('main') || document.body;
    const cards = [...document.querySelectorAll(cfg.card)].map(c => {
        const t = c.querySelector(cfg.title);
        const d = c.querySelector(cfg.date);
        const groups = {};
        for (const g of c.querySelectorAll(cfg.group)) {
            const box = g.parentElement;
            groups[clean(g.textContent)] = box ? [...box.querySelectorAll(cfg.group_value)].map(v => clean(v.textContent)) : [];
        }
        const footer = [...c.querySelectorAll(cfg.footer)].map(s => clean(s.textContent)).filter(s => s);
        if (footer.length && !groups['Category']) groups['Category'] = footer;
        return {href: c.href,
                title: t ? clean(t.getAttribute('title') || t.textContent) : clean(c.textContent),
                date_text: d ? clean(d.textContent) : '',
                groups: groups,
                text: clean(c.innerText)};
    });
    const current = document.querySelector(cfg.current_page);
    const next = document.querySelector(cfg.next_page);
    const text = scope ? clean(scope.innerText).toLowerCase() : '';
    return {cards: cards,
            current_page: current ? parseInt(clean(current.textContent), 10) : null,
            has_pagination: !!current,
            next_disabled: !next || next.disabled || next.getAttribute('aria-disabled') === 'true',
            no_results: !!cfg.no_results && text.includes(cfg.no_results.toLowerCase()),
            title: document.title};
}'''

_PAGE_INFO_SCRIPT = r'''(cfg) => {
    const clean = s => (s || '').replace(/\s+/g, ' ').trim();
    const scope = document.querySelector('main') || document.body;
    const hrefs = [...scope.querySelectorAll('a[href]')]
        .filter(a => !a.closest('nav, header, footer, [role=navigation]'))
        .map(a => a.getAttribute('href'));
    let lastUpdated = '';
    for (const el of scope.querySelectorAll('div, p, span, time')) {
        const t = clean(el.textContent);
        if (t.startsWith(cfg.last_updated) && t.length < 80) { lastUpdated = t; break; }
    }
    const h1 = scope.querySelector('h1');
    return {hrefs: hrefs, last_updated: lastUpdated,
            title: h1 ? clean(h1.textContent) : clean(document.title)};
}'''

_SCRIPT_ARGS = {
    'card': CARD_SELECTOR,
    'title': TITLE_SELECTOR,
    'date': DATE_SELECTOR,
    'group': GROUP_SELECTOR,
    'group_value': GROUP_VALUE_SELECTOR,
    'footer': FOOTER_LABEL_SELECTOR,
    'current_page': CURRENT_PAGE_SELECTOR,
    'next_page': NEXT_PAGE_SELECTOR,
    'no_results': NO_RESULTS_TEXT,
    'last_updated': LAST_UPDATED_PREFIX,
}


# =====================================================================
# ACRA scraper
# =====================================================================

class AcraScraper(BaseScraper):

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
            problems.append('filter_by_date_in_url must be false (ACRA has no date range filter in the URL)')
        if listing_type == 'section_pages' and category.get('expected_card_types'):
            problems.append('expected_card_types must be empty for section_pages (no cards)')
        if category.get('listing_url'):
            if urlsplit(category['listing_url']).netloc.lower() != base_host:
                problems.append(f'listing_url must be on {base_host}')
            try:
                url_filters(category['listing_url'])
            except ValueError as exc:
                problems.append(f'listing_url {FILTERS_PARAM} parameter is not valid: {exc}')
        return problems

    # cards

    async def _read_cards_page(self, page, url, page_number, filters):
        '''Open one listing page and poll until it shows the asked page with cards that match the
        Entry URL filters (or the no results text). Polls with page.evaluate (no
        wait_for_function, which strict Content Security Policies block).'''
        def ready(result):
            if not result or not (result['cards'] or result['no_results']):
                return False
            if result['has_pagination'] and result['current_page'] != page_number:
                return False
            if not result['has_pagination'] and page_number != 1:
                return False
            return all(card_matches_filters(c, filters) for c in result['cards'])

        async def action():
            await self._open(page, url)
            deadline = time.monotonic() + self.render_timeout / 1000
            result = None
            while time.monotonic() < deadline:
                try:
                    result = await page.evaluate(_CARDS_SCRIPT, _SCRIPT_ARGS)
                except PlaywrightError:
                    result = None                       # page still navigating
                if ready(result):
                    await self._settle(page)            # let the list finish rendering
                    final = await page.evaluate(_CARDS_SCRIPT, _SCRIPT_ARGS)
                    return final if ready(final) else result
                await asyncio.sleep(POLL_SECONDS)
            state = 'no cards' if not result or not result['cards'] else \
                f"page {result.get('current_page')} shown" if result.get('current_page') != page_number else \
                'cards outside the Entry URL filters (filters not applied?)'
            raise RetryableError(f'Listing not ready in time ({state}). Page title '
                                 f'{(result or {}).get("title")!r}: {url}')
        return await self._with_retry(action, f'read listing {url}')

    async def _collect_cards(self, page, category, scan_start, filters):
        '''All cards of a listing. Returns (cards, warning).'''
        max_pages = int(self.config.get('max_pages_per_category', 20))
        cards, seen = [], set()
        for page_number in range(1, max_pages + 1):
            result = await self._read_cards_page(page, build_listing_url(category, page_number),
                                                 page_number, filters)
            new_cards = [c for c in result['cards'] if c.get('href') and c['href'] not in seen]
            if not new_cards:
                if not cards:
                    return [], LISTING_EMPTY_WARNING
                return cards, (LISTING_PARTIAL_WARNING if result['cards'] else None)
            for card in new_cards:
                seen.add(card['href'])
            cards += new_cards
            dates = [d for d in (card_date(c) for c in new_cards) if d]
            if dates and min(dates) < scan_start:
                return cards, None                      # newest first: later pages are older
            if result['next_disabled']:
                return cards, None                      # last page
        raise PermanentError(f'More than {max_pages} listing pages, raise max_pages_per_category',
                             'PAGINATION_LIMIT')

    # section_pages

    async def _page_info(self, page, url):
        '''Open a page and return its links, Last updated line and title, with retries.'''
        async def action():
            await self._open(page, url)
            await self._settle(page)
            return await page.evaluate(_PAGE_INFO_SCRIPT, _SCRIPT_ARGS)
        return await self._with_retry(action, f'read {url}')

    async def _discover_section(self, page, category, scan_start, scan_end):
        entry_url = category['listing_url']
        entry_info = await self._page_info(page, entry_url)
        candidates = section_candidates(entry_info['hrefs'], category, self.base_url)
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
                info = await self._page_info(page, url)
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
                       f'Last updated date and were skipped.')
        self.logger.info('[%s] %s section pages checked', category['category_id'], len(candidates) + 1)
        return items, warning

    async def discover(self, page, category, scan_start, scan_end):
        if category['listing_type'] == 'cards':
            filters = url_filters(category['listing_url'])
            cards, warning = await self._collect_cards(page, category, scan_start, filters)
            items = [parse_card(c, category, self.base_url, scan_start, scan_end, filters) for c in cards]
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

    # Item processing

    async def _process_item(self, page, context, category, item, counters):
        '''Same flow as BaseScraper._process_item, but with the ACRA PDF filter above (PDFs on
        isomer-user-content.by.gov.sg). Keep in step with base_scraper.'''
        item_fields = {
            'category_id': category['category_id'], 'category_name': category['category_name'],
            'item_title': item['title'], 'item_date': item['item_date'],
            'item_date_label': item['item_date_label'], 'item_reference': item['item_reference'],
            'source_url': item['url'],
        }
        before = Counters(**asdict(counters))
        try:
            await self._goto(page, item['url'])
            await self._settle(page)

            documents = []
            if category.get('download_pdfs', True):
                links = filter_pdf_links(await self._page_links(page), category, self.base_url)
                documents += [('PDF', link) for link in links]
            if category.get('capture_html') or not documents:
                documents.append(('HTML', None))

            for content_format, link in documents:
                counters.documents_found += 1
                await self._process_document(page, context, category, item, item_fields,
                                             content_format, link, counters)

            failed_here = counters.documents_failed - before.documents_failed
            self.audit.add('ITEM', SUCCESS if failed_here == 0 else PARTIAL_SUCCESS, **item_fields,
                           documents_found=counters.documents_found - before.documents_found,
                           documents_new=counters.documents_new - before.documents_new,
                           documents_changed=counters.documents_changed - before.documents_changed,
                           documents_skipped=counters.documents_skipped - before.documents_skipped,
                           documents_failed=failed_here)
        except PermanentError as error:
            if error.category == 'SOURCE_UNAVAILABLE':
                self.audit.add('ITEM', FAILED, **item_fields, error_category=error.category,
                               error_details=str(error))
                counters.items_failed += 1
                raise
            self._item_failed(category, item, item_fields, counters, error)
        except Exception as error:
            self._item_failed(category, item, item_fields, counters, error)
