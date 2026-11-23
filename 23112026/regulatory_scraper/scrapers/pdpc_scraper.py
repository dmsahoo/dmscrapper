'''
PDPC discovery: which PDPC (www.pdpc.gov.sg) items fall inside the scan period for a category.

Built on scrapers/mas_scraper.py (the master scraper), scrapers/sso_scraper.py and
scrapers/mom_scraper.py: same structure, same category parameters as mas_config.yaml.
Everything after discovery (item pages, downloads, decisions, storage, the run loop) is shared
and lives in scrapers/base_scraper.py.

How PDPC items are found (follows the intake tracker)
  1_MEDIA_RELEASES          /media-events?type=Media+Releases          (PLACEHOLDER URL)
  2_ADVISORY_GUIDELINES     /organisations/regulations-decisions/regulatory-guidance?type=Advisory+Guidelines
  3_PUBLIC_CONSULTATIONS    /organisations/regulations-decisions/public-consultations
  4_RESPONSES_TO_FEEDBACK   same listing as 3, PDFs with 'response' only
  5_COMMISSION_DECISIONS    /organisations/regulations-decisions/enforcement-decisions?type=Commission's+Decisions
  6_VOLUNTARY_UNDERTAKINGS  /organisations/regulations-decisions/enforcement-decisions?type=Voluntary+Undertakings
  7A_ADVISORIES             /media-events?type=Advisories
  7B_SECTOR_GUIDANCE        /organisations/regulations-decisions/regulatory-guidance?type=Sector-Specific+Guidelines

No page selectors to configure
  The PDPC site sits behind an AWS WAF bot challenge (plain requests get HTTP 202, empty body),
  so its HTML could not be inspected. The browser passes the challenge itself; the scraper
  waits until the challenge page is gone (_settle). Instead of CSS selectors, a result card is
  found from what every PDPC listing shows: a link to the item with a date next to it.
    card            the smallest block around an item link that shows a date and links to
                    only that one item (links in nav, header and footer are ignored)
    title           the first heading in the card, else the longest link text
    item_date       the first date in the card text (12 March 2026, 12 Mar 2026, 12/03/2026)
    item_reference  the item path, for example /all-commissions-decisions/2026/03/...
  Links with a query (filters, paging, sort) and links back to the Entry URL are not items.
  A card may link straight to a PDF: then that PDF is the item's only document.

Paging
  An Entry URL with page=1 is read page=1, 2, ... (sort=latest, newest first) until a page adds
  no new cards or every dated card on it is older than the scan start. An Entry URL without a
  page parameter is read as one page.

Zero items
  Nothing in the period is recorded as 0 items. A listing that shows no dated item links at all
  is also recorded as 0 items, with a warning to check the Entry URL.

Safety checks that fail a category
  1. The listing did not show any dated item link or the no results text in time (this
     includes the bot challenge not being passed).
  2. More pages than max_pages_per_category.
'''

import asyncio
import re
import time
from dataclasses import asdict
from datetime import datetime
from urllib.parse import parse_qsl, quote_plus, urlencode, urlsplit, urlunsplit

from playwright.async_api import Error as PlaywrightError
from playwright.async_api import TimeoutError as PlaywrightTimeoutError

from scrapers.base_scraper import BaseScraper, Counters, PermanentError, RetryableError
from scrapers.tracking import FAILED, PARTIAL_SUCCESS, SUCCESS, normalize_url

LISTING_TYPES = {'cards'}
DATE_SOURCES = {'listing_date'}

# ---- PDPC page details (unverified: site blocked from the workspace, 2026-11-13) ----------
PAGE_PARAM = 'page'                     # page number parameter in the Entry URLs
NO_RESULTS_TEXT = 'No results'          # text that counts as an empty listing (PLACEHOLDER)
DATE_LABEL = 'Listing Date'             # name stored for the item date
MAX_CARD_LEVELS = 6                     # how far above an item link the card date is searched
CHALLENGE_MARKERS = ('gokuProps', 'challenge-container', 'awswaf')   # AWS WAF challenge page
POLL_SECONDS = 1.0
# -------------------------------------------------------------------------------------------

LISTING_EMPTY_WARNING = ('Listing showed no dated results at all. Recorded as 0 items. '
                         'Check that the Entry URL still works.')
LISTING_PARTIAL_WARNING = ('Listing stopped adding results before reaching the scan start '
                           '(the page number may be ignored). Items may be missing.')

_TEXT_DATE_RE = re.compile(r'\b(\d{1,2})(?:st|nd|rd|th)?\s+([A-Za-z]{3,9})\.?,?\s+(\d{4})\b')
_NUM_DATE_RE = re.compile(r'\b(\d{1,2})/(\d{1,2})/(\d{4})\b')


# =====================================================================
# Pure helpers (unit tested without a browser)
# =====================================================================

def parse_display_date(text):
    '''First date in the text written like 12 March 2026, 12 Mar 2026 or 12/03/2026 (day
    first, as on Singapore government sites), or None.'''
    text = text or ''
    candidates = []
    for match in _TEXT_DATE_RE.finditer(text):
        day, month, year = match.groups()
        for fmt in ('%d %B %Y', '%d %b %Y'):
            try:
                candidates.append((match.start(), datetime.strptime(f'{day} {month[:3] if fmt == "%d %b %Y" else month} {year}', fmt).date()))
                break
            except ValueError:
                continue
    for match in _NUM_DATE_RE.finditer(text):
        day, month, year = (int(g) for g in match.groups())
        try:
            candidates.append((match.start(), datetime(year, month, day).date()))
        except ValueError:
            continue
    return min(candidates)[1] if candidates else None


def is_paged(category):
    '''True when the Entry URL has a page parameter (the listing is read page by page).'''
    return PAGE_PARAM in dict(parse_qsl(urlsplit(category['listing_url']).query))


def build_listing_url(category, page_number=1):
    '''The category Entry URL with PAGE_PARAM set to page_number (other parameters kept).'''
    parts = urlsplit(category['listing_url'])
    query = [(k, v) for k, v in parse_qsl(parts.query, keep_blank_values=True) if k != PAGE_PARAM]
    if is_paged(category) or page_number > 1:
        query.append((PAGE_PARAM, str(page_number)))
    # quote_plus keeps the site's own form: type=Commission%27s+Decisions
    return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(query, quote_via=quote_plus), ''))


def is_pdf(url):
    return urlsplit(url or '').path.lower().endswith('.pdf')


def parse_card(card, category, base_url, scan_start, scan_end):
    '''Turn one card into an item dict, or None when it has no usable link or date or falls
    outside the scan period.'''
    url = normalize_url(card.get('href') or '', base_url)
    parts = urlsplit(url or '')
    if not url or parts.netloc != urlsplit(base_url).netloc.lower():
        return None
    if parts.path.rstrip('/').lower() in ('', urlsplit(category['listing_url']).path.rstrip('/').lower()):
        return None
    item_date = parse_display_date(card.get('text'))
    if item_date is None or item_date < scan_start or item_date > scan_end:
        return None
    return {
        'url': url,
        'title': ' '.join((card.get('title') or '').split()) or url,
        'item_date': item_date,
        'item_date_label': DATE_LABEL,
        'item_reference': parts.path.rstrip('/') or None,
    }


# Scripts run inside the browser. The date pattern is shared with parse_display_date (a match
# only has to be found here; Python parses it).

_CARDS_SCRIPT = r'''(cfg) => {
    const clean = s => (s || '').replace(/\s+/g, ' ').trim();
    const dateRe = new RegExp(cfg.date_re, 'i');
    const host = location.host.toLowerCase();
    const entryPath = cfg.entry_path;
    const html = document.documentElement ? document.documentElement.outerHTML.slice(0, 20000) : '';
    const challenge = cfg.challenge.some(m => html.includes(m));
    const scope = document.querySelector('main') || document.body;
    if (!scope) return {cards: [], no_results: false, challenge: challenge, title: document.title};

    const itemKey = a => {
        let u;
        try { u = new URL(a.href); } catch (e) { return null; }
        if (!/^https?:$/.test(u.protocol) || u.host.toLowerCase() !== host || u.search) return null;
        const path = u.pathname.replace(/\/+$/, '').toLowerCase();
        if (!path || path === entryPath) return null;
        if (a.closest('nav, header, footer, [role=navigation]')) return null;
        return path;
    };
    const isPdf = k => /\.pdf$/.test(k);
    const keysIn = el => new Set([...el.querySelectorAll('a[href]')].map(itemKey).filter(k => k));
    const pageKeysIn = el => new Set([...keysIn(el)].filter(k => !isPdf(k)));

    const byKey = new Map();
    for (const a of scope.querySelectorAll('a[href]')) {
        const key = itemKey(a);
        if (!key || byKey.has(key)) continue;
        let el = a, card = null;
        for (let level = 0; level <= cfg.max_levels && el && el !== scope.parentElement; level++) {
            // A block that holds other items has no date of its own. PDF links next to an item
            // page link belong to that item (its page is opened and the PDFs found there).
            if (isPdf(key) ? (pageKeysIn(el).size > 0 || keysIn(el).size > 1) : pageKeysIn(el).size > 1) break;
            if (dateRe.test(el.innerText || '')) { card = el; break; }
            el = el.parentElement;
        }
        if (!card) continue;
        const heading = card.querySelector('h1, h2, h3, h4, h5, [role=heading]');
        const links = [...card.querySelectorAll('a[href]')].filter(x => itemKey(x) === key)
            .map(x => clean(x.innerText || x.textContent)).sort((p, q) => q.length - p.length);
        byKey.set(key, {href: a.href,
                        title: heading ? clean(heading.innerText) : (links[0] || ''),
                        text: clean(card.innerText)});
    }
    const text = clean(scope.innerText).toLowerCase();
    return {cards: [...byKey.values()],
            no_results: !!cfg.no_results && text.includes(cfg.no_results.toLowerCase()),
            challenge: challenge,
            title: document.title};
}'''

_CHALLENGE_SCRIPT = r'''(markers) => {
    const html = document.documentElement ? document.documentElement.outerHTML.slice(0, 20000) : '';
    return markers.some(m => html.includes(m));
}'''

# Same date shapes as _TEXT_DATE_RE / _NUM_DATE_RE, in JavaScript syntax
_JS_DATE_RE = (r'\b\d{1,2}(st|nd|rd|th)?\s+(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*\.?,?\s+\d{4}\b'
               r'|\b\d{1,2}/\d{1,2}/\d{4}\b')


# =====================================================================
# PDPC scraper
# =====================================================================

class PdpcScraper(BaseScraper):

    @classmethod
    def validate_category(cls, category, base_host):
        problems = []
        for key in ('listing_type', 'listing_url', 'date_source'):
            if not category.get(key):
                problems.append(f'missing {key}')
        if category.get('listing_type') and category['listing_type'] not in LISTING_TYPES:
            problems.append(f'listing_type must be one of {sorted(LISTING_TYPES)}')
        if category.get('date_source') and category['date_source'] not in DATE_SOURCES:
            problems.append(f'date_source must be one of {sorted(DATE_SOURCES)}')
        if category.get('filter_by_date_in_url', False):
            problems.append('filter_by_date_in_url must be false (PDPC has no date range filter in the URL)')
        if category.get('expected_card_types'):
            problems.append('expected_card_types must be [] (card labels are not read on PDPC)')
        if category.get('listing_url') and urlsplit(category['listing_url']).netloc.lower() != base_host:
            problems.append(f'listing_url must be on {base_host}')
        return problems

    # Bot challenge

    async def _settle(self, page):
        '''Wait for background requests, then until the AWS WAF challenge page has gone (the
        browser solves it and reloads). Polls with page.evaluate (no wait_for_function, which
        strict Content Security Policies block). Gives up silently after render_timeout; the
        caller then sees no content.'''
        await super()._settle(page)
        deadline = time.monotonic() + self.render_timeout / 1000
        while time.monotonic() < deadline:
            try:
                if not await page.evaluate(_CHALLENGE_SCRIPT, list(CHALLENGE_MARKERS)):
                    return
            except PlaywrightError:
                pass                                    # page navigating after the challenge
            await asyncio.sleep(POLL_SECONDS)
        try:
            await page.wait_for_load_state('networkidle', timeout=10000)
        except PlaywrightTimeoutError:
            pass

    # Reading listing pages

    async def _read_listing_page(self, page, url, category):
        '''Open one listing page and poll until it shows dated item links or the no results
        text. Returns {cards, no_results}.'''
        args = {'date_re': _JS_DATE_RE, 'entry_path': urlsplit(category['listing_url']).path.rstrip('/').lower(),
                'no_results': NO_RESULTS_TEXT, 'challenge': list(CHALLENGE_MARKERS),
                'max_levels': MAX_CARD_LEVELS}

        async def action():
            await self._open(page, url)
            deadline = time.monotonic() + self.render_timeout / 1000
            result = None
            while time.monotonic() < deadline:
                try:
                    result = await page.evaluate(_CARDS_SCRIPT, args)
                except PlaywrightError:
                    result = None                       # page navigating after the challenge
                if result and not result['challenge'] and (result['cards'] or result['no_results']):
                    await self._settle(page)            # client rendered: let the list finish
                    return await page.evaluate(_CARDS_SCRIPT, args)
                await asyncio.sleep(POLL_SECONDS)
            state = 'bot challenge not passed' if result and result.get('challenge') else \
                'no dated item links (layout or date format changed?)'
            raise RetryableError(f'Listing not ready in time, {state}. Page title '
                                 f'{(result or {}).get("title")!r}: {url}')
        return await self._with_retry(action, f'read listing {url}')

    async def _collect_cards(self, page, category, scan_start):
        '''All cards of a listing. Returns (cards, warning).'''
        max_pages = int(self.config.get('max_pages_per_category', 20)) if is_paged(category) else 1
        cards, seen = [], set()
        for page_number in range(1, max_pages + 1):
            result = await self._read_listing_page(page, build_listing_url(category, page_number), category)
            new_cards = [c for c in result['cards'] if normalize_url(c['href']) not in seen]
            if not new_cards:
                if not cards:
                    return [], LISTING_EMPTY_WARNING
                return cards, (LISTING_PARTIAL_WARNING if result['cards'] else None)
            for card in new_cards:
                seen.add(normalize_url(card['href']))
            cards += new_cards
            dates = [d for d in (parse_display_date(c.get('text')) for c in new_cards) if d]
            if dates and max(dates) < scan_start:
                return cards, None                      # whole page older than the scan
            if not is_paged(category):
                return cards, None
        if not is_paged(category):
            return cards, None
        raise PermanentError(f'More than {max_pages} listing pages, raise max_pages_per_category',
                             'PAGINATION_LIMIT')

    async def discover(self, page, category, scan_start, scan_end):
        cards, warning = await self._collect_cards(page, category, scan_start)
        items = [parse_card(c, category, self.base_url, scan_start, scan_end) for c in cards]
        unique, seen = [], set()
        for item in items:
            if item and item['url'] not in seen:
                seen.add(item['url'])
                unique.append(item)
        self.logger.info('[%s] %s cards read, %s items in scan period%s', category['category_id'],
                         len(cards), len(unique),
                         ' (listing empty, recorded as 0)' if warning == LISTING_EMPTY_WARNING else '')
        return unique, warning

    # Item processing

    async def _process_item(self, page, context, category, item, counters):
        '''A card that links straight to a PDF: download it as the item's only document (when
        download_pdfs is true) instead of opening it as a page. Other items use the shared flow.'''
        if not is_pdf(item['url']) or not category.get('download_pdfs', True):
            if is_pdf(item['url']):
                self.logger.info('[%s] Skipped PDF item (download_pdfs false): %s',
                                 category['category_id'], item['url'])
                return
            return await super()._process_item(page, context, category, item, counters)
        haystack = (item['url'] + ' ' + item['title']).lower()
        include = [k.lower() for k in category.get('pdf_include_keywords') or []]
        exclude = [k.lower() for k in category.get('pdf_exclude_keywords') or []]
        if (include and not any(k in haystack for k in include)) or any(k in haystack for k in exclude):
            return                                      # PDF keyword filters apply to direct PDFs too
        item_fields = {
            'category_id': category['category_id'], 'category_name': category['category_name'],
            'item_title': item['title'], 'item_date': item['item_date'],
            'item_date_label': item['item_date_label'], 'item_reference': item['item_reference'],
            'source_url': item['url'],
        }
        before = Counters(**asdict(counters))
        try:
            counters.documents_found += 1
            await self._process_document(page, context, category, item, item_fields, 'PDF',
                                         {'url': item['url'], 'text': item['title']}, counters)
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
