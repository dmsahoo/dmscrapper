'''
Parliament discovery: which Parliament items fall inside the scan period for a category.

Built on scrapers/pdpc_scraper.py and scrapers/pdpc_scraper.py and scrapers/mas_scraper.py (the master scraper): same
structure, same category parameters as mas_config.yaml. Everything after discovery (item
pages, downloads, decisions, storage, the run loop) is shared and lives in
scrapers/base_scraper.py.

Three websites, no core website
  The intake tracker spreads the Parliament categories over three websites, so a category's
  website is the host of its Entry URL (listing_url). Everything website specific comes from
  SITES below, never from a category id:
    www.parliament.gov.sg  cards            1_FS_CROSS_CUTTING_BILLS, 2_NON_FS_BILLS (Bills Introduced)
    sprs.parl.gov.sg       sitting_reports  3_MINISTERIAL_STATEMENTS_HANSARD_FS (Hansard)
    www.egazette.gov.sg    cards, pdf_only  4_COMMENCEMENT_GAZETTED_ACTS (eGazette search)
  While a category runs, its website is the scraper's base_url: item links must be on it (or
  on its item_hosts), PDF links on item pages are kept only from it, and relative links resolve
  against it. base_url in the config is only the regulator's home, used for the landing page
  at the start of a run. Adding a website means adding one SITES entry.

listing_type cards (www.parliament.gov.sg, www.egazette.gov.sg)
  No page selectors to configure. A result card is the smallest block around an item link
  that shows a date and links to only that on is downloaded as the item's only document. On a pdf_only
  website (eGazette) every card is handled that way, so download_pdfs must be true and
  capture_html false. Cards carry no type label, so expected_card_types must be [].
  www.parliament.gov.sg sits behind an AWS WAF bot challenge (plain requests get HTTP 202 with
  an empty body); the browser passes it and _settle waits until the challenge page is gone.
  Listings are polled with page.evaluate, not page.wait_for_function, wh the card shows one, elsich the eGazette
  Content Security Policy blocks (no unsafe-eval).

listing_type sitting_reports (sprs.parl.gov.sg)
  The Hansard search is a single page app: its home page is a search form without result
  links, and results cannot be opened by URL. The app reads each sitting from a JSON service,
  wh the card shows one, els the card shows one, els the card shows one, els the card shows one, els the card shows one, els the card shows one, els the card shows one, els the card shows one, els the card shows one, els the card shows one, els the card shows one, els the card shows one, els the card shows one, els the card shows one, els the card shows one, els the card shows one, els the card shows one, els the card shows one, els the card shows one, els the card shows one, els the card shows one, els the card shows one, els the card shows one, els the card shows one, els the card shows one, els the card shows one, els the card shows one, els the card shows one, els the card shows one, els the card shows one, els the card shows one, els the card shows one, els the card shows one, els the card shows one, els the card shows one, els the card shows one, els the card shows one, els the card shows one, els the card shows one, els the card shows one, els the card shows one, els the card shows one, els the card shows one, els the card shows one, els the card shows one, els the card shows one, els the card shows one, els the card shows one, els the card shows one, els the card shows one, els the card shows one, els the card shows one, els the card shows one, els the card shows one, els the card shows one, els the card shows one, els the card shows one, els the card shows one, els the card shows one, els els els els els els els els els els els els els els els els els els els els els els els els els els els els els els els els els els els els els els els els els elsich the scraper calls for every day of the scan period:
    POST /search/getHansardReport/ {"sittingDate": "DD-MM-YYYY"}
      HTTP 200 with metadata     Parliament sat that day
      HTTP 500 with errorCode    no sitting that day
  Each sitting is one item (Sitting Date, link /search/#/fullreport?sittingdate=DD-MM-YYYY),
  saved as one HTML snapshot built from the sitting's sections (title, section type, text), so
  the category needs capture_html true. Its listing_url is only the website's Entry page.

Bills split (categories 1 and 2)
  Both read the Bills Introduced listing. Titles are split with
  title_keyword_sets.FS_CROSS_CUTTING in the config: category 1 keeps titles that match,
  category 2 keeps the rest, so every bill lands in exactly one of them.

Paging (cards)
  An Entry URL with page= is read page=1, 2, ... until a page adds no new cards or every dated
  card on it is older than the scan start. An Entry URL without page= is read as one page.
  No website has a known date filter in the URL, so filter_by_date_in_url must be false.

Zero items
  Nothing in the period is recorded as 0 items. A listing without any dated item link, or a
  period without any sitting, is also recorded as 0 items, with a warning.

Safety checks that fail a category
  1. The listing did not show any dated item link or the no results text in time (this
     includes the bot challenge not being passed).
  2. More pages than max_pages_per_category.
  3. The Hansard service answered other than described above, after retries.
'''

import asyncio
import calendar
import html
import re
import time
from dataclasses import asdict
from datetime import date, timedelta
from urllib.parse import parse_qsl, quote, urlencode, urlsplit, urlunsplit

from playwright.async_api import Error as PlaywrightError
from playwright.async_api import TimeoutError as PlaywrightTimeoutError

from scrapers.base_scraper import BaseScraper, Counters, PermanentError, RetryableError
from scrapers.tracking import FAILED, PARTIAL_SUCCESS, SUCCESS, normalize_url

LISTING_TYPES = {'cards', 'sitting_reports'}
DATE_SOURCES = {'listing_date'}

# ---- The Parliament websites: one entry per Entry URL host ------------------------------
#   listing     the listing_type every category on this website must use
#   date_label  name stored for the item date
#   item_hosts  other hosts item links may point to. eGazette results link to their PDF on
#               assets.egazette.gov.sg (checked 2026-11-18 in the eGazette search index)
#   pdf_only    every result links straight to its PDF
SITES = {
    'www.parliament.gov.sg': {'name': 'Parliament', 'listing': 'cards', 'date_label': 'Date Introduced',
                              'item_hosts': (), 'pdf_only': False},
    'sprs.parl.gov.sg': {'name': 'Hansard', 'listing': 'sitting_reports', 'date_label': 'Sitting Date',
                         'item_hosts': (), 'pdf_only': False},
    'www.egazette.gov.sg': {'name': 'eGazette', 'listing': 'cards', 'date_label': 'Gazette Date',
                            'item_hosts': ('assets.egazette.gov.sg',), 'pdf_only': True},
}

# ---- Parliament categories --------------------------------------------------------------
FS_KEYWORD_SET = 'FS_CROSS_CUTTING'
FS_SPLIT = {'1_FS_CROSS_CUTTING_BILLS': 'include', '2_NON_FS_BILLS': 'exclude'}

# ---- Card listings ----------------------------------------------------------------------
PAGE_PARAM = 'page'                     # page number parameter in the Entry URLs
NO_RESULTS_TEXT = 'No result'           # text that counts as an empty listing (also 'No results')
MAX_CARD_LEVELS = 6                     # how far above an item link the card date is searched
CHALLENGE_MARKERS = ('gokuProps', 'challenge-container')   # AWS WAF challenge page only
POLL_SECONDS = 1.0

# ---- Hansard (sprs.parl.gov.sg), checked 2026-11-17 -------------------------------------
HANSARD_REPORT_API = '/search/getHansardReport/'
HANSARD_REPORT_LINK = '/search/#/fullreport?sittingdate={}'
HANSARD_SECTION_TYPES = {               # section codes in the service; labels are descriptive
    'OA': 'Oral Answers to Questions',
    'WA': 'Written Answers to Questions',
    'WANA': 'Written Answers to Oral Questions not answered by end of Question Time',
    'OS': 'Other Statements and Proceedings',
    'MS': 'Ministerial Statement',
    'BI': 'Bill Introduced',
    'BP': 'Bill Proceedings',
}

LISTING_EMPTY_WARNING = ('Listing showed no dated results at all. Recorded as 0 items. '
                         'Check that the Entry URL still works.')
LISTING_PARTIAL_WARNING = ('Listing stopped adding results before reaching the scan start '
                           '(the page number may be ignored). Items may be missing.')
NO_SITTINGS_WARNING = ('No Parliament sitting found in the scan period. Recorded as 0 items. '
                       'If Parliament did sit, check the Hansard service.')

_TEXT_DATE_RE = re.compile(r'\b(\d{1,2})(?:st|nd|rd|th)?\s+([A-Za-z]{3,9})\.?,?\s+(\d{4})\b')
_NUM_DATE_RE = re.compile(r'\b(\d{1,2})[./-](\d{1,2})[./-](\d{4})\b')
_BILL_NO_RE = re.compile(r'Bill\s+No\.?\s*:?\s*(\d+/\d{4})', re.IGNORECASE)
_MONTHS = [m.lower() for m in calendar.month_name[1:]]
_TAG_RE = re.compile(r'<[^>]+>')


# =====================================================================
# Pure helpers (unit tested without a browser)
# =====================================================================

def parse_display_date(text):
    '''First date in the text written like 12 March 2026, 12 Mar 2026, 12/03/2026 or
    12-03-2026 (day first, as on Singapore government sites), or None.'''
    text = text or ''
    candidates = []
    for match in _TEXT_DATE_RE.finditer(text):
        day, month, year = match.groups()
        month_no = next((i for i, name in enumerate(_MONTHS, 1) if name.startswith(month.lower())), None)
        if month_no is None:
            continue
        try:
            candidates.append((match.start(), date(int(year), month_no, int(day))))
        except ValueError:
            continue
    for match in _NUM_DATE_RE.finditer(text):
        day, month, year = (int(g) for g in match.groups())
        try:
            candidates.append((match.start(), date(year, month, day)))
        except ValueError:
            continue
    return min(candidates)[1] if candidates else None


# ---- the category's website ----------------------------------------------------------------

def site_host(category):
    '''Host of the category Entry URL, for example sprs.parl.gov.sg.'''
    return urlsplit(category.get('listing_url') or '').netloc.lower()


def site_of(category):
    '''SITES entry of the category's website, or None for a website not in SITES.'''
    return SITES.get(site_host(category))


def site_base(category):
    '''Root of the category's website, for example https://sprs.parl.gov.sg.'''
    parts = urlsplit(category['listing_url'])
    return f'{parts.scheme.lower()}://{parts.netloc.lower()}'


def item_hosts(category):
    '''Hosts the category's item links may be on: its website, plus the website's item_hosts.'''
    return {site_host(category), *(site_of(category) or {}).get('item_hosts', ())}


# ---- card listings ---------------------------------------------------------------------------

def is_paged(category):
    '''True when the Entry URL has a page parameter (the listing is read page by page).'''
    return PAGE_PARAM in dict(parse_qsl(urlsplit(category['listing_url']).query))


def build_listing_url(category, page_number=1):
    '''The Entry URL as configured; on a paged listing with PAGE_PARAM set to page_number
    (other parameters and any #fragment kept).'''
    if not is_paged(category):
        return category['listing_url']
    parts = urlsplit(category['listing_url'])
    query = [(k, v) for k, v in parse_qsl(parts.query, keep_blank_values=True) if k != PAGE_PARAM]
    query.append((PAGE_PARAM, str(page_number)))
    return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(query, quote_via=quote),
                       parts.fragment))


def is_pdf(url):
    return urlsplit(url or '').path.lower().endswith('.pdf')


def parse_card(card, category, scan_start, scan_end):
    '''Turn one card into an item dict, or None when it has no usable link or date or falls
    outside the scan period. Card links come from the browser, so they are absolute.'''
    url = normalize_url(card.get('href') or '') if card.get('href') else ''
    parts = urlsplit(url)
    if not url or parts.netloc not in item_hosts(category):
        return None
    item_date = parse_display_date(card.get('text'))
    if item_date is None or item_date < scan_start or item_date > scan_end:
        return None
    bill_no = _BILL_NO_RE.search(card.get('text') or '')
    return {
        'url': url,
        'title': ' '.join((card.get('title') or '').split()) or url,
        'item_date': item_date,
        'item_date_label': site_of(category)['date_label'],
        'item_reference': bill_no.group(1) if bill_no else (parts.path.rstrip('/') or None),
    }


# ---- Hansard sittings ----------------------------------------------------------------------

def sitting_day_text(day):
    '''Sitting date as the Hansard service writes it: DD-MM-YYYY.'''
    return day.strftime('%d-%m-%Y')


def _text(value):
    '''Service values as text; the service writes missing values as None or \'None\'.'''
    return '' if value is None or value == 'None' else str(value)


def sitting_item(category, day, report):
    '''Item dict for one sitting read from the Hansard service.'''
    meta = report.get('metadata') or {}
    shown = _text(meta.get('dateToDisplay')) or day.strftime('%A, %d %B %Y')
    reference = ' '.join(f'{label} {_text(meta.get(key))}' for label, key in (
        ('Parliament', 'parlimentNO'), ('Session', 'sessionNO'), ('Volume', 'volumeNO'),
        ('Sitting', 'sittingNO')) if _text(meta.get(key)))
    return {
        'url': site_base(category) + HANSARD_REPORT_LINK.format(sitting_day_text(day)),
        'title': f'Hansard: {shown}',
        'item_date': day,
        'item_date_label': site_of(category)['date_label'],
        'item_reference': reference or None,
    }


def sitting_report_body(report):
    '''Page body for the HTML snapshot of one sitting: every section with its title and type,
    in the order of the Official Report. Same shape as BaseScraper._page_body.'''
    meta = report.get('metadata') or {}
    parts = [f'<p>{html.escape(_text(meta.get("dateToDisplay")))}</p>']
    for section in report.get('takesSectionVOList') or []:
        code = _text(section.get('sectionType'))
        sub_title = _text(section.get('subTitle'))
        parts.append(
            f'<section data-section-type="{html.escape(code)}">'
            f'<h2>{html.escape(_text(section.get("title")))}</h2>'
            + (f'<h3>{html.escape(sub_title)}</h3>' if sub_title else '')
            + f'<p><em>{html.escape(HANSARD_SECTION_TYPES.get(code, code))}</em></p>'
            f'{_text(section.get("content"))}</section>')
    body_html = '\n'.join(parts)
    return {'selector': 'hansard-service', 'html': body_html,
            'text': html.unescape(_TAG_RE.sub(' ', body_html))}


def fs_section_titles(report, keyword_sets):
    '''Titles of the sitting's sections that contain an FS keyword (logged, not filtered).'''
    pattern = _keyword_pattern((keyword_sets or {}).get(FS_KEYWORD_SET))
    titles = [_text(s.get('title')) for s in report.get('takesSectionVOList') or []]
    return [t for t in titles if pattern and pattern.search(' '.join(t.split()))]


# ---- FS split of the bills -----------------------------------------------------------------

def _keyword_pattern(keywords):
    words = [' '.join(k.split()) for k in keywords or [] if k and k.strip()]
    if not words:
        return None
    # (?<!\w) and (?!\w) instead of \b, so keywords ending in ')' still match.
    # A keyword ending in * matches any word starting with it (financ* = finance, financial)
    parts = [re.escape(w[:-1]) if w.endswith('*') else re.escape(w) + r'(?!\w)' for w in words]
    return re.compile(r'(?<!\w)(?:' + '|'.join(parts) + r')', re.IGNORECASE)


def fs_split_keeps(title, category, keyword_sets):
    '''FS split of the bills: 1_FS_CROSS_CUTTING_BILLS keeps titles with an FS keyword,
    2_NON_FS_BILLS keeps the others. Every other category keeps everything.'''
    mode = FS_SPLIT.get(category.get('category_id'))
    if mode is None:
        return True
    pattern = _keyword_pattern((keyword_sets or {}).get(FS_KEYWORD_SET))
    matched = bool(pattern and pattern.search(' '.join((title or '').split())))
    return matched if mode == 'include' else not matched


# Scripts run inside the browser. The date pattern matches the same shapes as
# parse_display_date (a match only has to be found here; Python parses it).
# An item key is host + lower case path, so links with and without a trailing slash match.

_CARDS_SCRIPT = r'''(cfg) => {
    const clean = s => (s || '').replace(/\s+/g, ' ').trim();
    const dateRe = new RegExp(cfg.date_re, 'i');
    const html = document.documentElement ? document.documentElement.outerHTML.slice(0, 20000) : '';
    const challenge = cfg.challenge.some(m => html.includes(m));
    const scope = document.querySelector('main') || document.body;
    if (!scope) return {cards: [], no_results: false, challenge: challenge, title: document.title};

    const isPdf = k => /\.pdf$/.test(k);
    const itemKey = a => {
        let u;
        try { u = new URL(a.href); } catch (e) { return null; }
        const host = u.host.toLowerCase();
        if (!/^https?:$/.test(u.protocol) || !cfg.hosts.includes(host)) return null;
        if (a.closest('nav, header, footer, [role=navigation]')) return null;
        const key = host + u.pathname.replace(/\/+$/, '').toLowerCase();
        if (u.search && !isPdf(key)) return null;        // filter, paging and sort links
        if (key === host || key === cfg.entry_key) return null;
        return key;
    };
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
               r'|\b\d{1,2}[./-]\d{1,2}[./-]\d{4}\b')


# =====================================================================
# Parliament scraper
# =====================================================================

class ParliamentScraper(BaseScraper):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.sitting_reports = {}       # Hansard item link -> sitting read in discover()
        self.prepared_body = None       # snapshot body that does not come from an open page

    @classmethod
    def validate_category(cls, category, base_host):
        '''base_host is not used: every category is checked against its own website.'''
        problems = []
        for key in ('listing_type', 'listing_url', 'date_source'):
            if not category.get(key):
                problems.append(f'missing {key}')
        site = site_of(category)
        listing_type = category.get('listing_type')
        if category.get('listing_url') and site is None:
            problems.append(f'listing_url must be on one of the Parliament websites {sorted(SITES)}')
        if listing_type and listing_type not in LISTING_TYPES:
            problems.append(f'listing_type must be one of {sorted(LISTING_TYPES)}')
        elif site and listing_type and listing_type != site['listing']:
            problems.append(f'listing_type must be {site["listing"]} on {site_host(category)}')
        if category.get('date_source') and category['date_source'] not in DATE_SOURCES:
            problems.append(f'date_source must be one of {sorted(DATE_SOURCES)}')
        if category.get('filter_by_date_in_url', False):
            problems.append('filter_by_date_in_url must be false (no Parliament website has a date filter in the URL)')
        if category.get('expected_card_types'):
            problems.append('expected_card_types must be [] (Parliament cards have no type label)')
        if site and site['pdf_only'] and (not category.get('download_pdfs', True) or category.get('capture_html')):
            problems.append(f'results on {site_host(category)} link straight to PDFs: '
                            'download_pdfs must be true and capture_html false')
        if site and site['listing'] == 'sitting_reports' and not category.get('capture_html'):
            problems.append('Hansard sittings are saved as HTML snapshots: capture_html must be true')
        return problems

    @classmethod
    def validate_config(cls, config):
        '''Shared checks, plus: the bill categories need the FS keyword list.'''
        config = super().validate_config(config)
        uses_split = any(c.get('category_id') in FS_SPLIT for c in config.get('categories') or [])
        words = (config.get('title_keyword_sets') or {}).get(FS_KEYWORD_SET)
        if uses_split and not (isinstance(words, list) and words
                               and all(isinstance(w, str) and w.strip() for w in words)):
            raise ValueError(f'Invalid scraper config for {config.get("regulator_id")}: '
                             f'title_keyword_sets.{FS_KEYWORD_SET} must be a non empty list of keywords')
        return config

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

    # Card listings (www.parliament.gov.sg, www.egazette.gov.sg)

    async def _read_listing_page(self, page, url, category):
        '''Open one listing page and poll until it shows dated item links or the no results
        text. Returns {cards, no_results}.'''
        args = {'date_re': _JS_DATE_RE, 'hosts': sorted(item_hosts(category)),
                'entry_key': site_host(category) + urlsplit(category['listing_url']).path.rstrip('/').lower(),
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
        paged = is_paged(category)
        max_pages = int(self.config.get('max_pages_per_category', 20)) if paged else 1
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
            if not paged or (dates and max(dates) < scan_start):
                return cards, None                      # one page, or whole page older than the scan
        raise PermanentError(f'More than {max_pages} listing pages, raise max_pages_per_category',
                             'PAGINATION_LIMIT')

    async def _discover_cards(self, page, category, scan_start, scan_end):
        cards, warning = await self._collect_cards(page, category, scan_start)
        items = [parse_card(c, category, scan_start, scan_end) for c in cards]
        keyword_sets = self.config.get('title_keyword_sets')
        unique, seen, split_out = [], set(), 0
        for item in items:
            if not item or item['url'] in seen:
                continue
            if not fs_split_keeps(item['title'], category, keyword_sets):
                split_out += 1
                continue
            seen.add(item['url'])
            unique.append(item)
        self.logger.info('[%s] %s: %s cards read, %s items in scan period%s%s', category['category_id'],
                         site_of(category)['name'], len(cards), len(unique),
                         f' ({split_out} left to the other bills category)' if split_out else '',
                         ' (listing empty, recorded as 0)' if warning == LISTING_EMPTY_WARNING else '')
        return unique, warning

    # Hansard sittings (sprs.parl.gov.sg)

    async def _read_sitting(self, page, category, day):
        '''The sitting held on day as read from the Hansard service, or None without a sitting.'''
        api = site_base(category) + HANSARD_REPORT_API
        day_text = sitting_day_text(day)

        async def action():
            response = await page.request.post(api, data={'sittingDate': day_text}, timeout=self.nav_timeout)
            try:
                body = await response.json()
            except Exception:
                body = None
            if response.status == 200 and isinstance(body, dict) and body.get('metadata'):
                return body
            if response.status == 500 and isinstance(body, dict) and 'errorCode' in body:
                return None                             # the service's answer for a day without a sitting
            raise RetryableError(f'Hansard service answered HTTP {response.status} for sitting date {day_text}')
        return await self._with_retry(action, f'read Hansard sitting {day_text}')

    async def _discover_sittings(self, page, category, scan_start, scan_end):
        keyword_sets = self.config.get('title_keyword_sets')
        items, day = [], scan_start
        while day <= scan_end:
            report = await self._read_sitting(page, category, day)
            if report:
                item = sitting_item(category, day, report)
                self.sitting_reports[item['url']] = report
                items.append(item)
                fs_titles = fs_section_titles(report, keyword_sets)
                self.logger.info('[%s] Sitting %s: %s sections, %s with FS keywords%s', category['category_id'],
                                 sitting_day_text(day), len(report.get('takesSectionVOList') or []),
                                 len(fs_titles), (': ' + '; '.join(fs_titles[:5])) if fs_titles else '')
            day += timedelta(days=1)
        self.logger.info('[%s] Hansard: %s sittings in scan period', category['category_id'], len(items))
        return items, (None if items else NO_SITTINGS_WARNING)

    async def discover(self, page, category, scan_start, scan_end):
        self.base_url = site_base(category)             # this category's website, see the docstring
        if site_of(category)['listing'] == 'sitting_reports':
            return await self._discover_sittings(page, category, scan_start, scan_end)
        return await self._discover_cards(page, category, scan_start, scan_end)

    # Item processing

    async def _page_body(self, page):
        '''Hansard snapshots come from the service, every other snapshot from the open page.'''
        if self.prepared_body is not None:
            return self.prepared_body
        return await super()._page_body(page)

    async def _process_item(self, page, context, category, item, counters):
        '''Hansard sitting: saved as one HTML snapshot built from the service. Result that is a
        PDF (every eGazette result, any .pdf link): downloaded as the item's only document.
        Other items: the shared flow (open the item page, save its PDFs or an HTML snapshot).'''
        self.base_url = site_base(category)
        site = site_of(category)
        if site['listing'] == 'sitting_reports':
            self.prepared_body = sitting_report_body(self.sitting_reports.pop(item['url'], None) or {})
            try:
                return await self._save_single_document(page, context, category, item, counters, 'HTML', None)
            finally:
                self.prepared_body = None
        if not (site['pdf_only'] or is_pdf(item['url'])):
            return await super()._process_item(page, context, category, item, counters)
        if not category.get('download_pdfs', True):
            self.logger.info('[%s] Skipped PDF item (download_pdfs false): %s', category['category_id'], item['url'])
            return
        haystack = (item['url'] + ' ' + item['title']).lower()
        include = [k.lower() for k in category.get('pdf_include_keywords') or []]
        exclude = [k.lower() for k in category.get('pdf_exclude_keywords') or []]
        if (include and not any(k in haystack for k in include)) or any(k in haystack for k in exclude):
            self.logger.info('[%s] Skipped PDF item (PDF keyword filters): %s', category['category_id'], item['url'])
            return
        return await self._save_single_document(page, context, category, item, counters, 'PDF',
                                                {'url': item['url'], 'text': item['title']})

    async def _save_single_document(self, page, context, category, item, counters, content_format, link):
        '''Save one document as the whole item, with the same ITEM audit row as the shared flow.'''
        item_fields = {
            'category_id': category['category_id'], 'category_name': category['category_name'],
            'item_title': item['title'], 'item_date': item['item_date'],
            'item_date_label': item['item_date_label'], 'item_reference': item['item_reference'],
            'source_url': item['url'],
        }
        before = Counters(**asdict(counters))
        try:
            counters.documents_found += 1
            await self._process_document(page, context, category, item, item_fields, content_format,
                                         link, counters)
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
