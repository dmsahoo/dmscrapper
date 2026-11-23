# Databricks notebook source
# MAGIC %md
# MAGIC # test_playwright_scrape
# MAGIC
# MAGIC | Item | Detail |
# MAGIC |---|---|
# MAGIC | **Purpose** | Smoke test of the scraper's browser setup. Checks, one step at a time, that Edge and Playwright are installed, that the cluster can reach the regulator website over HTTPS on port 443, that Playwright can open the page and find document links, and that one PDF can be downloaded. Each check prints PASS or FAIL, so a failure points to the exact layer: installation, network, browser or download. Changes nothing: no table is written and no file is kept. |
# MAGIC | **Job and task** | None. Run by hand after the init script, or after a firewall change |
# MAGIC | **Parameters** | `target_url` (an https address on port 443, default the MAS website). `timeout_seconds` (5 to 120). Both are checked before use. |
# MAGIC | **Inputs** | The regulator website, the Edge browser installed by `scripts/playwright_init.sh` |
# MAGIC | **Outputs** | PASS or FAIL per check, and a summary table, in the notebook output |
# MAGIC | **Compute** | A classic cluster with the init script `playwright_init.sh` (the same cluster the scraper uses). Not serverless |
# MAGIC | **Dependencies** | Outbound HTTPS (port 443) to the target host, allowed by the firewall. If a proxy is used, `HTTPS_PROXY` must be set on the cluster |

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1: Read and check the parameters
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | **Purpose** | Read the target address and the time limit, and accept only an https address on port 443, so the test can never be pointed at another port or protocol. |
# MAGIC | **Input** | Widgets `target_url`, `timeout_seconds` |
# MAGIC | **Output** | `TARGET_URL`, `HOST`, `TIMEOUT`, and the results list `RESULTS` |

# COMMAND ----------

import os
from urllib.parse import urlparse

dbutils.widgets.text('target_url', 'https://www.mas.gov.sg/regulation/notices')
dbutils.widgets.text('timeout_seconds', '30')

TARGET_URL = dbutils.widgets.get('target_url').strip()
parsed = urlparse(TARGET_URL)
# Only https on port 443 is allowed (no port in the address means 443 for https)
if parsed.scheme != 'https' or parsed.port not in (None, 443) or not parsed.hostname:
    raise ValueError('target_url must be an https address on port 443, for example https://www.mas.gov.sg/')
HOST = parsed.hostname

timeout_text = dbutils.widgets.get('timeout_seconds').strip()
if not timeout_text.isdigit() or not 5 <= int(timeout_text) <= 120:
    raise ValueError('timeout_seconds must be a whole number from 5 to 120')
TIMEOUT = int(timeout_text)

RESULTS = []                                             # one row per check: (check, PASS or FAIL, detail)


# Record and print the result of one check
def record(check, ok, detail):
    RESULTS.append((check, 'PASS' if ok else 'FAIL', str(detail)[:300]))
    print(f"{'PASS' if ok else 'FAIL'}  {check}: {detail}")


print(f'Target {TARGET_URL} | host {HOST} | port 443 | timeout {TIMEOUT}s')
print('Proxy:', os.environ.get('HTTPS_PROXY') or os.environ.get('https_proxy') or 'none')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Check the installation
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | **Purpose** | Check that the init script did its job: the Playwright library can be imported, and the Microsoft Edge browser is installed. If this fails, look at the init script log of the cluster, not the network. |
# MAGIC | **Input** | The cluster, after the init script |
# MAGIC | **Output** | Two results: Playwright library, Edge browser |

# COMMAND ----------

import subprocess
from importlib.metadata import version, PackageNotFoundError

# The Playwright library (installed by the init script)
try:
    record('Playwright library', True, f"version {version('playwright')}")
except PackageNotFoundError:
    record('Playwright library', False, 'not installed: check the init script log')

# The Edge browser (installed by the init script through playwright install msedge)
try:
    edge = subprocess.run(['/usr/bin/microsoft-edge-stable', '--version'],
                          capture_output=True, text=True, timeout=30)
    record('Edge browser', edge.returncode == 0, edge.stdout.strip() or edge.stderr.strip())
except FileNotFoundError:
    record('Edge browser', False, 'microsoft-edge-stable not found: check the init script log')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Check the network on port 443
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | **Purpose** | Without the browser, check the network path to the host: the name resolves (DNS), a connection opens on port 443 (firewall), and the secure HTTPS handshake succeeds (certificate). Uses only the Python standard library. When a proxy is set, the direct connection may be refused by design: then Step 4 shows whether the proxy path works. |
# MAGIC | **Input** | `HOST`, port 443 |
# MAGIC | **Output** | Three results: DNS, TCP port 443, TLS handshake |

# COMMAND ----------

import socket
import ssl

# DNS: can the cluster find the address of the host ?
try:
    addresses = sorted({a[4][0] for a in socket.getaddrinfo(HOST, 443, proto=socket.IPPROTO_TCP)})
    record('DNS', True, f'{HOST} resolves to {addresses[:3]}')
except socket.gaierror as error:
    record('DNS', False, f'{HOST} does not resolve: {error}')

# TCP and TLS: can the cluster open port 443 and complete a secure handshake ?
try:
    with socket.create_connection((HOST, 443), timeout=TIMEOUT) as raw:
        record('TCP port 443', True, f'connection to {HOST}:443 opened')
        with ssl.create_default_context().wrap_socket(raw, server_hostname=HOST) as tls:
            certificate = tls.getpeercert()
            issuer = dict(item[0] for item in certificate.get('issuer', []))
            record('TLS handshake', True, f"{tls.version()}, certificate issued by {issuer.get('organizationName', 'unknown')}")
except socket.timeout:
    record('TCP port 443', False, f'no answer from {HOST}:443 within {TIMEOUT}s: likely blocked by the firewall')
except ssl.SSLError as error:
    record('TLS handshake', False, f'secure handshake failed (certificate or inspection proxy): {error}')
except OSError as error:
    record('TCP port 443', False, f'connection refused or blocked: {error}')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4: Open the page with Playwright and Edge
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | **Purpose** | Do what the scraper does: start Edge without a screen (headless), open the page over HTTPS, and read its title and the document links. Uses the async Playwright API, because a Databricks notebook already runs an event loop. If a proxy is set on the cluster, the browser uses it. |
# MAGIC | **Input** | `TARGET_URL`, `TIMEOUT`, `HTTPS_PROXY` (optional) |
# MAGIC | **Output** | Results: browser start, page open, document links found. `PDF_LINKS` for Step 5 |

# COMMAND ----------

from playwright.async_api import async_playwright

PDF_LINKS = []
proxy = os.environ.get('HTTPS_PROXY') or os.environ.get('https_proxy')

async def open_page():
    async with async_playwright() as p:
        # Start Edge headless, through the cluster proxy when one is set
        browser = await p.chromium.launch(channel='msedge', headless=True,
                                          proxy={'server': proxy} if proxy else None)
        record('Browser start', True, 'Edge started headless')
        try:
            page = await browser.new_page()
            response = await page.goto(TARGET_URL, wait_until='domcontentloaded', timeout=TIMEOUT * 1000)
            status = response.status if response else 'no response'
            record('Page open (HTTPS)', response is not None and response.ok,
                   f'status {status}, title: {await page.title()}')
            # Every link on the page, then the ones that point to a PDF
            links = await page.eval_on_selector_all('a[href]', 'els => els.map(e => e.href)')
            PDF_LINKS.extend(sorted({l for l in links if l.lower().split('?')[0].endswith('.pdf')}))
            record('Links found', len(links) > 0, f'{len(links)} links, {len(PDF_LINKS)} PDF links')
        finally:
            await browser.close()

try:
    await open_page()                                    # top level await works in Databricks notebooks
except Exception as error:
    record('Browser or page', False, f'{type(error).__name__}: {error}')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 5: Download one PDF over HTTPS
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | **Purpose** | Download the first PDF link found, the way the scraper fetches documents, and check that it really is a PDF. The file is only checked in memory and never saved. Skipped when the page has no PDF link (some pages list documents on a deeper page). |
# MAGIC | **Input** | `PDF_LINKS` from Step 4 |
# MAGIC | **Output** | Result: PDF download, with its size |

# COMMAND ----------

# Download one PDF through the browser's own HTTPS client, and check it in memory only
async def download_pdf(url):
    async with async_playwright() as p:
        browser = await p.chromium.launch(channel='msedge', headless=True,
                                          proxy={'server': proxy} if proxy else None)
        try:
            context = await browser.new_context()
            response = await context.request.get(url, timeout=TIMEOUT * 1000)
            body = await response.body()
            is_pdf = body[:5] == b'%PDF-'                   # every PDF file starts with these bytes
            record('PDF download', response.ok and is_pdf,
                   f'status {response.status}, {len(body)} bytes, starts as PDF: {is_pdf}, {url}')
        finally:
            await browser.close()

if not PDF_LINKS:
    print('SKIP  PDF download: no PDF link on this page. Try a page that lists PDF files directly.')
elif urlparse(PDF_LINKS[0]).scheme != 'https' or urlparse(PDF_LINKS[0]).port not in (None, 443):
    record('PDF download', False, f'first PDF link is not https on port 443: {PDF_LINKS[0]}')
else:
    try:
        await download_pdf(PDF_LINKS[0])
    except Exception as error:
        record('PDF download', False, f'{type(error).__name__}: {error}')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 6: Summary
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | **Purpose** | Show all results in one table, and fail the run if any check failed, so a job or a person sees it at once. Read it from the top: the first FAIL is the layer to fix. |
# MAGIC | **Input** | `RESULTS` |
# MAGIC | **Output** | A summary table, and the run status |

# COMMAND ----------

display(spark.createDataFrame(RESULTS, 'check string, result string, detail string'))

failed = [r[0] for r in RESULTS if r[1] == 'FAIL']
if failed:
    raise RuntimeError(f'Playwright scrape test failed at: {failed}. The first one is the layer to fix.')
print('All checks passed: Playwright can scrape over HTTPS on port 443.')
