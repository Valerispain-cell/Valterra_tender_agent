"""
VALTERRA Tender Intelligence Agent
Runs 2x daily via GitHub Actions. Scrapes grain news sources,
extracts tender data via Claude API, appends to Google Sheets.
"""

import os, json, time, hashlib, re
from datetime import datetime, timezone
from typing import Optional

import requests
from bs4 import BeautifulSoup
import gspread
from google.oauth2.service_account import Credentials

# ── CONFIG ────────────────────────────────────────────────────────────
ANTHROPIC_API_KEY = os.environ["ANTHROPIC_API_KEY"]
GOOGLE_CREDENTIALS = json.loads(os.environ["GOOGLE_CREDENTIALS_JSON"])
SPREADSHEET_ID = os.environ["SPREADSHEET_ID"]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; VALTERRA-Bot/1.0; grain market research)",
    "Accept": "text/html,application/xhtml+xml",
    "Accept-Language": "en-US,en;q=0.9",
}

# Target buyers — used in filter prompt and dedup
TARGET_BUYERS = [
    "GASC", "Mostakbal Misr", "TMO", "OAIC", "Jordan MIT",
    "PASSCO", "TCP", "EGTE", "SAGO", "Tunisia", "Morocco",
    "Bangladesh", "Philippines", "Saudi"
]

FILTER_KEYWORDS = [
    "tender", "purchase", "buy", "bought", "contracted", "no purchase",
    "tmo", "oaic", "gasc", "mostakbal", "jordan", "passco", "egte", "sago",
    "algeria", "saudi", "ethiopia", "pakistan",
    "wheat", "barley", "corn", "sorghum"
]

# ── SOURCES ───────────────────────────────────────────────────────────
SOURCES = [
    {
        "name": "UkrAgroConsult",
        "url": "https://ukragroconsult.com/en/news/",
        "article_selector": "article a, .post-title a, h2 a, h3 a",
        "base_url": "https://ukragroconsult.com",
        "content_selector": ".entry-content, .post-content, article",
    },
    {
        "name": "APK-Inform",
        "url": "https://www.apk-inform.com/en/news",
        "article_selector": ".news-item a, .title a, h2 a, h3 a",
        "base_url": "https://www.apk-inform.com",
        "content_selector": ".news-text, .article-body, article",
    },
    {
        "name": "GrainCentral",
        "url": "https://www.graincentral.com/markets/feed",
        "is_rss": True,
    },
    {
        "name": "WorldGrain",
        "url": "https://www.world-grain.com/rss/grain-market-news",
        "is_rss": True,
    },
]


# ── SCRAPING ──────────────────────────────────────────────────────────
def fetch_html(url: str, timeout: int = 15) -> Optional[str]:
    try:
        r = requests.get(url, headers=HEADERS, timeout=timeout)
        r.raise_for_status()
        return r.text
    except Exception as e:
        print(f"  [WARN] fetch failed {url}: {e}")
        return None


def extract_article_links(html: str, source: dict) -> list[dict]:
    """Extract article links from news index page."""
    soup = BeautifulSoup(html, "html.parser")
    links = []
    seen = set()
    for a in soup.select(source["article_selector"]):
        href = a.get("href", "")
        if not href:
            continue
        if not href.startswith("http"):
            href = source["base_url"] + href
        if href in seen:
            continue
        seen.add(href)
        title = a.get_text(strip=True)
        # Quick pre-filter on title
        title_lower = title.lower()
        if any(kw in title_lower for kw in FILTER_KEYWORDS):
            links.append({"url": href, "title": title})
    return links[:20]  # max 20 articles per source per run


def parse_rss(xml: str, source_name: str) -> list[dict]:
    """Parse RSS feed, return pre-filtered items."""
    soup = BeautifulSoup(xml, "xml")
    items = []
    for item in soup.find_all("item")[:30]:
        title = item.find("title")
        link = item.find("link")
        desc = item.find("description")
        if not title or not link:
            continue
        title_text = title.get_text(strip=True)
        desc_text = desc.get_text(strip=True) if desc else ""
        combined = (title_text + " " + desc_text).lower()
        if any(kw in combined for kw in FILTER_KEYWORDS):
            items.append({
                "url": link.get_text(strip=True),
                "title": title_text,
                "text": desc_text,
                "source": source_name,
            })
    return items


def extract_article_text(html: str, selector: str) -> str:
    """Extract main text from article page."""
    soup = BeautifulSoup(html, "html.parser")
    # Try selector first
    content = soup.select_one(selector)
    if content:
        return content.get_text(separator=" ", strip=True)[:3000]
    # Fallback: largest text block
    paragraphs = soup.find_all("p")
    text = " ".join(p.get_text(strip=True) for p in paragraphs)
    return text[:3000]


# ── CLAUDE API ────────────────────────────────────────────────────────
def call_claude(prompt: str, max_tokens: int = 500) -> Optional[str]:
    """Call Claude API, return text response."""
    r = requests.post(
        "https://api.anthropic.com/v1/messages",
        headers={
            "x-api-key": ANTHROPIC_API_KEY,
            "anthropic-version": "2023-06-01",
            "content-type": "application/json",
        },
        json={
            "model": "claude-sonnet-4-20250514",
            "max_tokens": max_tokens,
            "system": (
                "You are VALTERRA Tender Intelligence Agent. "
                "Monitor grain news and extract tender data for commodity brokerage. "
                "Output only what is asked — no explanation, no markdown."
            ),
            "messages": [{"role": "user", "content": prompt}],
        },
        timeout=30,
    )
    if r.status_code != 200:
        print(f"  [ERROR] Claude API: {r.status_code} {r.text[:200]}")
        return None
    return r.json()["content"][0]["text"].strip()


FILTER_PROMPT = """You are a grain market news classifier.

Classify if this news article is about a COMPLETED or ANNOUNCED 
international grain tender from a STATE buyer.

Return ONLY: "YES" or "NO"

Rules for YES:
- Mentions a state grain buyer (GASC, Mostakbal Misr, TMO, OAIC, 
  Jordan MIT, PASSCO, TCP, EGTE, SAGO, Saudi, Algeria, Tunisia, 
  Morocco, Bangladesh, Philippines, Iran)
- About a tender result (purchased, bought, contracted, no purchase, 
  failed) OR new tender announcement with volume + price
- Involves physical grain (wheat, barley, corn, sorghum, soybeans)

Rules for NO:
- Domestic procurement from local farmers
- Price forecasts or analysis without specific tender
- Logistics, weather, crop reports
- Futures / financial instruments

Article:
\"\"\"{text}\"\"\"
"""

EXTRACT_PROMPT = """You are a grain market data extraction specialist.
Extract structured data from this grain tender news article.

Return ONLY valid JSON. No explanation, no markdown, no extra text.
If a field cannot be determined with confidence, use null.

JSON structure:
{{
  "date": "YYYY-MM-DD",
  "buyer": "",
  "country": "",
  "type": "result or announcement",
  "commodity": "",
  "commodity_spec": "",
  "volume_sought_t": null,
  "volume_bought_t": null,
  "price_usd_t": null,
  "price_range": null,
  "basis": "",
  "port": "",
  "origin": "",
  "shipment_from": "YYYY-MM-DD",
  "shipment_to": "YYYY-MM-DD",
  "winning_trader": "",
  "other_offers": [],
  "result": "purchased or no_purchase or announcement",
  "payment_terms": "",
  "source_url": "",
  "confidence": "high or medium or low"
}}

Rules:
- date: when tender was HELD, not article published
- buyer: use exact names: GASC | Mostakbal Misr | TMO | OAIC | Jordan MIT | PASSCO | TCP | EGTE | SAGO
- volume_bought_t: often half of volume_sought_t (e.g. sought 120k, bought 60k)
- price_usd_t: WINNING price only, not average of all offers
- origin "optional" or "any origin" → use "optional"
- no_purchase: volume_bought_t=0, price_usd_t=null, winning_trader=null
- confidence: high=all key fields present, medium=price or volume missing, low=buyer+commodity only

Article:
\"\"\"{text}\"\"\"

Source URL: {url}
"""


def is_tender_article(text: str) -> bool:
    answer = call_claude(FILTER_PROMPT.format(text=text[:2000]), max_tokens=5)
    return answer == "YES" if answer else False


def extract_tender(text: str, url: str) -> Optional[dict]:
    raw = call_claude(EXTRACT_PROMPT.format(text=text[:2500], url=url), max_tokens=800)
    if not raw:
        return None
    # Clean potential markdown fences
    raw = re.sub(r"```(?:json)?", "", raw).strip()
    try:
        data = json.loads(raw)
        # Basic validation
        if not data.get("buyer") or not data.get("commodity"):
            return None
        if data.get("confidence") == "low":
            return None
        return data
    except json.JSONDecodeError as e:
        print(f"  [WARN] JSON parse error: {e} | raw: {raw[:200]}")
        return None


# ── GOOGLE SHEETS ─────────────────────────────────────────────────────
SHEET_COLUMNS = [
    "id", "scraped_at", "date", "buyer", "country", "type",
    "commodity", "commodity_spec",
    "volume_sought_t", "volume_bought_t",
    "price_usd_t", "price_range", "basis", "port", "origin",
    "shipment_from", "shipment_to",
    "winning_trader", "other_offers",
    "result", "payment_terms",
    "source_name", "source_url", "confidence"
]


def get_sheet():
    creds = Credentials.from_service_account_info(
        GOOGLE_CREDENTIALS,
        scopes=[
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/drive",
        ],
    )
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(SPREADSHEET_ID)

    # Ensure raw_tenders sheet exists
    try:
        ws = sh.worksheet("raw_tenders")
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet("raw_tenders", rows=5000, cols=len(SHEET_COLUMNS))
        ws.append_row(SHEET_COLUMNS)
        ws.format("1:1", {"textFormat": {"bold": True}})
    return ws


def make_row_id(date: str, buyer: str, commodity: str, volume: Optional[int]) -> str:
    key = f"{date}|{buyer}|{commodity}|{volume or 0}"
    return hashlib.md5(key.encode()).hexdigest()[:12]


def get_existing_ids(ws) -> set:
    try:
        ids = ws.col_values(1)  # column A = id
        return set(ids[1:])     # skip header
    except Exception:
        return set()


def append_tender(ws, data: dict, source_name: str, existing_ids: set) -> bool:
    row_id = make_row_id(
        data.get("date", ""),
        data.get("buyer", ""),
        data.get("commodity", ""),
        data.get("volume_sought_t"),
    )
    if row_id in existing_ids:
        return False  # duplicate

    other_offers_str = json.dumps(data.get("other_offers") or [])
    row = [
        row_id,
        datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        data.get("date", ""),
        data.get("buyer", ""),
        data.get("country", ""),
        data.get("type", ""),
        data.get("commodity", ""),
        data.get("commodity_spec", ""),
        data.get("volume_sought_t", ""),
        data.get("volume_bought_t", ""),
        data.get("price_usd_t", ""),
        data.get("price_range", ""),
        data.get("basis", ""),
        data.get("port", ""),
        data.get("origin", ""),
        data.get("shipment_from", ""),
        data.get("shipment_to", ""),
        data.get("winning_trader", ""),
        other_offers_str,
        data.get("result", ""),
        data.get("payment_terms", ""),
        source_name,
        data.get("source_url", ""),
        data.get("confidence", ""),
    ]
    ws.append_row(row, value_input_option="USER_ENTERED")
    existing_ids.add(row_id)
    return True


# ── MAIN ──────────────────────────────────────────────────────────────
def run():
    print(f"\n{'='*60}")
    print(f"VALTERRA Tender Agent — {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"{'='*60}\n")

    ws = get_sheet()
    existing_ids = get_existing_ids(ws)
    print(f"Existing records in sheet: {len(existing_ids)}\n")

    total_found = 0
    total_added = 0

    for source in SOURCES:
        print(f"── Source: {source['name']} ──────────────────")
        html = fetch_html(source["url"])
        if not html:
            continue

        # Get articles
        if source.get("is_rss"):
            articles = parse_rss(html, source["name"])
        else:
            links = extract_article_links(html, source)
            articles = []
            for link in links:
                time.sleep(1.5)  # polite delay
                article_html = fetch_html(link["url"])
                if article_html:
                    text = extract_article_text(
                        article_html, source["content_selector"]
                    )
                    articles.append({
                        "url": link["url"],
                        "title": link["title"],
                        "text": text,
                        "source": source["name"],
                    })

        print(f"  Articles to check: {len(articles)}")

        for article in articles:
            text = article.get("text") or article.get("title", "")
            url = article["url"]

            # Step 1: filter
            if not is_tender_article(text):
                continue

            total_found += 1
            print(f"  ✓ Tender article: {article['title'][:60]}...")

            # Step 2: extract
            time.sleep(1)
            data = extract_tender(text, url)
            if not data:
                print(f"    → extraction failed or low confidence")
                continue

            # Step 3: save
            added = append_tender(ws, data, source["name"], existing_ids)
            if added:
                total_added += 1
                print(f"    → ADDED: {data.get('buyer')} | {data.get('commodity')} | "
                      f"{data.get('price_usd_t')} {data.get('basis')} | "
                      f"{data.get('result')}")
            else:
                print(f"    → DUPLICATE, skipped")

            time.sleep(1)

        print()

    print(f"{'='*60}")
    print(f"Run complete. Found: {total_found} | Added: {total_added}")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    run()
