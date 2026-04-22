"""
VALTERRA Tender Intelligence Agent v2
Uses Google News RSS — не блокируется серверами GitHub Actions.
"""

import os, json, time, hashlib, re
from datetime import datetime, timezone
from typing import Optional

import requests
from bs4 import BeautifulSoup
import gspread
from google.oauth2.service_account import Credentials

ANTHROPIC_API_KEY = os.environ["ANTHROPIC_API_KEY"]
GOOGLE_CREDENTIALS = json.loads(os.environ["GOOGLE_CREDENTIALS_JSON"])
SPREADSHEET_ID = os.environ["SPREADSHEET_ID"]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

FILTER_KEYWORDS = [
    "tender", "purchase", "buy", "bought", "contracted", "no purchase",
    "tmo", "oaic", "gasc", "mostakbal", "jordan", "passco", "egte", "sago",
    "algeria", "saudi", "ethiopia", "pakistan", "wheat", "barley", "corn", "grain"
]

SOURCES = [
    {"name": "GNews_GASC",     "url": "https://news.google.com/rss/search?q=GASC+wheat+tender+OR+Mostakbal+Misr+wheat&hl=en&gl=US&ceid=US:en",       "is_rss": True},
    {"name": "GNews_TMO",      "url": "https://news.google.com/rss/search?q=TMO+Turkey+grain+tender+wheat+barley&hl=en&gl=US&ceid=US:en",              "is_rss": True},
    {"name": "GNews_OAIC",     "url": "https://news.google.com/rss/search?q=OAIC+Algeria+wheat+tender&hl=en&gl=US&ceid=US:en",                         "is_rss": True},
    {"name": "GNews_Jordan",   "url": "https://news.google.com/rss/search?q=Jordan+grain+tender+wheat+barley+purchased&hl=en&gl=US&ceid=US:en",        "is_rss": True},
    {"name": "GNews_SAGO",     "url": "https://news.google.com/rss/search?q=SAGO+Saudi+Arabia+wheat+barley+tender&hl=en&gl=US&ceid=US:en",             "is_rss": True},
    {"name": "GNews_Pakistan", "url": "https://news.google.com/rss/search?q=Pakistan+PASSCO+TCP+wheat+tender+import&hl=en&gl=US&ceid=US:en",           "is_rss": True},
    {"name": "GNews_Ethiopia", "url": "https://news.google.com/rss/search?q=Ethiopia+EGTE+wheat+tender&hl=en&gl=US&ceid=US:en",                        "is_rss": True},
]


def fetch_html(url: str, timeout: int = 15) -> Optional[str]:
    try:
        r = requests.get(url, headers=HEADERS, timeout=timeout)
        r.raise_for_status()
        return r.text
    except Exception as e:
        print(f"  [WARN] fetch failed {url[:80]}: {e}")
        return None


def extract_article_text(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style", "nav", "footer", "header"]):
        tag.decompose()
    paragraphs = soup.find_all("p")
    text = " ".join(p.get_text(strip=True) for p in paragraphs)
    return text[:3000]


def parse_rss(xml: str, source_name: str) -> list[dict]:
    soup = BeautifulSoup(xml, "xml")
    items = []
    seen_urls = set()

    for item in soup.find_all("item")[:20]:
        title_el = item.find("title")
        link_el = item.find("link")
        desc_el = item.find("description")
        if not title_el:
            continue

        title_text = title_el.get_text(strip=True)
        url = ""
        if link_el:
            url = link_el.get_text(strip=True)
            if not url and link_el.next_sibling:
                url = str(link_el.next_sibling).strip()

        if not url or url in seen_urls:
            continue
        seen_urls.add(url)

        desc_text = desc_el.get_text(strip=True) if desc_el else ""
        combined = (title_text + " " + desc_text).lower()
        if not any(kw in combined for kw in FILTER_KEYWORDS):
            continue

        time.sleep(1.5)
        article_html = fetch_html(url)
        full_text = extract_article_text(article_html) if article_html else ""
        text = full_text or desc_text or title_text

        items.append({"url": url, "title": title_text, "text": text, "source": source_name})

    return items


def call_claude(prompt: str, max_tokens: int = 500) -> Optional[str]:
    r = requests.post(
        "https://api.anthropic.com/v1/messages",
        headers={"x-api-key": ANTHROPIC_API_KEY, "anthropic-version": "2023-06-01", "content-type": "application/json"},
        json={
            "model": "claude-sonnet-4-20250514",
            "max_tokens": max_tokens,
            "system": "You are VALTERRA Tender Intelligence Agent. Output only what is asked — no explanation, no markdown.",
            "messages": [{"role": "user", "content": prompt}],
        },
        timeout=30,
    )
    if r.status_code != 200:
        print(f"  [ERROR] Claude API: {r.status_code}")
        return None
    return r.json()["content"][0]["text"].strip()


FILTER_PROMPT = """Classify if this news article is about a COMPLETED or ANNOUNCED international grain tender from a STATE buyer.
Return ONLY: "YES" or "NO"

YES if: state grain buyer (GASC, Mostakbal Misr, TMO, OAIC, Jordan MIT, PASSCO, TCP, EGTE, SAGO, Saudi, Algeria) + tender result or announcement + physical grain.
NO if: domestic procurement, price analysis, weather, futures.

Article: \"\"\"{text}\"\"\""""

EXTRACT_PROMPT = """Extract grain tender data. Return ONLY valid JSON, no markdown.

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

Rules: date=tender date (not publish date), buyer=exact name (GASC|TMO|OAIC|Jordan MIT|SAGO|PASSCO|TCP|EGTE), price_usd_t=winning price only, origin "any"="optional", confidence high=all fields present.

Article: \"\"\"{text}\"\"\"
Source URL: {url}"""


def is_tender_article(text: str) -> bool:
    answer = call_claude(FILTER_PROMPT.format(text=text[:2000]), max_tokens=5)
    return answer == "YES" if answer else False


def extract_tender(text: str, url: str) -> Optional[dict]:
    raw = call_claude(EXTRACT_PROMPT.format(text=text[:2500], url=url), max_tokens=800)
    if not raw:
        return None
    raw = re.sub(r"```(?:json)?|```", "", raw).strip()
    try:
        data = json.loads(raw)
        if not data.get("buyer") or not data.get("commodity"):
            return None
        if data.get("confidence") == "low":
            return None
        return data
    except json.JSONDecodeError as e:
        print(f"  [WARN] JSON parse: {e} | {raw[:100]}")
        return None


SHEET_COLUMNS = ["id","scraped_at","date","buyer","country","type","commodity","commodity_spec",
                 "volume_sought_t","volume_bought_t","price_usd_t","price_range","basis","origin",
                 "shipment_from","shipment_to","winning_trader","other_offers","result",
                 "payment_terms","source_name","source_url","confidence"]


def get_sheet():
    creds = Credentials.from_service_account_info(GOOGLE_CREDENTIALS,
        scopes=["https://spreadsheets.google.com/feeds","https://www.googleapis.com/auth/drive"])
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(SPREADSHEET_ID)
    try:
        ws = sh.worksheet("raw_tenders")
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet("raw_tenders", rows=5000, cols=len(SHEET_COLUMNS))
        ws.append_row(SHEET_COLUMNS)
        ws.format("1:1", {"textFormat": {"bold": True}})
    return ws


def make_row_id(date, buyer, commodity, volume):
    return hashlib.md5(f"{date}|{buyer}|{commodity}|{volume or 0}".encode()).hexdigest()[:12]


def get_existing_ids(ws) -> set:
    try:
        return set(ws.col_values(1)[1:])
    except Exception:
        return set()


def append_tender(ws, data: dict, source_name: str, existing_ids: set) -> bool:
    row_id = make_row_id(data.get("date",""), data.get("buyer",""), data.get("commodity",""), data.get("volume_sought_t"))
    if row_id in existing_ids:
        return False
    row = [row_id, datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
           data.get("date",""), data.get("buyer",""), data.get("country",""), data.get("type",""),
           data.get("commodity",""), data.get("commodity_spec",""), data.get("volume_sought_t",""),
           data.get("volume_bought_t",""), data.get("price_usd_t",""), data.get("price_range",""),
           data.get("basis",""), data.get("origin",""), data.get("shipment_from",""),
           data.get("shipment_to",""), data.get("winning_trader",""),
           json.dumps(data.get("other_offers") or []), data.get("result",""),
           data.get("payment_terms",""), source_name, data.get("source_url",""), data.get("confidence","")]
    ws.append_row(row, value_input_option="USER_ENTERED")
    existing_ids.add(row_id)
    return True


def run():
    print(f"\n{'='*60}")
    print(f"VALTERRA Tender Agent v2 — {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"{'='*60}\n")

    ws = get_sheet()
    existing_ids = get_existing_ids(ws)
    print(f"Existing records: {len(existing_ids)}\n")

    total_found = total_added = 0

    for source in SOURCES:
        print(f"── {source['name']} ──────────────")
        html = fetch_html(source["url"])
        if not html:
            continue

        articles = parse_rss(html, source["name"])
        print(f"  Articles to check: {len(articles)}")

        for article in articles:
            text = article.get("text") or article.get("title", "")
            url = article["url"]

            if not is_tender_article(text):
                continue

            total_found += 1
            print(f"  ✓ {article['title'][:60]}...")

            time.sleep(1)
            data = extract_tender(text, url)
            if not data:
                print(f"    → extraction failed")
                continue

            added = append_tender(ws, data, source["name"], existing_ids)
            if added:
                total_added += 1
                print(f"    → ADDED: {data.get('buyer')} | {data.get('commodity')} | ${data.get('price_usd_t')} {data.get('basis')} | {data.get('result')}")
            else:
                print(f"    → duplicate")
            time.sleep(1)
        print()

    print(f"{'='*60}")
    print(f"Done. Found: {total_found} | Added: {total_added}")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    run()
