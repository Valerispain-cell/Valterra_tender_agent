"""
VALTERRA Tender Intelligence Agent v4
Source: Telegram public channels via t.me/s/
Deduplication: hash + fuzzy date/buyer/volume + Claude semantic check
"""

import os, json, time, hashlib, re
from datetime import datetime, timezone, date, timedelta
from typing import Optional

import requests
from bs4 import BeautifulSoup
import gspread
from google.oauth2.service_account import Credentials

# ── CONFIG ────────────────────────────────────────────────────────────
ANTHROPIC_API_KEY  = os.environ["ANTHROPIC_API_KEY"]
GOOGLE_CREDENTIALS = json.loads(os.environ["GOOGLE_CREDENTIALS_JSON"])
SPREADSHEET_ID     = os.environ["SPREADSHEET_ID"]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

# ── TELEGRAM CHANNELS ─────────────────────────────────────────────────
CHANNELS = [
    {"handle": "UkrAgroConsult", "name": "UkrAgroConsult"},
    {"handle": "apk_inform",     "name": "APK-Inform"},
    {"handle": "asap_agri",      "name": "ASAP Agri"},
    {"handle": "zerno_online",   "name": "Zerno Online"},
    {"handle": "latifundist",    "name": "Latifundist"},
]

TENDER_KEYWORDS = [
    "tender", "purchased", "contracted", "no purchase", "bought",
    "tmo", "oaic", "gasc", "mostakbal", "sago", "passco", "egte",
    "jordan", "algeria", "saudi", "ethiopia", "pakistan", "tunisia", "odc",
    "тендер", "закупил", "закупила", "купил", "не купил",
    "египет", "алжир", "иордания", "саудов", "турция", "тмо",
    "пшениц", "ячмен", "кукуруз",
]

# ── DEDUP CONFIG ───────────────────────────────────────────────────────
FUZZY_DATE_WINDOW_DAYS = 3    # same tender if dates within ±3 days
FUZZY_VOLUME_TOLERANCE = 0.10 # same tender if volumes within ±10%


# ── TELEGRAM SCRAPER ──────────────────────────────────────────────────
def fetch_channel_posts(handle: str, n_pages: int = 2) -> list[dict]:
    posts = []
    url   = f"https://t.me/s/{handle}"

    for page in range(n_pages):
        try:
            r = requests.get(url, headers=HEADERS, timeout=15)
            r.raise_for_status()
        except Exception as e:
            print(f"  [WARN] fetch {url}: {e}")
            break

        soup = BeautifulSoup(r.text, "html.parser")
        messages = soup.select(".tgme_widget_message")

        if not messages:
            break

        for msg in messages:
            text_el = msg.select_one(".tgme_widget_message_text")
            if not text_el:
                continue

            # get_text() reads ALL text including tg-spoiler hidden spans
            text = text_el.get_text(separator=" ", strip=True)

            date_el  = msg.select_one(".tgme_widget_message_date time")
            date_str = date_el["datetime"][:10] if date_el and date_el.get("datetime") else ""

            msg_el   = msg.get("data-post", "")
            msg_id   = msg_el.split("/")[-1] if "/" in msg_el else ""

            link_el  = msg.select_one(".tgme_widget_message_date")
            msg_url  = link_el.get("href", "") if link_el else ""

            posts.append({
                "text":   text,
                "date":   date_str,
                "msg_id": msg_id,
                "url":    msg_url,
            })

        if page < n_pages - 1 and posts:
            oldest_id = min(
                (p["msg_id"] for p in posts if p["msg_id"].isdigit()),
                key=int, default=None,
            )
            if oldest_id:
                url = f"https://t.me/s/{handle}?before={oldest_id}"
            else:
                break
        time.sleep(2)

    return posts


def is_tender_post(text: str) -> bool:
    t = text.lower()
    return any(kw in t for kw in TENDER_KEYWORDS)


# ── CLAUDE API ────────────────────────────────────────────────────────
def call_claude(prompt: str, max_tokens: int = 800) -> Optional[str]:
    r = requests.post(
        "https://api.anthropic.com/v1/messages",
        headers={
            "x-api-key":         ANTHROPIC_API_KEY,
            "anthropic-version": "2023-06-01",
            "content-type":      "application/json",
        },
        json={
            "model":      "claude-haiku-4-5-20251001",
            "max_tokens": max_tokens,
            "system": (
                "You are VALTERRA Tender Intelligence Agent. "
                "Output ONLY what is requested — no explanation, no markdown."
            ),
            "messages": [{"role": "user", "content": prompt}],
        },
        timeout=30,
    )
    if r.status_code != 200:
        print(f"  [ERROR] Claude {r.status_code}: {r.text[:200]}")
        return None
    return r.json()["content"][0]["text"].strip()


FILTER_PROMPT = """Is this post about an international grain TENDER from a STATE buyer?
State buyers: GASC, Mostakbal Misr, TMO, OAIC, Jordan MIT, PASSCO, TCP, EGTE, SAGO, Tunisia ODC, Morocco ONICL, Bangladesh, Philippines.
Must be: tender result (purchased/no purchase) OR announcement with volume.
NOT: domestic procurement, price analysis, crop forecasts, logistics, futures.

Reply ONLY "YES" or "NO".

Post: \"\"\"{text}\"\"\""""

EXTRACT_PROMPT = """Extract grain tender data. Return ONLY valid JSON — no markdown.
Use null for unknown fields. Dates as YYYY-MM-DD.

{{
  "date":            "YYYY-MM-DD",
  "buyer":           "",
  "country":         "",
  "type":            "result|announcement",
  "commodity":       "Wheat|Barley|Corn|Sorghum|Soybeans",
  "commodity_spec":  "",
  "volume_sought_t": null,
  "volume_bought_t": null,
  "price_usd_t":     null,
  "price_range":     null,
  "basis":           "CIF|FOB|CFR|C&F",
  "origin":          "",
  "shipment_from":   "YYYY-MM-DD",
  "shipment_to":     "YYYY-MM-DD",
  "winning_trader":  "",
  "other_offers":    [],
  "result":          "purchased|no_purchase|announcement",
  "payment_terms":   "",
  "source_url":      "",
  "confidence":      "high|medium|low"
}}

Rules:
- date = tender date; if unknown use post_date
- buyer = exact name: GASC|Mostakbal Misr|TMO|OAIC|Jordan MIT|PASSCO|TCP|EGTE|SAGO|Tunisia ODC
- price_usd_t = WINNING price only (not average)
- volume_bought_t = actual purchased (often 50% of sought)
- origin "any"/"optional" → "optional"
- other_offers = [{{"trader":"X","price_usd_t":000}}] for each competing bid
- no_purchase: volume_bought_t=0, price_usd_t=null, winning_trader=null
- tg-spoiler text IS visible — extract all numbers you see

Post date: {post_date}
Source URL: {url}
Post: \"\"\"{text}\"\"\""""

DEDUP_PROMPT = """You are checking if two grain tender records refer to the SAME tender event.

Existing record:
{existing}

New record:
{new_record}

Same tender = same buyer + same commodity + similar date (within 5 days) + similar volume (within 15%).
Different tender = different shipment period OR clearly different volume OR weeks apart.

Reply ONLY "DUPLICATE" or "NEW"."""


def classify(text: str) -> bool:
    answer = call_claude(FILTER_PROMPT.format(text=text[:1500]), max_tokens=3)
    return (answer or "").strip().upper() == "YES"


def extract(text: str, post_date: str, url: str) -> Optional[dict]:
    raw = call_claude(
        EXTRACT_PROMPT.format(text=text[:3000], post_date=post_date, url=url),
        max_tokens=900,
    )
    if not raw:
        return None
    raw = re.sub(r"```(?:json)?|```", "", raw).strip()
    try:
        data = json.loads(raw)
        if not data.get("buyer") or not data.get("commodity"):
            return None
        if (data.get("confidence") == "low"
                and not data.get("price_usd_t")
                and not data.get("volume_sought_t")):
            return None
        return data
    except json.JSONDecodeError as e:
        print(f"  [WARN] JSON: {e} | {raw[:100]}")
        return None


# ── DEDUPLICATION ─────────────────────────────────────────────────────
def make_hash(tender_date: str, buyer: str, commodity: str, volume) -> str:
    """Level 1: exact hash."""
    key = f"{tender_date}|{buyer}|{commodity}|{volume or 0}"
    return hashlib.md5(key.encode()).hexdigest()[:12]


def fuzzy_match(new: dict, existing_records: list[dict]) -> Optional[dict]:
    """
    Level 2: fuzzy match.
    Returns the existing record if it's likely the same tender, else None.
    Checks: same buyer + commodity, date within ±FUZZY_DATE_WINDOW_DAYS,
    volume within ±FUZZY_VOLUME_TOLERANCE.
    """
    new_buyer     = (new.get("buyer") or "").lower()
    new_commodity = (new.get("commodity") or "").lower()
    new_vol       = new.get("volume_sought_t") or new.get("volume_bought_t")
    new_date_str  = new.get("date", "")

    try:
        new_date = date.fromisoformat(new_date_str) if new_date_str else None
    except ValueError:
        new_date = None

    for rec in existing_records:
        # Must match buyer and commodity
        if (rec.get("buyer", "").lower() != new_buyer or
                rec.get("commodity", "").lower() != new_commodity):
            continue

        # Date proximity check
        if new_date:
            try:
                rec_date = date.fromisoformat(rec.get("date", ""))
                if abs((new_date - rec_date).days) > FUZZY_DATE_WINDOW_DAYS:
                    continue
            except ValueError:
                pass

        # Volume proximity check
        rec_vol = rec.get("volume_sought_t") or rec.get("volume_bought_t")
        if new_vol and rec_vol:
            try:
                nv, rv = float(new_vol), float(rec_vol)
                if rv > 0 and abs(nv - rv) / rv > FUZZY_VOLUME_TOLERANCE:
                    continue
            except (TypeError, ValueError):
                pass

        return rec  # fuzzy match found

    return None


def claude_dedup(new: dict, existing: dict) -> bool:
    """
    Level 3: Claude semantic check for ambiguous cases.
    Returns True if DUPLICATE.
    """
    answer = call_claude(
        DEDUP_PROMPT.format(
            existing=json.dumps(existing, ensure_ascii=False),
            new_record=json.dumps(new, ensure_ascii=False),
        ),
        max_tokens=10,
    )
    return (answer or "").strip().upper() == "DUPLICATE"


# ── GOOGLE SHEETS ─────────────────────────────────────────────────────
COLUMNS = [
    "id", "scraped_at", "post_date", "date", "buyer", "country", "type",
    "commodity", "commodity_spec", "volume_sought_t", "volume_bought_t",
    "price_usd_t", "price_range", "basis", "origin",
    "shipment_from", "shipment_to", "winning_trader", "other_offers",
    "result", "payment_terms", "channel", "source_url", "confidence",
    "raw_text",  # original post text for manual verification
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
    try:
        ws = sh.worksheet("raw_tenders")
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet("raw_tenders", rows=5000, cols=len(COLUMNS))
        ws.append_row(COLUMNS)
        ws.format("1:1", {"textFormat": {"bold": True}})
        ws.freeze(rows=1)
    return ws


def load_existing(ws) -> tuple[set, list[dict]]:
    """
    Returns:
      - set of existing hashes (for Level 1 dedup)
      - list of recent records as dicts (for Level 2/3 dedup)
    """
    try:
        rows = ws.get_all_records()
    except Exception:
        return set(), []

    hashes  = {r.get("id", "") for r in rows if r.get("id")}

    # Only keep records from last 30 days for fuzzy matching
    cutoff = (datetime.now(timezone.utc) - timedelta(days=30)).strftime("%Y-%m-%d")
    recent = [
        r for r in rows
        if (r.get("date") or r.get("post_date") or "") >= cutoff
    ]

    return hashes, recent


def save_record(ws, data: dict, post_date: str, post_text: str,
                channel_name: str, existing_hashes: set) -> bool:
    rid = make_hash(
        data.get("date", post_date),
        data.get("buyer", ""),
        data.get("commodity", ""),
        data.get("volume_sought_t"),
    )

    row = [
        rid,
        datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        post_date,
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
        data.get("origin", ""),
        data.get("shipment_from", ""),
        data.get("shipment_to", ""),
        data.get("winning_trader", ""),
        json.dumps(data.get("other_offers") or [], ensure_ascii=False),
        data.get("result", ""),
        data.get("payment_terms", ""),
        channel_name,
        data.get("source_url", ""),
        data.get("confidence", ""),
        post_text[:500],  # raw_text truncated to 500 chars
    ]

    ws.append_row(row, value_input_option="USER_ENTERED")
    existing_hashes.add(rid)
    return True


# ── MAIN ──────────────────────────────────────────────────────────────
def run():
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    print(f"\n{'='*60}")
    print(f"VALTERRA Tender Agent v4 — {ts}")
    print(f"{'='*60}\n")

    ws                      = get_sheet()
    existing_hashes, recent = load_existing(ws)

    print(f"Records in sheet: {len(existing_hashes)} | Recent (30d): {len(recent)}\n")

    stats = {"posts": 0, "keyword": 0, "classified": 0, "extracted": 0,
             "added": 0, "dup_hash": 0, "dup_fuzzy": 0, "dup_claude": 0}

    for ch in CHANNELS:
        handle = ch["handle"]
        name   = ch["name"]
        print(f"── @{handle} ({name}) ──────────────")

        posts = fetch_channel_posts(handle, n_pages=2)
        stats["posts"] += len(posts)
        print(f"  Posts fetched: {len(posts)}")

        candidates = [p for p in posts if is_tender_post(p["text"])]
        stats["keyword"] += len(candidates)
        print(f"  Keyword match: {len(candidates)}")

        for post in candidates:
            text      = post["text"]
            post_date = post["date"]
            url       = post["url"]

            # ── Level 1: classify ──────────────────────────────────
            if not classify(text):
                continue
            stats["classified"] += 1

            snippet = text[:65].replace("\n", " ")
            print(f"  ✓ {snippet}...")

            # ── Extract ────────────────────────────────────────────
            time.sleep(0.5)
            data = extract(text, post_date, url)
            if not data:
                print(f"    → extraction failed")
                continue
            stats["extracted"] += 1

            # ── Level 2: hash dedup ────────────────────────────────
            rid = make_hash(
                data.get("date", post_date),
                data.get("buyer", ""),
                data.get("commodity", ""),
                data.get("volume_sought_t"),
            )
            if rid in existing_hashes:
                stats["dup_hash"] += 1
                print(f"    → duplicate (hash)")
                continue

            # ── Level 3: fuzzy dedup ───────────────────────────────
            fuzzy = fuzzy_match(data, recent)
            if fuzzy:
                # ── Level 4: Claude semantic check for ambiguous ───
                time.sleep(0.3)
                is_dup = claude_dedup(data, fuzzy)
                if is_dup:
                    stats["dup_claude"] += 1
                    print(f"    → duplicate (Claude semantic)")
                    continue
                else:
                    print(f"    → fuzzy match but Claude says NEW — saving")

            # ── Save ───────────────────────────────────────────────
            save_record(ws, data, post_date, text, name, existing_hashes)
            recent.append(data)  # add to in-memory list for this run
            stats["added"] += 1

            print(
                f"    → ADDED: {data.get('buyer')} | "
                f"{data.get('commodity')} | "
                f"${data.get('price_usd_t')} {data.get('basis')} | "
                f"{data.get('result')} | conf={data.get('confidence')}"
            )
            time.sleep(1)

        print()

    # ── Summary ────────────────────────────────────────────────────────
    print(f"{'='*60}")
    print(f"Posts scanned : {stats['posts']}")
    print(f"Keyword match : {stats['keyword']}")
    print(f"Classified    : {stats['classified']}")
    print(f"Extracted     : {stats['extracted']}")
    print(f"  Dup (hash)  : {stats['dup_hash']}")
    print(f"  Dup (Claude): {stats['dup_claude']}")
    print(f"  ADDED       : {stats['added']}")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    run()
