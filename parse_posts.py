import re
import duckdb

# baby regex 
PRICE_RE = re.compile(r'(?i)(?:\$\s*|usd\s*)([0-9]{2,5}(?:\.[0-9]{1,2})?)')
SOLD_RE = re.compile(r'(?i)\b(sold|pending)\b')
URL_RE = re.compile(r'(?i)\bhttps?://\S+|\bimgur\.com/\S+')

# junk words that appear all over titles/bodies I know are bad - really need a better approach
STOPWORDS = {
    "paypal", "pp", "conus", "timestamp", "timestamps", "shipping", "shipped",
    "obo", "wts", "wtt", "w", "h", "lf", "looking", "looking for",
    "price", "pricing", "asking", "bundle", "bundles", "each",
    "pm", "comment", "before", "trade", "trades", "sold", "pending"
}

# If item_key becomes one of these, it’s not a real item
BAD_KEYS = {
    "asking", "price", "pricing", "bundle", "looking", "looking for",
    "paypal", "shipping", "shipped", "timestamps", "timestamp", "original",
    "base", "switches"
}
# maybe keep? further expanison
SECTION_HINTS = ["keyboards", "keycaps", "switches", "deskmat", "deskmats", "misc"]


# Helpers

def normalize_text(s: str) -> str:
    s = (s or "").strip()
    s = URL_RE.sub(" ", s)
    s = s.replace("\u00a0", " ")  # nonbreaking space
    s = re.sub(r"\s+", " ", s).strip()
    return s

def is_bundleish(line: str) -> bool:
    l = line.lower()
    return any(k in l for k in ["bundle", "take all", "all for", "for everything", "everything for", "lot for"])

def is_garbage_line(line: str) -> bool:
    l = line.lower().strip()
    if not l:
        return True
    if URL_RE.search(l):
        return True
    # if it's mostly punctuation
    if len(re.sub(r"[a-z0-9]", "", l)) > len(l) * 0.75:
        return True
    return False

def clean_item_candidate(s: str) -> str:
    """
    Remove common markup and noise, return short-ish candidate name.
    """
    s = normalize_text(s)

    # remove markdown formatting
    s = s.replace("~~", "")  # strikethrough markers
    s = re.sub(r"^[\-\*\u2022]+\s*", "", s)  # bullets -,*,•
    s = s.strip(" |:-\t")

    # remove sold/pend tags
    s = SOLD_RE.sub("", s)

    # remove leftover price fragments
    s = re.sub(r"(?i)\b(?:usd)\b", " ", s)
    s = re.sub(r"\$\s*[0-9]{2,5}(?:\.[0-9]{1,2})?", " ", s)
    s = re.sub(r"\s+", " ", s).strip()

    # don’t let it get huge
    if len(s) > 120:
        s = s[:120].rstrip()

    return s

def make_item_key(name: str) -> str:
    """
    Normalize into a grouping key. Keep letters/numbers.
    """
    s = (name or "").lower()

    # remove country style tags if they sneak in
    s = re.sub(r"\[[^\]]*\]", " ", s)

    # remove obvious noise words
    for w in STOPWORDS:
        s = re.sub(rf"\b{re.escape(w)}\b", " ", s)

    # keep only alnum
    s = re.sub(r"[^a-z0-9]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()

    return s

def looks_like_real_item(item_name: str, item_key: str) -> bool:
    """
    Filter out junk extractions.
    """
    if not item_name or not item_key:
        return False
    if len(item_key) < 3:
        return False
    if item_key in BAD_KEYS:
        return False
    if URL_RE.search(item_name.lower()):
        return False

    # if it’s basically a sentence- junk
    if len(item_key.split()) > 10:
        return False

    # if it's just one generic word
    if item_key in {"original", "base", "switches", "keycaps"}:
        return False

    return True

def extract_item_from_price_line(line: str) -> str:
    """
    Best-effort extraction of item name from the same line as a price.
    Strategy:
      1) if line is a markdown table row with pipes -> take left-most column
      2) otherwise take text BEFORE the price token ($ or USD)
      3) if that fails, take text after the price token (sometimes " $200 - Item")
    """
    raw = normalize_text(line)

    # If markdown table row: "Item | Desc | $600" 0-- hopefully?  try different formats maybe with further testing? few common
    if "|" in raw:
        parts = [p.strip() for p in raw.split("|") if p.strip()]
        # choose the first meaningful column that isn't just a header
        if parts:
            return clean_item_candidate(parts[0])

    # Split around first price occurrence --- check if there is a better aproach on futher tetsing
    m = PRICE_RE.search(raw)
    if not m:
        return ""

    start = m.start()
    left = raw[:start].strip()
    right = raw[m.end():].strip()

    ## left side best side
    candidate = clean_item_candidate(left)

    # If left is whack, try right side
    if len(candidate) < 3:
        candidate = clean_item_candidate(right)

    return candidate


def parse_post(body: str, title: str):
    """
    Returns list of rows:
      item_name, item_key, price, is_sold, is_bundle, source_line
    """
    results = []
    if not body:
        return results

    title_clean = normalize_text(title)
    title_key = make_item_key(title_clean)

    lines = [ln.strip() for ln in body.splitlines() if ln.strip()]

    # just progressing through
    for ln in lines:
        if is_garbage_line(ln):
            continue

        price_matches = list(PRICE_RE.finditer(ln))
        if not price_matches:
            continue

        # check if we get price on first hit
        price_raw = price_matches[0].group(1)
        try:
            price = int(float(price_raw))
        except ValueError:
            continue

        sold = bool(SOLD_RE.search(ln)) or ("~~" in ln)  # strike-through means sold
        bundle = is_bundleish(ln)

        item_name = extract_item_from_price_line(ln)

        # check again for generic fill
        item_key = make_item_key(item_name)
        if not looks_like_real_item(item_name, item_key):
            item_name = title_clean
            item_key = title_key

       # last filter
        if not looks_like_real_item(item_name, item_key):
            continue

        # avoid absurdly long keys even after title fallback
        if len(item_key.split()) > 12:
            continue

        results.append({
            "item_name": item_name,
            "item_key": item_key,
            "price": price,
            "is_sold": sold,
            "is_bundle": bundle,
            "source_line": normalize_text(ln)
        })

    return results


def main():
    con = duckdb.connect("md:raw_reddit_listings")

    LIMIT = 996 # just because I know this is what I am getting right now

    rows = con.execute("""
        SELECT post_id, title, body, created_utc
        FROM raw_posts
        ORDER BY created_utc DESC
        LIMIT ?
    """, [LIMIT]).fetchall()

    inserted = 0

    for post_id, title, body, created_utc in rows:
        parsed = parse_post(body, title)

        for r in parsed:
            con.execute("""
                INSERT INTO parsed_items (
                    post_id, item_name, item_key,
                    price, currency,
                    is_sold, is_bundle,
                    source_line, created_utc
                )
                VALUES (?, ?, ?, ?, 'USD', ?, ?, ?, ?)
                ON CONFLICT DO NOTHING
            """, [
                post_id,
                r["item_name"],
                r["item_key"],
                r["price"],
                r["is_sold"],
                r["is_bundle"],
                r["source_line"],
                created_utc
            ])
            inserted += 1

    print(f"Done. Inserted {inserted} rows into parsed_items (duplicates skipped).")

if __name__ == "__main__":
    main()
