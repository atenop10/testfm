import os
import re
import html
import hashlib
import io
import json
import time
import sqlite3
import asyncio
import logging
from datetime import datetime, timezone
from pathlib import Path

import httpx
from dotenv import load_dotenv
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ParseMode
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    CallbackQueryHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")

# ----------------- CONFIG (from .env) -----------------
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
API_KEY = os.getenv("API_KEY", "").strip()
API_BASE = os.getenv("API_BASE", "https://openmarketplace.in/public/index.php/api/pocket-fm-api").rstrip("/")
SEARCH_PATH = os.getenv("SEARCH_PATH", "/universal-search")
SEARCH_PARAM = os.getenv("SEARCH_PARAM", "query")   # change if your curl uses q / keyword / search
ADMIN_IDS = {int(x) for x in os.getenv("ADMIN_IDS", "").replace(" ", "").split(",") if x.isdigit()}
FORCE_IPV4 = os.getenv("FORCE_IPV4", "1") == "1"
MAX_BATCH = int(os.getenv("MAX_BATCH", "20"))   # max episodes per request
PAGE_CACHE_TTL = int(os.getenv("PAGE_CACHE_TTL", "21600"))   # 6h for ascending shows (stable); 1h if newest-first
# --- episode pagination (the API returns only ~8 episodes per page) ---
EPISODE_PAGE_PARAM = os.getenv("EPISODE_PAGE_PARAM", "").strip()    # pin the page param name, e.g. page / offset
EPISODE_PAGE_MODE = os.getenv("EPISODE_PAGE_MODE", "page").strip()  # "page" (1,2,3..) or "offset" (0,8,16..) when pinned
EPISODE_LIMIT_PARAM = os.getenv("EPISODE_LIMIT_PARAM", "").strip()  # optional page-size param, e.g. limit / per_page
EPISODE_LIMIT = int(os.getenv("EPISODE_LIMIT", "100"))
MAX_EPISODE_PAGES = int(os.getenv("MAX_EPISODE_PAGES", "200"))   # only used by the rare fallback
MAX_UPLOAD_MB = 50

if not BOT_TOKEN or not API_KEY:
    raise SystemExit("Missing TELEGRAM_BOT_TOKEN or API_KEY in .env")

HEADERS = {
    "X-API-KEY": API_KEY,
    "Accept": "application/json",
    "User-Agent": "curl/8.5.0",   # same style as curl, which works for you
}

DOWNLOAD_DIR = BASE_DIR / "downloads"
DOWNLOAD_DIR.mkdir(exist_ok=True)
DB_PATH = BASE_DIR / "bot.db"

logging.basicConfig(format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO)
logging.getLogger("httpx").setLevel(logging.WARNING)
logger = logging.getLogger("pocketfm")


# ----------------- DATABASE (stats) -----------------
def db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    with db() as c:
        c.executescript("""
        CREATE TABLE IF NOT EXISTS users(
            user_id INTEGER PRIMARY KEY, username TEXT, first_seen TEXT, last_seen TEXT);
        CREATE TABLE IF NOT EXISTS events(
            id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER,
            kind TEXT, detail TEXT, ok INTEGER, ts TEXT);
        CREATE TABLE IF NOT EXISTS ids(key TEXT PRIMARY KEY, value TEXT);
        CREATE TABLE IF NOT EXISTS page_cache(
            show_id TEXT, page INTEGER, data TEXT, ts REAL, PRIMARY KEY(show_id, page));
        """)


def now():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def track_user(update: Update):
    u = update.effective_user
    if not u:
        return
    with db() as c:
        c.execute(
            "INSERT INTO users(user_id, username, first_seen, last_seen) VALUES(?,?,?,?) "
            "ON CONFLICT(user_id) DO UPDATE SET username=excluded.username, last_seen=excluded.last_seen",
            (u.id, u.username or "", now(), now()),
        )


def reg(value) -> str:
    """Short key for long IDs (Telegram callback_data is limited to 64 bytes)."""
    value = str(value)
    key = hashlib.md5(value.encode()).hexdigest()[:10]
    with db() as c:
        c.execute("INSERT OR IGNORE INTO ids(key, value) VALUES(?,?)", (key, value))
    return key


def unreg(key: str):
    with db() as c:
        row = c.execute("SELECT value FROM ids WHERE key=?", (key,)).fetchone()
    return row[0] if row else None


def log_event(user_id: int, kind: str, detail: str, ok: bool):
    with db() as c:
        c.execute("INSERT INTO events(user_id, kind, detail, ok, ts) VALUES(?,?,?,?,?)",
                  (user_id, kind, detail[:200], int(ok), now()))


# ----------------- API CLIENT -----------------
class ApiError(Exception):
    def __init__(self, message, status=None):
        super().__init__(message)
        self.status = status


# --- rate limiting: spacing between calls + retry on 429 ---
MIN_INTERVAL = float(os.getenv("API_MIN_INTERVAL", "1.0"))   # seconds between API calls
_api_lock = asyncio.Lock()
_last_call = 0.0
_search_cache = {}          # query -> (timestamp, payload)
SEARCH_CACHE_TTL = 600      # 10 minutes


def make_client(timeout: float = 30.0) -> httpx.AsyncClient:
    # local_address 0.0.0.0 forces IPv4 (fixes VPSes with broken IPv6 where curl still works)
    transport = httpx.AsyncHTTPTransport(local_address="0.0.0.0", retries=2) if FORCE_IPV4 \
        else httpx.AsyncHTTPTransport(retries=2)
    return httpx.AsyncClient(timeout=timeout, transport=transport, follow_redirects=True, headers=HEADERS)


async def api_request(method: str, path: str, *, params=None, json=None):
    global _last_call
    url = f"{API_BASE}{path}"
    for attempt in range(3):
        async with _api_lock:
            wait = MIN_INTERVAL - (time.monotonic() - _last_call)
            if wait > 0:
                await asyncio.sleep(wait)
            _last_call = time.monotonic()

        async with make_client() as client:
            try:
                resp = await client.request(method, url, params=params, json=json)
            except httpx.TimeoutException as e:
                raise ApiError(f"Timeout reaching API ({type(e).__name__})") from e
            except httpx.HTTPError as e:
                raise ApiError(f"Network error: {type(e).__name__}: {e}") from e

        logger.info("%s %s -> %s", method, resp.url, resp.status_code)
        try:
            log_event(0, "api", path, resp.status_code < 400)
        except Exception:
            pass

        if resp.status_code == 429:
            ra = resp.headers.get("Retry-After", "")
            try:
                delay = min(float(ra), 30.0)
            except ValueError:
                delay = 3.0 * (attempt + 1)
            logger.warning("429 rate limited (Retry-After=%r, body=%s)", ra, resp.text[:200])
            if attempt < 2:
                await asyncio.sleep(delay)
                continue
            raise ApiError("API rate limit reached (HTTP 429). Please wait a minute and try again.", 429)

        if resp.status_code >= 400:
            logger.error("API body: %s", resp.text[:500])
            try:
                j = resp.json()
                detail = str(j.get("message") or j.get("error") or j.get("errors") or j) if isinstance(j, dict) else str(j)
            except ValueError:
                detail = resp.text
            raise ApiError(f"API returned HTTP {resp.status_code}: {detail[:200]}", resp.status_code)
        try:
            return resp.json()
        except ValueError:
            logger.error("Non-JSON body: %s", resp.text[:500])
            raise ApiError("API did not return JSON")


LIST_KEYS = ("shows", "episodes", "results", "result", "stories", "items", "list", "data")


def find_list(payload, depth=0):
    """Find the main list of dicts in whatever shape the API returns (e.g. data.result.shows)."""
    if depth > 6:
        return []
    if isinstance(payload, list):
        return payload
    if not isinstance(payload, dict):
        return []
    for k in LIST_KEYS:
        v = payload.get(k)
        if isinstance(v, list) and v:
            return v
    for k in LIST_KEYS:
        v = payload.get(k)
        if isinstance(v, dict):
            found = find_list(v, depth + 1)
            if found:
                return found
    for v in payload.values():
        if isinstance(v, list) and v and isinstance(v[0], dict):
            return v
    for v in payload.values():
        if isinstance(v, dict):
            found = find_list(v, depth + 1)
            if found:
                return found
    return []


def first(d: dict, *keys, default=None):
    for k in keys:
        if d.get(k) not in (None, ""):
            return d[k]
    return default


def shallow_find(obj, *keys):
    """Look for a key at the top level or one dict level down (not inside lists)."""
    if not isinstance(obj, dict):
        return None
    v = first(obj, *keys)
    if v is not None:
        return v
    for child in obj.values():
        if isinstance(child, dict):
            v = first(child, *keys)
            if v is not None:
                return v
    return None


def find_url(obj, depth=0):
    """DFS for an audio url under common key names."""
    if depth > 6:
        return None
    if isinstance(obj, dict):
        for k in ("media_url", "audio_url", "stream_url", "hls_url", "url"):
            v = obj.get(k)
            if isinstance(v, str) and v.startswith("http"):
                return v
        for v in obj.values():
            r = find_url(v, depth + 1)
            if r:
                return r
    elif isinstance(obj, list):
        for v in obj:
            r = find_url(v, depth + 1)
            if r:
                return r
    return None


def _collect_lists(obj, out, depth=0):
    if depth > 8:
        return
    if isinstance(obj, dict):
        for v in obj.values():
            _collect_lists(v, out, depth + 1)
    elif isinstance(obj, list):
        if obj and all(isinstance(x, dict) for x in obj):
            out.append(obj)
        for x in obj:
            _collect_lists(x, out, depth + 1)


def find_episode_list(details):
    """Longest list of dicts anywhere in the response, ignoring the show-info block."""
    lists = []
    _collect_lists(details, lists)
    lists = [l for l in lists if not any("show_title" in x for x in l)]
    return max(lists, key=len) if lists else []


def get_show_name(details):
    node = details
    if isinstance(node, dict) and isinstance(node.get("result"), list) and node["result"]:
        node = node["result"][0]
    return shallow_find(node, "show_title", "title", "name") or "Episodes"


def get_sort_order(details):
    node = details
    if isinstance(node, dict) and isinstance(node.get("result"), list) and node["result"]:
        node = node["result"][0]
    return str(shallow_find(node, "sort_order") or "asc").lower()


def shape(obj, depth=0):
    """Compact outline of a JSON structure (long strings trimmed, first list item only)."""
    pad = "  " * depth
    if isinstance(obj, dict):
        out = []
        for k, v in obj.items():
            if isinstance(v, dict):
                out.append(f"{pad}{k}: dict({len(v)})")
                out.append(shape(v, depth + 1))
            elif isinstance(v, list):
                out.append(f"{pad}{k}: list({len(v)})")
                if v:
                    out.append(shape(v[0], depth + 1) if isinstance(v[0], (dict, list)) else f"{pad}  [0]: {repr(v[0])[:40]}")
            else:
                out.append(f"{pad}{k}: {repr(v)[:40]}")
        return "\n".join(out)
    if isinstance(obj, list):
        return f"{pad}list({len(obj)})\n" + (shape(obj[0], depth + 1) if obj else "")
    return f"{pad}{repr(obj)[:40]}"


SHOW_PARAMS = ("story_id", "show_id", "entity_id")
PLAYER_KEYS = (
    (("show_id", "S"), ("story_id", "E")),
    (("story_id", "S"), ("episode_id", "E")),
    (("show_id", "S"), ("episode_id", "E")),
    (("entity_id", "S"), ("episode_id", "E")),
    (("story_id", "E"),),
    (("episode_id", "E"),),
    (("entity_id", "E"),),
)


def player_body(tpl, show_id, ep_id):
    return {k: (show_id if v == "S" else ep_id) for k, v in tpl}


PAGE_MODES = [("page", "page"), ("page_no", "page"), ("pageNo", "page"), ("p", "page"),
              ("offset", "offset"), ("start", "offset"), ("skip", "offset")]
_page_mode = (EPISODE_PAGE_PARAM, EPISODE_PAGE_MODE) if EPISODE_PAGE_PARAM else None
NUM_KEYS = ("episode_number", "sequence_number", "episode_no", "seq_number", "story_number", "number", "index")
TOTAL_KEYS = ("total_episodes", "total_count", "total_stories", "episode_count", "episodes_count", "total")


def _ep_key(ep: dict) -> str:
    v = first(ep, "entity_id", "story_id", "id", "episode_id")
    return str(v) if v is not None else json.dumps(ep, sort_keys=True, default=str)[:200]


# ---- persistent cache (survives restarts, so repeat requests cost 0 API calls) ----
def cache_get(show_id: str, page: int, ttl: float):
    with db() as c:
        r = c.execute("SELECT data, ts FROM page_cache WHERE show_id=? AND page=?", (show_id, page)).fetchone()
    if r and time.time() - r["ts"] < ttl:
        return json.loads(r["data"])
    return None


def cache_put(show_id: str, page: int, data):
    with db() as c:
        c.execute("INSERT OR REPLACE INTO page_cache(show_id, page, data, ts) VALUES(?,?,?,?)",
                  (show_id, page, json.dumps(data, ensure_ascii=False), time.time()))


def load_page_mode():
    global _page_mode
    if _page_mode:
        return
    with db() as c:
        r = c.execute("SELECT value FROM ids WHERE key='page_mode'").fetchone()
    if r:
        _page_mode = tuple(json.loads(r[0]))


def save_page_mode(mode):
    with db() as c:
        c.execute("INSERT OR REPLACE INTO ids(key, value) VALUES('page_mode', ?)", (json.dumps(list(mode)),))


async def _details_page(id_param: str, show_id: str, extra: dict):
    params = {id_param: show_id, **extra}
    if EPISODE_LIMIT_PARAM:
        params.setdefault(EPISODE_LIMIT_PARAM, EPISODE_LIMIT)
    payload = await api_request("GET", "/show-details-story", params=params)
    details = payload.get("data", payload) if isinstance(payload, dict) else {}
    return payload, details, find_episode_list(details)


def _page_extra(mode, page_no: int, size: int) -> dict:
    name, kind = mode
    return {name: page_no} if kind == "page" else {name: (page_no - 1) * size}


def get_total(details):
    node = details
    if isinstance(node, dict) and isinstance(node.get("result"), list) and node["result"]:
        node = node["result"][0]
    for src in (node, details):
        v = shallow_find(src, *TOTAL_KEYS)
        try:
            if v is not None and int(v) > 0:
                return int(v)
        except (TypeError, ValueError):
            pass
    return None


async def fetch_show_details(show_id: str):
    """ONE API call: page 1 of the show (name, sort order, total if given, first episodes)."""
    global SHOW_PARAMS
    last_err = None
    for p in SHOW_PARAMS:
        try:
            payload, details, episodes = await _details_page(p, show_id, {})
        except ApiError as e:
            if e.status == 429:
                raise
            last_err = e
            continue
        if episodes:
            SHOW_PARAMS = (p,) + tuple(x for x in SHOW_PARAMS if x != p)
            return get_show_name(details), episodes, get_sort_order(details), get_total(details)
        logger.warning("show-details with %s returned no episodes: %s", p, str(payload)[:400])
    raise last_err or ApiError("No episodes found (admin: try /debugshow)")


async def fetch_page_remote(show_id: str, page_no: int, size: int):
    """Fetch exactly one page (>=2). The pagination param is detected once, then remembered on disk."""
    global _page_mode
    page1 = cache_get(show_id, 1, 10 ** 12) or []
    page1_keys = {_ep_key(e) for e in page1}
    id_param = SHOW_PARAMS[0]
    if _page_mode:
        _, _, eps = await _details_page(id_param, show_id, _page_extra(_page_mode, page_no, size))
        return eps
    for cand in PAGE_MODES:
        try:
            _, _, eps = await _details_page(id_param, show_id, _page_extra(cand, page_no, size))
        except ApiError as e:
            if e.status == 429:
                raise
            continue
        if not eps:
            # empty could mean "past the end" for a correct param; only trust it if page 1 is also tiny
            continue
        if page1_keys and {_ep_key(e) for e in eps} & page1_keys:
            continue          # same episodes as page 1 -> this param is ignored by the API
        _page_mode = cand
        save_page_mode(cand)
        logger.info("Pagination param detected and saved: %s", cand)
        return eps
    raise ApiError("Couldn't find the API's page parameter. Admin: run /debugshow <id> page 2 and set "
                   "EPISODE_PAGE_PARAM in .env")


def _numfield(eps):
    for key in NUM_KEYS:
        try:
            ints = [int(e[key]) for e in eps]
        except (KeyError, TypeError, ValueError):
            continue
        if len(set(ints)) == len(ints):
            return key, ints
    return None, None


async def load_show(show_id: str) -> dict:
    """Meta about a show. Costs 1 API call the first time, 0 afterwards."""
    meta = cache_get(show_id, 0, 3600)
    if meta:
        return meta
    name, eps, sort_order, total = await fetch_show_details(show_id)
    key, ints = _numfield(eps)
    zero = bool(key and min(ints) == 0)
    if key and not total and sort_order == "desc":
        total = max(ints) + (1 if zero else 0)
    meta = {"name": name, "size": len(eps), "sort": sort_order, "total": total, "numfield": key, "zero": zero}
    cache_put(show_id, 1, eps)
    # rare fallback: newest-first list with no way to know the total -> we must walk the pages once
    if sort_order == "desc" and not total:
        n_pages, all_n, page_no = 1, len(eps), 2
        while page_no <= MAX_EPISODE_PAGES:
            more = await fetch_page_remote(show_id, page_no, meta["size"])
            if not more:
                break
            cache_put(show_id, page_no, more)
            all_n += len(more)
            page_no += 1
        meta["total"] = all_n
    cache_put(show_id, 0, meta)
    return meta


async def get_page(show_id: str, meta: dict, page_no: int):
    ttl = 3600 if meta["sort"] == "desc" else PAGE_CACHE_TTL
    cached = cache_get(show_id, page_no, ttl)
    if cached is not None:
        return cached
    eps = await fetch_page_remote(show_id, page_no, meta["size"]) if page_no > 1 else None
    if page_no == 1:
        _, eps, _, _ = await fetch_show_details(show_id)
    cache_put(show_id, page_no, eps)
    return eps


def locate(meta: dict, n: int):
    size = meta["size"] or 8
    total = meta.get("total")
    if total and n > total:
        return None
    if meta["sort"] == "desc":
        if not total:
            return None
        pos = total - n + 1
    else:
        pos = n
    if pos < 1:
        return None
    return (pos - 1) // size + 1, (pos - 1) % size


async def resolve_episode(show_id: str, meta: dict, n: int):
    loc = locate(meta, n)
    if not loc:
        return None
    page, idx = loc
    size = meta["size"] or 8
    nf = meta.get("numfield")
    for _ in range(2):
        eps = await get_page(show_id, meta, page)
        if not eps:
            return None
        if not nf:
            return eps[idx] if idx < len(eps) else None
        try:
            nums = [int(e[nf]) + (1 if meta.get("zero") else 0) for e in eps]
        except (KeyError, TypeError, ValueError):
            return eps[idx] if idx < len(eps) else None
        if n in nums:
            return eps[nums.index(n)]
        step = round((n - nums[len(nums) // 2]) / size)
        if meta["sort"] == "desc":
            step = -step
        if step == 0 or page + step < 1:
            return None
        page += step
    return None


async def fetch_player(show_id: str, ep_id: str):
    global PLAYER_KEYS
    last_err = None
    for tpl in PLAYER_KEYS:
        try:
            payload = await api_request("POST", "/player", json=player_body(tpl, show_id, ep_id))
        except ApiError as e:
            if e.status == 429:
                raise
            last_err = e
            continue
        url = find_url(payload)
        if url:
            PLAYER_KEYS = (tpl,) + tuple(x for x in PLAYER_KEYS if x != tpl)
            data = payload.get("data", payload) if isinstance(payload, dict) else {}
            return url, shallow_find(data, "episode_title", "story_title", "title", "name")
        logger.warning("player %s returned no url: %s", dict(tpl), str(payload)[:400])
    if last_err:
        raise last_err
    return None, None


# ----------------- DOWNLOAD HELPERS -----------------
def clean_filename(name: str) -> str:
    return re.sub(r'[\\/*?:"<>|]', "", str(name)).strip()[:70] or "audio"


async def fetch_direct_audio(url: str, target: Path):
    async with make_client(timeout=180.0) as client:
        async with client.stream("GET", url) as resp:
            resp.raise_for_status()
            with open(target, "wb") as f:
                async for chunk in resp.aiter_bytes(65536):
                    f.write(chunk)


async def fetch_hls_stream(m3u8_url: str, target: Path):
    proc = await asyncio.create_subprocess_exec(
        "ffmpeg", "-y", "-loglevel", "error", "-i", m3u8_url,
        "-vn", "-c:a", "libmp3lame", "-b:a", "128k", str(target),
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
    )
    try:
        _, err = await asyncio.wait_for(proc.communicate(), timeout=600)
    except asyncio.TimeoutError:
        proc.kill()
        raise RuntimeError("FFmpeg timed out")
    if proc.returncode != 0:
        raise RuntimeError(f"FFmpeg error: {err.decode(errors='ignore')[:300]}")


def esc(s) -> str:
    return html.escape(str(s))


def is_admin(update: Update) -> bool:
    return bool(update.effective_user and update.effective_user.id in ADMIN_IDS)


# ----------------- USER HANDLERS -----------------
async def start_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    track_user(update)
    await update.message.reply_text(
        "👋 <b>Welcome to Pocket FM Bot!</b>\n\n"
        "1. Search a series:\n<code>/search show title</code>\n"
        "2. Pick the show from the list\n"
        "3. Send the episode number(s) you want, e.g. <code>340</code> or <code>340-344</code>",
        parse_mode=ParseMode.HTML,
    )


async def search_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    track_user(update)
    uid = update.effective_user.id
    query = " ".join(context.args).strip()
    if not query:
        await update.message.reply_text("⚠️ Usage: <code>/search show title</code>", parse_mode=ParseMode.HTML)
        return

    status = await update.message.reply_text(f"🔍 Searching for <b>{esc(query)}</b>...", parse_mode=ParseMode.HTML)
    try:
        cached = _search_cache.get(query.lower())
        if cached and time.time() - cached[0] < SEARCH_CACHE_TTL:
            payload = cached[1]
        else:
            payload = await api_request("GET", SEARCH_PATH, params={SEARCH_PARAM: query})
            _search_cache[query.lower()] = (time.time(), payload)
            if len(_search_cache) > 200:
                _search_cache.pop(next(iter(_search_cache)))
        shows = find_list(payload)
        if not shows:
            log_event(uid, "search", query, True)
            await status.edit_text("❌ No shows found for that keyword.")
            return

        buttons = []
        for item in shows[:8]:
            if not isinstance(item, dict):
                continue
            title = first(item, "title", "name", "show_title", default="Story")
            sid = first(item, "entity_id", "id", "story_id", "show_id")
            if sid is not None:
                buttons.append([InlineKeyboardButton(f"📖 {str(title)[:40]}", callback_data=f"sh|{reg(sid)}")])
        if not buttons:
            logger.error("Search payload had no usable ids: %s", str(payload)[:500])
            raise ApiError("Unexpected API response format")

        log_event(uid, "search", query, True)
        await status.edit_text("Select a show to view episodes:", reply_markup=InlineKeyboardMarkup(buttons))

    except ApiError as e:
        log_event(uid, "search", query, False)
        await status.edit_text(f"❌ {esc(e)}", parse_mode=ParseMode.HTML)
    except Exception as e:
        logger.exception("Search failed")
        log_event(uid, "search", query, False)
        await status.edit_text(f"❌ Unexpected error: {esc(type(e).__name__)}")


def parse_ranges(text: str):
    """'340', '340-344', '1,3,10-12', '5 to 7' -> [numbers]; None if the text isn't a valid selection."""
    t = re.sub(r"\s*(?:-|–|—|\bto\b)\s*", "-", text.strip().lower())
    nums, seen = [], set()
    for tok in re.split(r"[,\s]+", t):
        if not tok:
            continue
        m = re.fullmatch(r"(\d+)(?:-(\d+))?", tok)
        if not m:
            return None
        lo = int(m.group(1))
        hi = int(m.group(2)) if m.group(2) else lo
        if hi < lo or hi - lo > 500:
            return None
        for n in range(lo, hi + 1):
            if n not in seen:
                seen.add(n)
                nums.append(n)
    return nums or None


async def callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    track_user(update)
    parts = q.data.split("|")
    if parts[0] != "sh":
        return
    show_id = unreg(parts[1])
    if not show_id:
        await q.edit_message_text("⚠️ Button expired. Please search again.")
        return
    try:
        meta = cache_get(show_id, 0, 3600)
        if not meta:
            await q.edit_message_text("⏳ Loading show info...")
            meta = await load_show(show_id)
        context.user_data["show_id"] = show_id
        total = meta.get("total")
        avail = f"📚 <b>{total}</b> episodes (#1 to #{total})" if total else "📚 Send any episode number"
        await q.edit_message_text(
            f"🎧 <b>{esc(meta['name'])}</b>\n{avail}\n\n"
            "Send the episode number(s) you want:\n"
            "• <code>340</code>\n• <code>340-344</code>\n• <code>1,5,10-12</code>\n\n"
            f"Max {MAX_BATCH} per request. /cancel to stop.",
            parse_mode=ParseMode.HTML,
        )
    except ApiError as e:
        await q.edit_message_text(f"❌ {esc(e)}", parse_mode=ParseMode.HTML)
    except Exception:
        logger.exception("Show load failed")
        await q.edit_message_text("❌ Failed to load the show.")


async def send_episode(message, uid: int, show_id: str, num: int, ep: dict) -> bool:
    ep_id = first(ep, "entity_id", "story_id", "id", "episode_id")
    status = await message.reply_text(f"⚡ Episode {num}: generating link...")
    file_path = None
    try:
        if ep_id is None:
            await status.edit_text(f"❌ Episode {num}: no ID found in API data.")
            return False
        audio_url, raw_title = await fetch_player(show_id, str(ep_id))
        raw_title = raw_title or first(ep, "title", "story_title", "episode_title", "name") or f"Episode_{num}"
        if not audio_url:
            log_event(uid, "download", f"{show_id}/{ep_id} locked", False)
            await status.edit_text(f"🔒 Episode {num} is coin-locked or unavailable.")
            return False

        file_path = DOWNLOAD_DIR / f"{uid}_{num}_{clean_filename(raw_title)}.mp3"
        if ".m3u8" in audio_url:
            await status.edit_text(f"⏳ Episode {num}: processing stream via FFmpeg...")
            await fetch_hls_stream(audio_url, file_path)
        else:
            await status.edit_text(f"📥 Episode {num}: downloading...")
            await fetch_direct_audio(audio_url, file_path)

        size_mb = file_path.stat().st_size / (1024 * 1024)
        if size_mb > MAX_UPLOAD_MB:
            log_event(uid, "download", f"{show_id}/{ep_id} too big", False)
            await status.edit_text(f"⚠️ Episode {num} is {size_mb:.0f}MB, above Telegram's {MAX_UPLOAD_MB}MB bot limit.")
            return False

        await status.edit_text(f"📤 Episode {num}: uploading...")
        with open(file_path, "rb") as f:
            await message.reply_audio(
                audio=f, title=str(raw_title), performer="Pocket FM",
                caption=f"🎧 <b>{esc(raw_title)}</b>\nEpisode {num}", parse_mode=ParseMode.HTML,
                read_timeout=300, write_timeout=300,
            )
        log_event(uid, "download", f"{show_id}/{ep_id}", True)
        await status.delete()
        return True

    except ApiError as e:
        log_event(uid, "download", f"{show_id}/{ep_id}", False)
        await status.edit_text(f"❌ Episode {num}: {esc(e)}", parse_mode=ParseMode.HTML)
        if e.status == 429:
            raise
        return False
    except Exception as e:
        logger.exception("Download failed")
        log_event(uid, "download", f"{show_id}/{ep_id}", False)
        await status.edit_text(f"❌ Episode {num}: {esc(type(e).__name__)}: {esc(str(e)[:200])}", parse_mode=ParseMode.HTML)
        return False
    finally:
        if file_path and file_path.exists():
            file_path.unlink()


async def text_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    track_user(update)
    uid = update.effective_user.id
    show_id = context.user_data.get("show_id")
    if not show_id:
        await update.message.reply_text("Use <code>/search show title</code> first, then pick a show.", parse_mode=ParseMode.HTML)
        return

    nums = parse_ranges(update.message.text)
    if nums is None:
        await update.message.reply_text(
            "⚠️ Send episode numbers like <code>340</code>, <code>340-344</code> or <code>1,5,10-12</code>.",
            parse_mode=ParseMode.HTML)
        return
    if len(nums) > MAX_BATCH:
        await update.message.reply_text(f"⚠️ Max {MAX_BATCH} episodes per request (you asked for {len(nums)}).")
        return

    if context.user_data.get("busy"):
        await update.message.reply_text("⏳ Still working on your previous request. Please wait.")
        return
    context.user_data["busy"] = True
    try:
        try:
            meta = cache_get(show_id, 0, 3600) or await load_show(show_id)
            resolved, missing = {}, []
            for n in nums:                       # pages are cached, so 340-345 usually costs 1-2 calls total
                ep = await resolve_episode(show_id, meta, n)
                if ep is None:
                    missing.append(n)
                else:
                    resolved[n] = ep
        except ApiError as e:
            await update.message.reply_text(f"❌ {esc(e)}", parse_mode=ParseMode.HTML)
            return
        if missing:
            total = meta.get("total")
            await update.message.reply_text(
                f"⚠️ Not available: {', '.join(map(str, missing[:15]))}" + (f"\nAvailable: #1 to #{total}" if total else ""))
            return

        ok = 0
        for n in nums:
            try:
                if await send_episode(update.message, uid, show_id, n, resolved[n]):
                    ok += 1
            except ApiError:
                await update.message.reply_text("⛔ Rate limit hit, stopping. Try the remaining episodes in a minute.")
                break
        if len(nums) > 1:
            await update.message.reply_text(f"✅ Done: {ok}/{len(nums)} episodes sent.")
    finally:
        context.user_data["busy"] = False


async def cancel_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.pop("show_id", None)
    await update.message.reply_text("Cancelled. Use /search to start again.")


# ----------------- ADMIN HANDLERS -----------------
async def stats_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update):
        return
    today = datetime.now(timezone.utc).date().isoformat()
    with db() as c:
        one = lambda sql, *a: c.execute(sql, a).fetchone()[0]
        users = one("SELECT COUNT(*) FROM users")
        new_today = one("SELECT COUNT(*) FROM users WHERE first_seen LIKE ?", today + "%")
        active_today = one("SELECT COUNT(*) FROM users WHERE last_seen LIKE ?", today + "%")
        searches = one("SELECT COUNT(*) FROM events WHERE kind='search'")
        searches_today = one("SELECT COUNT(*) FROM events WHERE kind='search' AND ts LIKE ?", today + "%")
        dl_ok = one("SELECT COUNT(*) FROM events WHERE kind='download' AND ok=1")
        dl_today = one("SELECT COUNT(*) FROM events WHERE kind='download' AND ok=1 AND ts LIKE ?", today + "%")
        failed = one("SELECT COUNT(*) FROM events WHERE ok=0 AND kind!='api'")
        api_today = one("SELECT COUNT(*) FROM events WHERE kind='api' AND ts LIKE ?", today + "%")
        api_total = one("SELECT COUNT(*) FROM events WHERE kind='api'")
    await update.message.reply_text(
        "📊 <b>Bot Stats</b>\n\n"
        f"👥 Users: <b>{users}</b> (new today {new_today}, active today {active_today})\n"
        f"🔍 Searches: <b>{searches}</b> (today {searches_today})\n"
        f"⬇️ Downloads: <b>{dl_ok}</b> (today {dl_today})\n"
        f"⚠️ Failed actions: <b>{failed}</b>\n"
        f"🌐 API calls: <b>{api_total}</b> (today {api_today})",
        parse_mode=ParseMode.HTML,
    )


async def users_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update):
        return
    with db() as c:
        rows = c.execute("SELECT user_id, username, last_seen FROM users ORDER BY last_seen DESC LIMIT 20").fetchall()
    lines = [f"<code>{r['user_id']}</code> @{esc(r['username'] or '-')} · {esc(r['last_seen'][:16])}" for r in rows]
    await update.message.reply_text("👥 <b>Recent users</b>\n" + ("\n".join(lines) or "none"), parse_mode=ParseMode.HTML)


async def broadcast_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update):
        return
    text = " ".join(context.args).strip()
    if not text:
        await update.message.reply_text("Usage: /broadcast your message")
        return
    with db() as c:
        ids = [r[0] for r in c.execute("SELECT user_id FROM users").fetchall()]
    sent = failed = 0
    for uid in ids:
        try:
            await context.bot.send_message(uid, text)
            sent += 1
        except Exception:
            failed += 1
        await asyncio.sleep(0.05)
    await update.message.reply_text(f"📣 Sent: {sent}, failed: {failed}")


async def debug_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin only: /debug <query> shows the raw API response so you can fix field names."""
    if not is_admin(update):
        return
    query = " ".join(context.args).strip() or "test"
    try:
        payload = await api_request("GET", SEARCH_PATH, params={SEARCH_PARAM: query})
        await update.message.reply_text(f"<pre>{esc(str(payload)[:3500])}</pre>", parse_mode=ParseMode.HTML)
    except Exception as e:
        await update.message.reply_text(f"{type(e).__name__}: {e}")


async def send_debug(update: Update, payload, label: str):
    outline = shape(payload)
    await update.message.reply_text(
        f"<b>{esc(label)}</b>\n<pre>{esc(outline[:3500])}</pre>", parse_mode=ParseMode.HTML)
    raw = json.dumps(payload, ensure_ascii=False, indent=1).encode()
    await update.message.reply_document(io.BytesIO(raw), filename="response.json")


async def debugshow_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin only: /debugshow <show_entity_id> dumps raw show-details response."""
    if not is_admin(update) or not context.args:
        return
    sid = context.args[0]
    extra = {}
    if len(context.args) >= 3:          # /debugshow <id> <param> <value>  e.g. /debugshow 123 page 2
        extra = {context.args[1]: context.args[2]}
    for p in SHOW_PARAMS:
        try:
            payload = await api_request("GET", "/show-details-story", params={p: sid, **extra})
            await send_debug(update, payload, f"param={p}")
            return
        except Exception as e:
            await update.message.reply_text(f"param={p}: {type(e).__name__}: {e}")


async def debugplay_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin only: /debugplay <show_id> <episode_id> dumps raw player response."""
    if not is_admin(update) or len(context.args) < 2:
        return
    sid, eid = context.args[:2]
    errors = []
    for tpl in PLAYER_KEYS:
        body = player_body(tpl, sid, eid)
        try:
            payload = await api_request("POST", "/player", json=body)
            await send_debug(update, payload, f"OK with {list(body)}")
            return
        except Exception as e:
            errors.append(f"{list(body)} -> {e}")
            if getattr(e, "status", None) == 429:
                break
    await update.message.reply_text("All attempts failed:\n" + "\n".join(errors)[:3800])


async def error_handler(update, context: ContextTypes.DEFAULT_TYPE):
    logger.error("Unhandled error", exc_info=context.error)


# ----------------- MAIN -----------------
def main():
    init_db()
    load_page_mode()
    app = (
        ApplicationBuilder()
        .token(BOT_TOKEN)
        .read_timeout(60)
        .write_timeout(120)
        .concurrent_updates(True)
        .build()
    )
    app.add_handler(CommandHandler("start", start_handler))
    app.add_handler(CommandHandler("search", search_handler))
    app.add_handler(CommandHandler("stats", stats_handler))
    app.add_handler(CommandHandler("users", users_handler))
    app.add_handler(CommandHandler("broadcast", broadcast_handler))
    app.add_handler(CommandHandler("debug", debug_handler))
    app.add_handler(CommandHandler("debugshow", debugshow_handler))
    app.add_handler(CommandHandler("debugplay", debugplay_handler))
    app.add_handler(CommandHandler("cancel", cancel_handler))
    app.add_handler(CallbackQueryHandler(callback_handler))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_handler))
    app.add_error_handler(error_handler)
    logger.info("Bot online. Admins: %s", sorted(ADMIN_IDS) or "none set")
    app.run_polling()


if __name__ == "__main__":
    main()
