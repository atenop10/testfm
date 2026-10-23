import os
import re
import html
import hashlib
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
PAGE_SIZE = 8
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
            raise ApiError(f"API returned HTTP {resp.status_code}", resp.status_code)
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


SHOW_PARAMS = ("story_id", "show_id", "entity_id")
PLAYER_KEYS = (("story_id", "episode_id"), ("show_id", "episode_id"),
               ("entity_id", "episode_id"), ("show_id", "entity_id"))


async def fetch_show_details(show_id: str):
    global SHOW_PARAMS
    last_err = None
    for p in SHOW_PARAMS:
        try:
            payload = await api_request("GET", "/show-details-story", params={p: show_id})
        except ApiError as e:
            if e.status == 429:
                raise
            last_err = e
            continue
        details = payload.get("data", payload) if isinstance(payload, dict) else {}
        episodes = [e for e in find_list(details) if isinstance(e, dict)]
        if episodes:
            SHOW_PARAMS = (p,) + tuple(x for x in SHOW_PARAMS if x != p)
            name = shallow_find(details, "title", "name", "show_title") or "Episodes"
            return name, episodes
        logger.warning("show-details with %s returned no episodes: %s", p, str(payload)[:400])
    raise last_err or ApiError("No episodes found (admin: try /debugshow)")


async def fetch_player(show_id: str, ep_id: str):
    global PLAYER_KEYS
    last_err = None
    for ks, ke in PLAYER_KEYS:
        try:
            payload = await api_request("POST", "/player", json={ks: show_id, ke: ep_id})
        except ApiError as e:
            if e.status == 429:
                raise
            last_err = e
            continue
        url = find_url(payload)
        if url:
            PLAYER_KEYS = ((ks, ke),) + tuple(x for x in PLAYER_KEYS if x != (ks, ke))
            data = payload.get("data", payload) if isinstance(payload, dict) else {}
            return url, shallow_find(data, "episode_title", "title", "name")
        logger.warning("player with %s/%s returned no url: %s", ks, ke, str(payload)[:400])
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
        "Search any audio series using:\n<code>/search show title</code>\n\n"
        "Example:\n<code>/search Instamillionaire</code>",
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


def episode_keyboard(show_id: str, episodes: list, page: int) -> InlineKeyboardMarkup:
    ks = reg(show_id)
    start = page * PAGE_SIZE
    rows = []
    for i, ep in enumerate(episodes[start:start + PAGE_SIZE], start=start + 1):
        ep_id = first(ep, "entity_id", "id", "episode_id")
        if ep_id is None:
            continue
        title = first(ep, "title", "episode_title", "name") or f"Episode {first(ep, 'episode_number', 'number', default=i)}"
        rows.append([InlineKeyboardButton(f"⬇️ {str(title)[:38]}", callback_data=f"dl|{ks}|{reg(ep_id)}")])
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"pg|{ks}|{page - 1}"))
    if start + PAGE_SIZE < len(episodes):
        nav.append(InlineKeyboardButton("Next ➡️", callback_data=f"pg|{ks}|{page + 1}"))
    if nav:
        rows.append(nav)
    return InlineKeyboardMarkup(rows)


async def callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    track_user(update)
    parts = q.data.split("|")
    action = parts[0]
    uid = q.from_user.id
    cache = context.bot_data.setdefault("episodes", {})

    # ---- show selected / page change ----
    if action in ("sh", "pg"):
        show_id = unreg(parts[1])
        if not show_id:
            await q.edit_message_text("⚠️ Button expired. Please search again.")
            return
        page = int(parts[2]) if action == "pg" else 0
        try:
            if show_id not in cache:
                await q.edit_message_text("⏳ Fetching episodes list...")
                name, episodes = await fetch_show_details(show_id)
                cache[show_id] = {"name": name, "episodes": episodes}
            entry = cache[show_id]
            await q.edit_message_text(
                f"🎧 <b>{esc(entry['name'])}</b>\nSelect an episode ({len(entry['episodes'])} total):",
                reply_markup=episode_keyboard(show_id, entry["episodes"], page),
                parse_mode=ParseMode.HTML,
            )
        except ApiError as e:
            await q.edit_message_text(f"❌ {esc(e)}", parse_mode=ParseMode.HTML)
        except Exception:
            logger.exception("Episode list failed")
            await q.edit_message_text("❌ Failed to retrieve episode list.")
        return

    # ---- episode selected -> download ----
    if action == "dl":
        show_id, ep_id = unreg(parts[1]), unreg(parts[2])
        if not show_id or not ep_id:
            await q.message.reply_text("⚠️ Button expired. Please search again.")
            return
        status = await q.message.reply_text("⚡ Generating download link...")
        file_path = None
        try:
            audio_url, raw_title = await fetch_player(show_id, ep_id)
            if not raw_title:
                for ep in cache.get(show_id, {}).get("episodes", []):
                    if str(first(ep, "entity_id", "id", "episode_id")) == ep_id:
                        raw_title = first(ep, "title", "episode_title", "name")
                        break
            raw_title = raw_title or f"Episode_{ep_id[:8]}"
            if not audio_url:
                log_event(uid, "download", f"{show_id}/{ep_id} locked", False)
                await status.edit_text("🔒 This episode is coin-locked or unavailable.")
                return

            file_path = DOWNLOAD_DIR / f"{uid}_{clean_filename(raw_title)}.mp3"
            if ".m3u8" in audio_url:
                await status.edit_text("⏳ Processing HLS stream via FFmpeg...")
                await fetch_hls_stream(audio_url, file_path)
            else:
                await status.edit_text("📥 Downloading audio file...")
                await fetch_direct_audio(audio_url, file_path)

            size_mb = file_path.stat().st_size / (1024 * 1024)
            if size_mb > MAX_UPLOAD_MB:
                log_event(uid, "download", f"{show_id}/{ep_id} too big", False)
                await status.edit_text(f"⚠️ File is {size_mb:.0f}MB, above Telegram's {MAX_UPLOAD_MB}MB bot limit.")
                return

            await status.edit_text("📤 Uploading audio to Telegram...")
            with open(file_path, "rb") as f:
                await q.message.reply_audio(
                    audio=f, title=str(raw_title), performer="Pocket FM",
                    caption=f"🎧 <b>{esc(raw_title)}</b>", parse_mode=ParseMode.HTML,
                    read_timeout=300, write_timeout=300,
                )
            log_event(uid, "download", f"{show_id}/{ep_id}", True)
            await status.delete()

        except ApiError as e:
            log_event(uid, "download", f"{show_id}/{ep_id}", False)
            await status.edit_text(f"❌ {esc(e)}", parse_mode=ParseMode.HTML)
        except Exception as e:
            logger.exception("Download failed")
            log_event(uid, "download", f"{show_id}/{ep_id}", False)
            await status.edit_text(f"❌ Error: {esc(type(e).__name__)}: {esc(str(e)[:200])}", parse_mode=ParseMode.HTML)
        finally:
            if file_path and file_path.exists():
                file_path.unlink()


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
        failed = one("SELECT COUNT(*) FROM events WHERE ok=0")
    await update.message.reply_text(
        "📊 <b>Bot Stats</b>\n\n"
        f"👥 Users: <b>{users}</b> (new today {new_today}, active today {active_today})\n"
        f"🔍 Searches: <b>{searches}</b> (today {searches_today})\n"
        f"⬇️ Downloads: <b>{dl_ok}</b> (today {dl_today})\n"
        f"⚠️ Failed actions: <b>{failed}</b>",
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


async def debugshow_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin only: /debugshow <show_entity_id> dumps raw show-details response."""
    if not is_admin(update) or not context.args:
        return
    sid = context.args[0]
    for p in SHOW_PARAMS:
        try:
            payload = await api_request("GET", "/show-details-story", params={p: sid})
            await update.message.reply_text(f"<b>param={p}</b>\n<pre>{esc(str(payload)[:3300])}</pre>", parse_mode=ParseMode.HTML)
            return
        except Exception as e:
            await update.message.reply_text(f"param={p}: {type(e).__name__}: {e}")


async def debugplay_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin only: /debugplay <show_id> <episode_id> dumps raw player response."""
    if not is_admin(update) or len(context.args) < 2:
        return
    sid, eid = context.args[:2]
    for ks, ke in PLAYER_KEYS:
        try:
            payload = await api_request("POST", "/player", json={ks: sid, ke: eid})
            await update.message.reply_text(f"<b>{ks}/{ke}</b>\n<pre>{esc(str(payload)[:3300])}</pre>", parse_mode=ParseMode.HTML)
            return
        except Exception as e:
            await update.message.reply_text(f"{ks}/{ke}: {type(e).__name__}: {e}")


async def error_handler(update, context: ContextTypes.DEFAULT_TYPE):
    logger.error("Unhandled error", exc_info=context.error)


# ----------------- MAIN -----------------
def main():
    init_db()
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
    app.add_handler(CallbackQueryHandler(callback_handler))
    app.add_error_handler(error_handler)
    logger.info("Bot online. Admins: %s", sorted(ADMIN_IDS) or "none set")
    app.run_polling()


if __name__ == "__main__":
    main()
