import os
import re
import html
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


def log_event(user_id: int, kind: str, detail: str, ok: bool):
    with db() as c:
        c.execute("INSERT INTO events(user_id, kind, detail, ok, ts) VALUES(?,?,?,?,?)",
                  (user_id, kind, detail[:200], int(ok), now()))


# ----------------- API CLIENT -----------------
class ApiError(Exception):
    pass


def make_client(timeout: float = 30.0) -> httpx.AsyncClient:
    # local_address 0.0.0.0 forces IPv4 (fixes VPSes with broken IPv6 where curl still works)
    transport = httpx.AsyncHTTPTransport(local_address="0.0.0.0", retries=2) if FORCE_IPV4 \
        else httpx.AsyncHTTPTransport(retries=2)
    return httpx.AsyncClient(timeout=timeout, transport=transport, follow_redirects=True, headers=HEADERS)


async def api_request(method: str, path: str, *, params=None, json=None):
    url = f"{API_BASE}{path}"
    async with make_client() as client:
        try:
            resp = await client.request(method, url, params=params, json=json)
        except httpx.TimeoutException as e:
            raise ApiError(f"Timeout reaching API ({type(e).__name__})") from e
        except httpx.HTTPError as e:
            raise ApiError(f"Network error: {type(e).__name__}: {e}") from e

    logger.info("%s %s -> %s", method, resp.url, resp.status_code)
    if resp.status_code >= 400:
        logger.error("API body: %s", resp.text[:500])
        raise ApiError(f"API returned HTTP {resp.status_code}")
    try:
        return resp.json()
    except ValueError:
        logger.error("Non-JSON body: %s", resp.text[:500])
        raise ApiError("API did not return JSON")


LIST_KEYS = ("results", "stories", "shows", "items", "list", "episodes", "data")


def find_list(payload):
    """Find a list of dicts in whatever shape the API returns."""
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        for k in LIST_KEYS:
            v = payload.get(k)
            if isinstance(v, list):
                return v
            if isinstance(v, dict):
                found = find_list(v)
                if found:
                    return found
        for v in payload.values():
            if isinstance(v, list) and v and isinstance(v[0], dict):
                return v
    return []


def first(d: dict, *keys, default=None):
    for k in keys:
        if d.get(k) not in (None, ""):
            return d[k]
    return default


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
        payload = await api_request("GET", SEARCH_PATH, params={SEARCH_PARAM: query})
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
            sid = first(item, "id", "story_id", "show_id")
            if sid is not None:
                buttons.append([InlineKeyboardButton(f"📖 {str(title)[:40]}", callback_data=f"sh|{sid}")])
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
    start = page * PAGE_SIZE
    rows = []
    for ep in episodes[start:start + PAGE_SIZE]:
        ep_id = first(ep, "id", "episode_id")
        title = first(ep, "title", "episode_title") or f"Episode {first(ep, 'episode_number', 'number', default='')}"
        rows.append([InlineKeyboardButton(f"⬇️ {str(title)[:38]}", callback_data=f"dl|{show_id}|{ep_id}")])
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"pg|{show_id}|{page - 1}"))
    if start + PAGE_SIZE < len(episodes):
        nav.append(InlineKeyboardButton("Next ➡️", callback_data=f"pg|{show_id}|{page + 1}"))
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
        show_id = parts[1]
        page = int(parts[2]) if action == "pg" else 0
        try:
            if show_id not in cache:
                await q.edit_message_text("⏳ Fetching episodes list...")
                payload = await api_request("GET", "/show-details-story", params={"story_id": show_id})
                details = payload.get("data", payload) if isinstance(payload, dict) else {}
                episodes = find_list(details.get("episodes") if isinstance(details, dict) and "episodes" in details else details)
                name = first(details, "title", "name", default="Episodes") if isinstance(details, dict) else "Episodes"
                cache[show_id] = {"name": name, "episodes": [e for e in episodes if isinstance(e, dict)]}
            entry = cache[show_id]
            if not entry["episodes"]:
                await q.edit_message_text("❌ No episodes available for this show.")
                return
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
        _, show_id, ep_id = parts
        status = await q.message.reply_text("⚡ Generating download link...")
        file_path = None
        try:
            payload = await api_request("POST", "/player", json={"story_id": show_id, "episode_id": ep_id})
            pdata = payload.get("data", payload) if isinstance(payload, dict) else {}
            audio_url = first(pdata, "media_url", "audio_url", "stream_url", "url")
            raw_title = first(pdata, "episode_title", "title", default=f"Episode_{ep_id}")
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
    app.add_handler(CallbackQueryHandler(callback_handler))
    app.add_error_handler(error_handler)
    logger.info("Bot online. Admins: %s", sorted(ADMIN_IDS) or "none set")
    app.run_polling()


if __name__ == "__main__":
    main()
