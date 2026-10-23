import os
import re
import asyncio
import logging
import httpx
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    CallbackQueryHandler,
    ContextTypes,
)

# ----------------- CREDENTIALS & CONSTANTS -----------------
TELEGRAM_BOT_TOKEN = "8801885460:AAEWXLULe9sYlp6zyx2R2NvcnUMn_BQftO8"
API_KEY = "api_wgqehrC8nwl8NdKeZSRiCf6Vp1J4kX4D24Gu7slx"
API_BASE = "https://openmarketplace.in/public/index.php/api/pocket-fm-api"

HEADERS = {
    "X-API-KEY": API_KEY,
    "Content-Type": "application/json",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
}

DOWNLOAD_DIR = "downloads"
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)


# ----------------- UTILITY FUNCTIONS -----------------

def clean_filename(name: str) -> str:
    """Strip illegal filesystem characters."""
    return re.sub(r'[\\/*?:"<>|]', "", name).strip()[:70]


async def fetch_direct_audio(url: str, target_path: str):
    """Download direct MP3/M4A audio file stream."""
    async with httpx.AsyncClient(timeout=120.0, follow_redirects=True) as client:
        async with client.stream("GET", url, headers=HEADERS) as resp:
            resp.raise_for_status()
            with open(target_path, "wb") as f:
                async for chunk in resp.aiter_bytes(chunk_size=65536):
                    f.write(chunk)


async def fetch_hls_stream(m3u8_url: str, target_path: str):
    """Mux and convert .m3u8 stream to MP3 via ffmpeg."""
    cmd = [
        "ffmpeg",
        "-y",
        "-i", m3u8_url,
        "-c:a", "libmp3lame",
        "-b:a", "128k",
        target_path
    ]
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE
    )
    stdout, stderr = await proc.communicate()
    if proc.returncode != 0:
        raise RuntimeError(f"FFmpeg error: {stderr.decode(errors='ignore')}")


# ----------------- HANDLERS -----------------

async def start_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = (
        "👋 *Welcome to Pocket FM Bot!*\n\n"
        "Search any audio series using:\n"
        "`/search <show title>`\n\n"
        "Example:\n"
        "`/search Instamillionaire`"
    )
    await update.message.reply_text(msg, parse_mode="Markdown")


async def search_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = " ".join(context.args).strip()
    if not query:
        await update.message.reply_text("⚠️ Please provide a title: `/search <name>`", parse_mode="Markdown")
        return

    status = await update.message.reply_text(f"🔍 Searching for *{query}*...", parse_mode="Markdown")

    async with httpx.AsyncClient(timeout=25.0) as client:
        try:
            resp = await client.get(
                f"{API_BASE}/universal-search",
                headers=HEADERS,
                params={"query": query}
            )
            resp.raise_for_status()
            data = resp.json()

            shows = data.get("data") or data.get("results") or []
            if not shows:
                await status.edit_text("❌ No shows found for that keyword.")
                return

            buttons = []
            for item in shows[:6]:
                title = item.get("title") or item.get("name") or "Story"
                show_id = item.get("id") or item.get("story_id")
                if show_id:
                    buttons.append([InlineKeyboardButton(f"📖 {title[:35]}", callback_data=f"sh_{show_id}")])

            await status.edit_text(
                "Select a show to view available episodes:",
                reply_markup=InlineKeyboardMarkup(buttons)
            )

        except Exception as e:
            logger.error(f"Search API Error: {e}")
            await status.edit_text("❌ Failed to contact Pocket FM API. Check your network or credentials.")


async def callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data

    # Step 1: Show selected -> get episode list
    if data.startswith("sh_"):
        show_id = data.replace("sh_", "")
        await query.edit_message_text("⏳ Fetching episodes list...")

        async with httpx.AsyncClient(timeout=25.0) as client:
            try:
                resp = await client.get(
                    f"{API_BASE}/show-details-story",
                    headers=HEADERS,
                    params={"story_id": show_id}
                )
                resp.raise_for_status()
                details = resp.json().get("data", {})
                episodes = details.get("episodes", [])

                if not episodes:
                    await query.edit_message_text("❌ No episodes available for this show.")
                    return

                buttons = []
                for ep in episodes[:10]:
                    ep_id = ep.get("id") or ep.get("episode_id")
                    ep_title = ep.get("title") or f"Episode {ep.get('episode_number', '')}"
                    buttons.append([InlineKeyboardButton(f"⬇️ {ep_title[:32]}", callback_data=f"dl_{show_id}_{ep_id}")])

                show_name = details.get("title", "Episodes")
                await query.edit_message_text(
                    f"🎧 *{show_name}*\nSelect an episode to download:",
                    reply_markup=InlineKeyboardMarkup(buttons),
                    parse_mode="Markdown"
                )

            except Exception as e:
                logger.error(f"Episode fetch error: {e}")
                await query.edit_message_text("❌ Failed to retrieve episode list.")

    # Step 2: Episode selected -> download and send file
    elif data.startswith("dl_"):
        _, show_id, ep_id = data.split("_")
        status_msg = await query.message.reply_text("⚡ Generating download link...")

        async with httpx.AsyncClient(timeout=25.0) as client:
            file_path = None
            try:
                resp = await client.post(
                    f"{API_BASE}/player",
                    headers=HEADERS,
                    json={"story_id": show_id, "episode_id": ep_id}
                )
                resp.raise_for_status()
                player_data = resp.json().get("data", {})

                audio_url = (
                    player_data.get("media_url")
                    or player_data.get("audio_url")
                    or player_data.get("stream_url")
                )
                raw_title = player_data.get("episode_title") or f"Episode_{ep_id}"
                clean_title = clean_filename(raw_title)

                if not audio_url:
                    await status_msg.edit_text("🔒 This episode is coin-locked or unavailable.")
                    return

                file_path = os.path.join(DOWNLOAD_DIR, f"{query.from_user.id}_{clean_title}.mp3")

                if ".m3u8" in audio_url:
                    await status_msg.edit_text("⏳ Processing HLS stream via FFmpeg...")
                    await fetch_hls_stream(audio_url, file_path)
                else:
                    await status_msg.edit_text("📥 Downloading audio file...")
                    await fetch_direct_audio(audio_url, file_path)

                # Check Telegram 50MB bot upload ceiling
                size_mb = os.path.getsize(file_path) / (1024 * 1024)
                if size_mb > 50.0:
                    await status_msg.edit_text("⚠️ File is larger than 50MB (Telegram bot limit).")
                    return

                await status_msg.edit_text("📤 Uploading audio to Telegram...")

                with open(file_path, "rb") as f:
                    await query.message.reply_audio(
                        audio=f,
                        title=raw_title,
                        performer="Pocket FM",
                        caption=f"🎧 *{raw_title}*",
                        parse_mode="Markdown"
                    )

                await status_msg.delete()

            except Exception as e:
                logger.error(f"Download/Delivery error: {e}")
                await status_msg.edit_text(f"❌ Error downloading file: {e}")

            finally:
                if file_path and os.path.exists(file_path):
                    os.remove(file_path)


# ----------------- EXECUTION -----------------

def main():
    app = (
        ApplicationBuilder()
        .token(TELEGRAM_BOT_TOKEN)
        .read_timeout(60)
        .write_timeout(60)
        .build()
    )

    app.add_handler(CommandHandler("start", start_handler))
    app.add_handler(CommandHandler("search", search_handler))
    app.add_handler(CallbackQueryHandler(callback_handler))

    print("✅ Bot is online and listening for Telegram updates...")
    app.run_polling()


if __name__ == "__main__":
    main()
