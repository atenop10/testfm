# Pocket FM Telegram Bot

## Deploy on a VPS (Ubuntu/Debian)
```bash
git clone <your-repo-url> pocketfm_bot && cd pocketfm_bot
chmod +x setup.sh && ./setup.sh
nano .env                      # fill token, API key, ADMIN_IDS
sudo systemctl restart pocketfm-bot
journalctl -u pocketfm-bot -f  # live logs
```

## Commands
- `/search <title>` - pick a show, then send episode numbers like `340` or `340-344` or `1,5,10-12`
- `/cancel` - clear the selected show
- `/start` - everyone
- `/stats`, `/users`, `/broadcast <text>`, `/debug <query>` - admins only (ADMIN_IDS)

## If search fails
Run `/debug <title>` as admin to see the raw API response, and check logs.
Match `SEARCH_PATH` / `SEARCH_PARAM` in `.env` to your working curl command.

## Episode pages (8 per page) and API usage
The API returns ~8 episodes per request, so the bot only fetches the pages it needs.
- Picking a show: 1 call (0 if cached).
- Typing `340-345`: fetches just the page(s) holding those episodes (usually 1-2 calls), then 1 `/player` call per episode.
- Everything is cached in `bot.db` (survives restarts): 6h for oldest-first shows, 1h for newest-first ones.
- `/stats` (admin) shows API calls total/today so you can watch usage.

If it can't find the page parameter, run `/debugshow <show_id> page 2` (or `offset 8`) as admin,
then pin it in `.env` (`EPISODE_PAGE_PARAM`, `EPISODE_PAGE_MODE`). Run `/debugshow <show_id>` once and
check whether the response includes a total episode count; if it does not AND the list is newest-first,
the bot has to walk all pages once for that show (many calls, then cached).
