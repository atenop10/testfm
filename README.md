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

## Episode pages (8 per page)
The API returns ~8 episodes per request. After you pick a show, the bot now fetches **all** pages
(auto-detects `page` / `offset`) so you can type any number: `340`, `340-345`, `1,5,10-12`.
First load of a long show takes a while (rate-limited, ~1 call/sec); the list is cached for 1 hour.
If it still only sees 8 episodes, as admin run `/debugshow <show_id> page 2` (or `offset 8`) to see
which param works, then set `EPISODE_PAGE_PARAM` / `EPISODE_PAGE_MODE` in `.env`.
