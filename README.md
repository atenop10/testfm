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
- `/start`, `/search <title>` - everyone
- `/stats`, `/users`, `/broadcast <text>`, `/debug <query>` - admins only (ADMIN_IDS)

## If search fails
Run `/debug <title>` as admin to see the raw API response, and check logs.
Match `SEARCH_PATH` / `SEARCH_PARAM` in `.env` to your working curl command.
