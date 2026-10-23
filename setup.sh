#!/usr/bin/env bash
set -e
echo "=== 1. Installing system packages ==="
sudo apt-get update -y
sudo apt-get install -y python3 python3-pip python3-venv ffmpeg

echo "=== 2. Virtual environment + dependencies ==="
[ -d venv ] || python3 -m venv venv
source venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt

echo "=== 3. .env file ==="
if [ ! -f .env ]; then
  cp .env.example .env
  echo "Created .env - EDIT IT NOW (nano .env), then run: sudo systemctl restart pocketfm-bot"
fi

echo "=== 4. systemd service ==="
CURRENT_DIR=$(pwd)
CURRENT_USER=$(whoami)
SERVICE_NAME="pocketfm-bot"
sudo tee /etc/systemd/system/${SERVICE_NAME}.service >/dev/null <<UNIT
[Unit]
Description=Pocket FM Telegram Bot
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=${CURRENT_USER}
WorkingDirectory=${CURRENT_DIR}
ExecStart=${CURRENT_DIR}/venv/bin/python ${CURRENT_DIR}/bot.py
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
UNIT

sudo systemctl daemon-reload
sudo systemctl enable ${SERVICE_NAME}
sudo systemctl restart ${SERVICE_NAME}
echo "Done. Logs: journalctl -u ${SERVICE_NAME} -f"
