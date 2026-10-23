#!/usr/bin/env bash
set -e

echo "=== 1. Updating packages and installing FFmpeg & Python venv ==="
sudo apt-get update -y
sudo apt-get install -y python3 python3-pip python3-venv ffmpeg

echo "=== 2. Creating Python virtual environment ==="
if [ ! -d "venv" ]; then
    python3 -m venv venv
    echo "Virtual environment created."
fi

echo "=== 3. Installing Python dependencies ==="
source venv/bin/activate
pip install --upgrade pip
pip install python-telegram-bot httpx

echo "=== 4. Creating systemd background service ==="
CURRENT_DIR=$(pwd)
CURRENT_USER=$(whoami)
SERVICE_NAME="pocketfm-bot"

sudo bash -c "cat <<EOF > /etc/systemd/system/${SERVICE_NAME}.service
[Unit]
Description=Pocket FM Telegram Bot Service
After=network.target

[Service]
Type=simple
User=${CURRENT_USER}
WorkingDirectory=${CURRENT_DIR}
ExecStart=${CURRENT_DIR}/venv/bin/python ${CURRENT_DIR}/bot.py
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF"

echo "=== 5. Enabling and starting service ==="
sudo systemctl daemon-reload
sudo systemctl enable ${SERVICE_NAME}
sudo systemctl restart ${SERVICE_NAME}

echo "=== Setup complete! Service is active. ==="
sudo systemctl status ${SERVICE_NAME} --no-pager
