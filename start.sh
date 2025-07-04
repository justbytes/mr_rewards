#!/bin/bash
# start.sh

# Start the server in the background
python3 server/index.py &

# Give the server a moment to start up
sleep 1

# Start the Telegram bot
python3 server/src/telegram_bot.py &

# Wait for all background processes
wait