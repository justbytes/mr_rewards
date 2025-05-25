#!/bin/bash
# start.sh

# Start the server in the background
python3 server/index.py &

# Give the server a moment to start up
sleep 2

# Start the Telegram bot (this will run in foreground)
python3 server/src/telegram_bot.py &

# Wait for all background processes
wait