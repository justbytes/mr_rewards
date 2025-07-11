#!/bin/bash
# start.sh

echo "🎯 Starting Mr. Rewards Backend & Bot..."

# Start the FastAPI server in the background
echo "🚀 Starting FastAPI server..."
python3 server/server.py &
SERVER_PID=$!

# Give the server a moment to start up
echo "⏳ Waiting for server to initialize..."
sleep 10

# Start the Telegram bot
echo "🤖 Starting Telegram bot..."
python3 server/telegram_bot.py &
BOT_PID=$!

echo "✅ Both services started successfully!"
echo "📡 FastAPI server PID: $SERVER_PID"
echo "🤖 Telegram bot PID: $BOT_PID"
echo "Press Ctrl+C to stop both services"

# Function to handle cleanup on exit
cleanup() {
    echo ""
    echo "🛑 Shutting down services..."
    kill $SERVER_PID $BOT_PID 2>/dev/null
    echo "✅ All services stopped"
    exit 0
}

# Set up signal handlers
trap cleanup SIGINT SIGTERM

# Wait for all background processes
wait