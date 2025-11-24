#!/bin/bash
set -euo pipefail

export PYTHONPATH=/opt/chatbot:/opt/chatbot/chatbot:$PYTHONPATH

# Path to your local model directory
MODEL_DIR="./chatbot/models"

# Start the Rasa actions server in the background
echo "🚀 Starting Rasa actions server..."
rasa run actions --debug > action_logs.txt 2>&1 &

# Capture its PID so we can kill it if needed
ACTIONS_PID=$!

# Start the Rasa server pointing to your model folder
echo "🚀 Starting Rasa server..."
rasa run --model "$MODEL_DIR" --enable-api --cors "*" --endpoints ./chatbot/endpoints.yml

# When the Rasa server exits, stop the actions server
echo "🧹 Stopping Rasa actions server..."
kill $ACTIONS_PID 2>/dev/null || true