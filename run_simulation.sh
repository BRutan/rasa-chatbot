#!/bin/bash

set -euo pipefail

echo "Stopping all running services."
docker compose down db endpoints chatbot 

echo "Running all services needed to perform full scale local testing."
docker compose build db endpoints chatbot
docker compose up -d db endpoints chatbot 

docker exec chatbot -it './clear_cache.sh && cd bot && rasa train'
docker exec chatbot -it './deploy.sh'

docker exec endpoints -it 'python tests/test_transaction.py'