#!/bin/bash

set -euo pipefail

echo "Stopping all running services."
docker compose down db endpoints chatbot 

echo "Running all services needed to perform full scale local testing."

docker compose up db endpoints chatbot 

docker exec endpoints bash -c "source ~/.zshrc && ./entrypoint.sh" &
docker exec chatbot bash -c "source ~/.zshrc && ./deploy.sh" &