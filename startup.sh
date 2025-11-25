#!/bin/bash

set -euo pipefail

echo "Stopping all running services."
./build_run_handle_architecture.sh down db endpoints chatbot 

echo "Running all services needed to perform full scale local testing."

./build_run_handle_architecture.sh up db endpoints chatbot 

docker exec endpoints bash -c "source ~/.zshrc && ./entrypoint.sh" &
docker exec chatbot bash -c "source ~/.zshrc && ./deploy.sh" &