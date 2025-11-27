# rasa-chatbot

## Description:
Chatbot that initiates transactions and resolves disputes using the rasa
library.

## Commands:
To demonstrate functionality, run the below commands:

```console
docker compose -f docker-compose.yml build db endpoints chatbot
docker compose -f docker-compose.yml up -d db chatbot

```