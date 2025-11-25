# chatbot:

## environment variables:
```console
RESET_BACKEND: If 1 then will do full reset of db on each application server start. Otherwise will be persisted.
```

## files:
* config.yml: List transformer layers. Custom layers are in custom_components/.
* data/nlu.yml: List intents.
* domain.yml: List responses.