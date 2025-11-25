ARG PYTHON_VERSION=3.9
FROM python:${PYTHON_VERSION}-slim

ENV HOME=/opt/chatbot
RUN mkdir -p /tmp/model_offload

WORKDIR ${HOME}
ENV PYTHONPATH=${HOME}:${HOME}/chatbot

COPY ./chatbot/requirements.txt .
RUN python3 -m pip install -r requirements.txt
RUN echo 'export PYTHONPATH=${PYTHONPATH}'

COPY ./chatbot/bot/ ./bot/
COPY ./chatbot/test/ ./test/
COPY ./chatbot/deploy.sh .
COPY ./chatbot/clear_cache.sh .

# Install additional packages and yq to assist with fixing yaml:
RUN apt-get update -o Acquire::ForceIPv4=true -yqq \
    && apt-get install jq wget sudo -y --no-install-recommends

ARG ENDPOINTS_HOST
ARG ENDPOINTS_PORT
ARG APP_NAME
ARG PG_USERNAME
ARG PG_PASSWORD
ARG PG_HOST
ARG PG_PORT
ARG PG_DBNAME

ENV ENDPOINTS_HOST=${ENDPOINTS_HOST}
ENV ENDPOINTS_PORT=${ENDPOINTS_PORT}
ENV APP_NAME=${APP_NAME}
ENV PG_USERNAME=${PG_USERNAME}
ENV PG_PASSWORD=${PG_PASSWORD}
ENV PG_HOST=${PG_HOST}
ENV PG_PORT=${PG_PORT}
ENV PG_DBNAME=${PG_DBNAME}

WORKDIR ${HOME}

RUN echo 'export ENDPOINTS_HOST=${ENDPOINTS_HOST}' >> ~/.bashrc && \
    echo 'export ENDPOINTS_HOST=${ENDPOINTS_HOST}' >> ~/.zshrc && \
    echo 'export ENDPOINTS_PORT=${ENDPOINTS_PORT}' >> ~/.bashrc && \
    echo 'export ENDPOINTS_PORT=${ENDPOINTS_PORT}' >> ~/.zshrc && \
    echo 'export APP_NAME=${APP_NAME}' >> ~/.bashrc && \
    echo 'export APP_NAME=${APP_NAME}' >> ~/.zshrc && \
    echo 'export PG_USERNAME=${PG_USERNAME}' >> ~/.bashrc && \
    echo 'export PG_USERNAME=${PG_USERNAME}' >> ~/.zshrc && \
    echo 'export PG_PASSWORD=${PG_PASSWORD}' >> ~/.bashrc && \
    echo 'export PG_PASSWORD=${PG_PASSWORD}' >> ~/.zshrc && \
    echo 'export PG_HOST=${PG_HOST}' >> ~/.bashrc && \
    echo 'export PG_HOST=${PG_HOST}' >> ~/.zshrc && \
    echo 'export PG_PORT=${PG_PORT}' >> ~/.bashrc && \
    echo 'export PG_PORT=${PG_PORT}' >> ~/.zshrc && \
    echo 'export PG_DBNAME=${PG_DBNAME}' >> ~/.bashrc && \
    echo 'export PG_DBNAME=${PG_DBNAME}' >> ~/.zshrc

EXPOSE 5005

#ENTRYPOINT [ '/opt/chatbot/deploy.sh' ]