# postgres 16.0 + pgvector library installed
FROM pgvector/pgvector:pg16

RUN apt-get update -yqq && \
    apt-get install -y \
    curl \
    dos2unix \
    git \
    procps \
    psmisc \
    build-essential \ 
    man \
    sudo \
    unzip

# Run all sql scripts in the db_objects folder on startup:
RUN rm -r /docker-entrypoint-initdb.d/
COPY ./db/db_objects/* /docker-entrypoint-initdb.d/

ARG IS_LOCAL="0" 
ARG APP_NAME
ARG RESET_BACKEND
ARG ENDPOINTS_HOST
ARG ENDPOINTS_PORT
ARG PG_USERNAME
ARG PG_PASSWORD
ARG PG_HOST
ARG PG_PORT
ARG PG_DBNAME

ENV IS_LOCAL=${IS_LOCAL}
ENV APP_NAME=${APP_NAME}
ENV ENDPOINTS_HOST=${ENDPOINTS_HOST}
ENV ENDPOINTS_PORT=${ENDPOINTS_PORT}
ENV PG_USERNAME=${PG_USERNAME}
ENV PG_PASSWORD=${PG_PASSWORD}
ENV PG_HOST=${PG_HOST}
ENV PG_PORT=${PG_PORT}
ENV PG_DBNAME=${PG_DBNAME}
ENV RESET_BACKEND=${RESET_BACKEND}

RUN echo "export ENDPOINTS_HOST='${ENDPOINTS_HOST}'" >> ~/.bashrc && \
    echo "export ENDPOINTS_HOST='${ENDPOINTS_HOST}'" >> ~/.zshrc && \
    echo "export ENDPOINTS_PORT='${ENDPOINTS_PORT}'" >> ~/.bashrc && \
    echo "export ENDPOINTS_PORT='${ENDPOINTS_PORT}'" >> ~/.zshrc && \
    echo "export PG_USERNAME='${PG_USERNAME}'" >> ~/.bashrc && \
    echo "export PG_USERNAME='${PG_USERNAME}'" >> ~/.zshrc && \
    echo "export PG_PASSWORD='${PG_PASSWORD}'" >> ~/.bashrc && \
    echo "export PG_PASSWORD='${PG_PASSWORD}'" >> ~/.zshrc && \
    echo "export PG_HOST='${PG_HOST}'" >> ~/.bashrc && \
    echo "export PG_HOST='${PG_HOST}'" >> ~/.zshrc && \
    echo "export PG_PORT='${PG_PORT}'" >> ~/.bashrc && \
    echo "export PG_PORT='${PG_PORT}'" >> ~/.zshrc && \
    echo "export PG_DBNAME='${PG_DBNAME}'" >> ~/.bashrc && \
    echo "export PG_DBNAME='${PG_DBNAME}'" >> ~/.zshrc && \
    echo "export RESET_BACKEND='${RESET_BACKEND}'" >> ~/.bashrc && \
    echo "export RESET_BACKEND='${RESET_BACKEND}'" >> ~/.zshrc && \
    echo "export APP_NAME='${APP_NAME}'" >> ~/.bashrc && \
    echo "export APP_NAME='${APP_NAME}'" >> ~/.zshrc && \
    echo "export IS_LOCAL='${IS_LOCAL}'" >> ~/.bashrc && \
    echo "export IS_LOCAL='${IS_LOCAL}'" >> ~/.zshrc && \
    echo "export MIGRATOR_HOME='${MIGRATOR_HOME}'" >> ~/.bashrc && \
    echo "export MIGRATOR_HOME='${MIGRATOR_HOME}'" >> ~/.zshrc 

COPY ./db/logs/ ${MIGRATOR_HOME}/logs/

EXPOSE 5432