ARG PYTHON_VERSION=3.9
FROM python:${PYTHON_VERSION}-slim

ENV HOME=/opt/endpoints

# Install system dependencies:
RUN apt-get update -o Acquire::ForceIPv4=true -yqq \
    && apt-get -o Acquire::ForceIPv4=true install -y --no-install-recommends \
    gcc \
    curl \
    build-essential \
    dos2unix \
    git && rm -rf /var/lib/apt/lists/* && \
    apt update && apt install netcat-traditional

WORKDIR ${HOME}

# Install gcloud sdk:
RUN curl -sSL https://sdk.cloud.google.com | bash -s -- --disable-prompts

# Install Python dependencies:
COPY ./endpoints/requirements.txt .
RUN pip install --upgrade pip && \
    pip install -r requirements.txt

ARG PYTHON_VERSION
ARG APP_NAME="endpoints"
ARG IS_LOCAL="1"
ARG FASTAPI_PORT
ARG PG_USERNAME
ARG PG_PASSWORD
ARG PG_HOST
ARG PG_PORT
ARG PG_DBNAME
ARG RASA_HOST
ARG RASA_PORT

ENV IS_LOCAL=${IS_LOCAL}
ENV PYTHONPATH=${HOME}
ENV FASTAPI_PORT=${FASTAPI_PORT}
ENV PG_USERNAME=${PG_USERNAME}
ENV PG_PASSWORD=${PG_PASSWORD}
ENV PG_HOST=${PG_HOST}
ENV PG_PORT=${PG_PORT}
ENV PG_DBNAME=${PG_DBNAME}
ENV RASA_HOST=${RASA_HOST}
ENV RASA_PORT=${RASA_PORT}
ENV EVIDENCE_DIR="${HOME}/evidence"
ENV TRANS_DOC_DIR="${HOME}/transactions/documents"
ENV USER_ID_DIR="${HOME}/users/identification"

RUN echo "export APP_NAME=${APP_NAME}" >> ~/.bashrc && \
    echo "export APP_NAME=${APP_NAME}" >> ~/.zshrc && \
    echo "export PG_USERNAME=${PG_USERNAME}" >> ~/.bashrc && \
    echo "export PG_USERNAME=${PG_USERNAME}" >> ~/.zshrc && \
    echo "export PG_PASSWORD=${PG_PASSWORD}" >> ~/.bashrc && \
    echo "export PG_PASSWORD=${PG_PASSWORD}" >> ~/.zshrc && \
    echo "export PG_HOST=${PG_HOST}" >> ~/.bashrc && \
    echo "export PG_HOST=${PG_HOST}" >> ~/.zshrc && \
    echo "export PG_PORT=${PG_PORT}" >> ~/.bashrc && \
    echo "export PG_PORT=${PG_PORT}" >> ~/.zshrc && \
    echo "export PG_DBNAME=${PG_DBNAME}" >> ~/.bashrc && \
    echo "export PG_DBNAME=${PG_DBNAME}" >> ~/.zshrc && \
    echo "export RASA_HOST=${RASA_HOST}" >> ~/.bashrc && \
    echo "export RASA_HOST=${RASA_HOST}" >> ~/.zshrc && \
    echo "export RASA_PORT=${RASA_PORT}" >> ~/.bashrc && \
    echo "export RASA_PORT=${RASA_PORT}" >> ~/.zshrc && \
    echo "export PYTHONPATH=${PYTHONPATH}" >> ~/.bashrc && \
    echo "export PYTHONPATH=${PYTHONPATH}" >> ~/.zshrc && \
    echo "export HOME=${HOME}" >> ~/.bashrc && \
    echo "export HOME=${HOME}" >> ~/.zshrc && \
    echo "export EVIDENCE_DIR=${EVIDENCE_DIR}" >> ~/.bashrc && \
    echo "export EVIDENCE_DIR=${EVIDENCE_DIR}" >> ~/.zshrc && \
    echo "export TRANS_DOC_DIR=${TRANS_DOC_DIR}" >> ~/.bashrc && \
    echo "export TRANS_DOC_DIR=${TRANS_DOC_DIR}" >> ~/.zshrc && \
    echo "export USER_ID_DIR=${USER_ID_DIR}" >> ~/.bashrc && \
    echo "export USER_ID_DIR=${USER_ID_DIR}" >> ~/.zshrc && \
    echo "alias start-endpoints='fastapi dev endpoints/main.py --host 0.0.0.0 --port ${FASTAPI_PORT}'" >> ~/.bashrc && \
    echo "alias start-endpoints='fastapi dev endpoints/main.py --host 0.0.0.0 --port ${FASTAPI_PORT}'" >> ~/.zshrc

EXPOSE ${FASTAPI_PORT}

RUN touch __init__.py && \
    mkdir -p /var/log/supervisor && \
    mkdir -p ${EVIDENCE_DIR} && \
    mkdir -p ${TRANS_DOC_DIR} && \
    mkdir -p ${USER_ID_DIR}

COPY ./endpoints/endpoints/ ./endpoints/
COPY ./endpoints/logs/ /app/logs/
COPY ./endpoints/entrypoint.sh .
COPY ./shared/ ./objects/
ADD ./data/transactions/ ${TRANS_DOC_DIR}
ADD ./data/users/identification ${USER_ID_DIR}
ADD ./data/evidence/ ${EVIDENCE_DIR}
RUN chmod +x entrypoint.sh && dos2unix entrypoint.sh

#ENTRYPOINT [ "./entrypoint.sh" ]