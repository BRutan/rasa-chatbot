#!/bin/bash

export PYTHONPATH=${HOME}:${PYTHONPATH}
#export PYTHONUNBUFFERED=1

cd $HOME && /usr/local/bin/uvicorn main:app --host 0.0.0.0 --port 8000