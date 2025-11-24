#!/bin/bash

cd /opt/chatbot/chatbot
find . -name "__pycache__" -type d -exec rm -rf {} +
rm -rf models/*
rm -rf .rasa/cache/*
