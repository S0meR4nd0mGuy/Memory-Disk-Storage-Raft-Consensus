#!/bin/bash

# Entrypoint script for Docker container

set -e

echo "Starting Distributed Key-Value Store Node: $NODE_ID"
echo "Listening on $LISTEN_ADDR:$LISTEN_PORT"
echo "API on $API_HOST:$API_PORT"

# Start the node
exec python src/main.py
