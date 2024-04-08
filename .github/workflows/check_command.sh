#!/bin/bash

if ! command -v docker &> /dev/null; then
    echo "docker could not be found, exiting."
    exit 1
fi

if ! command -v docker-proxy &> /dev/null; then
    echo "docker-proxy could not be found, exiting."
    exit 1
fi

# More commands can be added here
echo "All required commands are installed."