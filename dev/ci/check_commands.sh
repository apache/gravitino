#!/bin/bash
#
# Copyright 2024 Datastrato Pvt Ltd.
# This software is licensed under the Apache License version 2.
#

set -ex

if ! command -v docker; then
    echo "docker could not be found, exiting."
    exit 1
fi

if ! file $(which docker) | grep -q 'executable'; then
    echo "docker is not a executable file, exiting."
    exit 1
fi

if ! command -v docker-proxy; then
    echo "docker-proxy could not be found, exiting."
    exit 1
fi

if ! file $(which docker-proxy) | grep -q 'executable'; then
    echo "docker-proxy is not a executable file, exiting."
    exit 1
fi

# More commands can be added here
echo "All required commands are installed."