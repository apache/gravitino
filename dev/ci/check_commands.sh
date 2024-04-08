#!/bin/bash
#
# Copyright 2024 Datastrato Pvt Ltd.
# This software is licensed under the Apache License version 2.
#

if ! command -v docker &> /dev/null || file $(which docker) | grep -q 'executable';  then
    echo "docker could not be found or is not a real executable file, exiting."
    exit 1
fi

if ! command -v docker-proxy &> /dev/null || ! file $(which docker-proxy) | grep -q 'executable'; then
    echo "docker-proxy could not be found or is not a real executable file, exiting."
    exit 1
fi

# More commands can be added here
echo "All required commands are installed."