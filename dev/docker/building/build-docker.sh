#!/bin/bash
#
# Copyright 2024 Datastrato Pvt Ltd.
# This software is licensed under the Apache License version 2.
#
# Set Docker BuildKit
export DOCKER_BUILDKIT=1

# Construct Docker Image
docker build --rm=true -f dev/docker/building/dev-env.Dockerfile -t gravitino/dev-env:main-v1.0 .

docker run -it --rm gravitino/dev-env:main-v1.0 /bin/bash
