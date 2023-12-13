#
# Copyright 2023 Datastrato Pvt Ltd.
# This software is licensed under the Apache License version 2.
#
FROM openjdk:8-jdk-buster
LABEL maintainer="support@datastrato.com"

WORKDIR /root/gravitino

COPY packages/gravitino /root/gravitino

EXPOSE 8090
EXPOSE 9001

ENTRYPOINT ["/bin/bash", "/root/gravitino/bin/gravitino.sh", "start"]