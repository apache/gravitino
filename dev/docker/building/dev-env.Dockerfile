#
# Copyright 2024 Datastrato Pvt Ltd.
# This software is licensed under the Apache License version 2.
#
FROM openjdk:8-jdk-buster
LABEL maintainer="support@datastrato.com"

WORKDIR /root/gravitino

COPY Dockerfile /root/gravitino

RUN ./gradlew assembleDistribution -x test
