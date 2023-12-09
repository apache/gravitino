#
# Copyright 2023 Datastrato.
# This software is licensed under the Apache License version 2.
#

FROM ubuntu:22.04
LABEL maintainer="support@datastrato.com"

WORKDIR /root

RUN apt-get update
RUN apt-get upgrade -y
RUN apt-get install --fix-missing -yq git \
  wget \
  nginx \
  openjdk-17-jdk
RUN git clone --branch 1.0.3 https://github.com/spring-projects/spring-authorization-server.git
COPY conf/nginx.conf /etc/nginx/
COPY conf/oauth2-registered-client-schema.sql spring-authorization-server/oauth2-authorization-server/src/main/resources/org/springframework/security/oauth2/server/authorization/client/
COPY start.sh /root/start.sh
RUN chmod 777 /root/start.sh
EXPOSE 8077
RUN mkdir /usr/share/nginx/logs/
RUN chmod 777 /usr/share/nginx/logs/
WORKDIR /root/spring-authorization-server
RUN ./gradlew build -x test
ENTRYPOINT ["/bin/bash", "-c", "/root/start.sh"]

