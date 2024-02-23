#
# Copyright 2024 Datastrato Pvt Ltd.
# This software is licensed under the Apache License version 2.
#
FROM ubuntu:22.04
LABEL maintainer="support@datastrato.com"

ARG DORIS_VERSION

WORKDIR /

################################################################################
# update and install basic tools
RUN apt-get update -y &&  DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
    patchelf gdb binutils binutils-common mysql-client \
    curl wget less vim htop iproute2 numactl jq iotop sysstat tzdata xz-utils \
    tcpdump iputils-ping dnsutils strace lsof blktrace \
    bpfcc-tools linux-headers-realtime linux-tools-realtime silversearcher-ag \
    net-tools openjdk-8-jdk && \
    rm -rf /var/lib/apt/lists/*

################################################################################
# set environment variables
ENV JAVA_HOME=/usr/local/jdk
ENV DORIS_HOME=/opt/apache-doris
ENV DORIS_FE_HOME=${DORIS_HOME}/fe
ENV DORIS_BE_HOME=${DORIS_HOME}/be

ENV PATH=${JAVA_HOME}/bin:${DORIS_FE_HOME}/bin:${DORIS_BE_HOME}/bin:${PATH}

RUN mkdir ${DORIS_HOME}
COPY packages /tmp/packages
COPY start.sh ${DORIS_HOME}

################################################################################
# setup java, unzip doris package

RUN ARCH=$(uname -m) && \
  if [ "$ARCH" = "aarch64" ] || [ "$ARCH" = "arm64" ]; then \
    ln -s /usr/lib/jvm/java-8-openjdk-arm64 ${JAVA_HOME}; \
    tar -xv -C "${DORIS_HOME}" --strip-components 1 -f /tmp/packages/apache-doris-${DORIS_VERSION}-bin-arm64.tar.xz; \
  else \
    ln -s /usr/lib/jvm/java-8-openjdk-amd64 ${JAVA_HOME}; \
    tar -xv -C "${DORIS_HOME}" --strip-components 1 -f /tmp/packages/apache-doris-${DORIS_VERSION}-bin-x64.tar.xz; \
  fi

################################################################################
# set WORKDIR/EXPOSE/CMD

WORKDIR ${DORIS_HOME}

EXPOSE 8030
EXPOSE 9030

CMD ["bash", "start.sh"]