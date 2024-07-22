#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
FROM ubuntu:22.04
LABEL maintainer="dev@gravitino.apache.org"

ARG DORIS_VERSION
ARG TARGETARCH

WORKDIR /

################################################################################
# update and install basic tools
RUN apt-get update -y &&  DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
    patchelf gdb binutils binutils-common mysql-client \
    curl wget less vim htop iproute2 numactl jq iotop sysstat tzdata xz-utils \
    tcpdump iputils-ping dnsutils strace lsof blktrace \
    bpfcc-tools linux-headers-realtime linux-tools-realtime silversearcher-ag \
    net-tools netcat openjdk-8-jdk && \
    rm -rf /var/lib/apt/lists/*

################################################################################
# set environment variables
ENV JAVA_HOME=/usr/local/jdk
ENV DORIS_HOME=/opt/apache-doris
ENV DORIS_FE_HOME=${DORIS_HOME}/fe
ENV DORIS_BE_HOME=${DORIS_HOME}/be

ENV PATH=${JAVA_HOME}/bin:${DORIS_FE_HOME}/bin:${DORIS_BE_HOME}/bin:${PATH}

################################################################################
# setup java

RUN ARCH=$(uname -m) && \
  if [ "$ARCH" = "aarch64" ] || [ "$ARCH" = "arm64" ]; then \
    ln -s /usr/lib/jvm/java-8-openjdk-arm64 ${JAVA_HOME}; \
  else \
    ln -s /usr/lib/jvm/java-8-openjdk-amd64 ${JAVA_HOME}; \
  fi

#################################################################################
## add files
ADD packages/doris-${TARGETARCH}.tar.xz /opt/
RUN ln -s /opt/apache-doris-${DORIS_VERSION}-bin-* ${DORIS_HOME}

COPY start.sh ${DORIS_HOME}

################################################################################
# set WORKDIR/EXPOSE/CMD

WORKDIR ${DORIS_HOME}

EXPOSE 8030
EXPOSE 9030

CMD ["bash", "start.sh"]
