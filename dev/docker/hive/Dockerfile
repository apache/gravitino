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

FROM ubuntu:16.04
LABEL maintainer="dev@gravitino.apache.org"

ARG HADOOP2_VERSION
ARG HADOOP3_VERSION
ARG HIVE2_VERSION
ARG HIVE3_VERSION
ARG ZOOKEEPER_VERSION
ARG RANGER_VERSION
ARG MYSQL_JDBC_DRIVER_VERSION
ARG DEBIAN_FRONTEND=noninteractive

WORKDIR /

################################################################################
# prepare MySQL installation
ENV MYSQL_PWD=ds123
RUN echo "mysql-server mysql-server/root_password password ${MYSQL_PWD}" | debconf-set-selections
RUN echo "mysql-server mysql-server/root_password_again password ${MYSQL_PWD}" | debconf-set-selections

###############################################################################
# update and install basic tools
RUN apt-get update && apt-get upgrade -y && apt-get install --fix-missing -yq \
  git \
  libkrb5-dev \
  libmysqlclient-dev \
  libssl-dev \
  libsasl2-dev \
  libsasl2-modules-gssapi-mit \
  libsqlite3-dev \
  libtidy-0.99-0 \
  libxml2-dev \
  libxslt-dev \
  libffi-dev \
  libldap2-dev \
  python-dev \
  python-setuptools \
  libgmp3-dev \
  libz-dev \
  curl \
  software-properties-common \
  vim \
  openssh-server \
  wget \
  sudo \
  openjdk-8-jdk \
  mysql-server

# setup mysql configuration
RUN chown -R mysql:mysql /var/lib/mysql
RUN usermod -d /var/lib/mysql/ mysql
RUN sed -i "s/.*bind-address.*/bind-address = 0.0.0.0/" /etc/mysql/mysql.conf.d/mysqld.cnf

#################################################################################
# setup ssh
RUN mkdir /root/.ssh
RUN cat /dev/zero | ssh-keygen -q -N "" > /dev/null && cat /root/.ssh/id_rsa.pub > /root/.ssh/authorized_keys

################################################################################
# set environment variables
ENV JAVA_HOME=/usr/local/jdk
ENV HIVE_HOME=/usr/local/hive
ENV HIVE2_HOME=/usr/local/hive2
ENV HIVE3_HOME=/usr/local/hive3
ENV HIVE_CONF_DIR=${HIVE_HOME}/conf
ENV HIVE_TMP_CONF_DIR=/tmp/hive-conf
ENV HADOOP_HOME=/usr/local/hadoop
ENV HADOOP2_HOME=/usr/local/hadoop2
ENV HADOOP3_HOME=/usr/local/hadoop3
ENV HADOOP_HEAPSIZE=512
ENV HADOOP_INSTALL=${HADOOP_HOME}
ENV HADOOP_MAPRED_HOME=${HADOOP_INSTALL}
ENV HADOOP_COMMON_HOME=${HADOOP_INSTALL}
ENV HADOOP_HDFS_HOME=${HADOOP_INSTALL}
ENV HADOOP_CONF_DIR=${HADOOP_HOME}/etc/hadoop
ENV HADOOP_TMP_CONF_DIR=/tmp/hadoop-conf
ENV YARN_HOME=${HADOOP_INSTALL}
ENV ZK_HOME=/usr/local/zookeeper
ENV RANGER_HDFS_PLUGIN_HOME=/usr/local/ranger-hdfs-plugin
ENV RANGER_HIVE_PLUGIN_HOME=/usr/local/ranger-hive-plugin
ENV PATH=${JAVA_HOME}/bin:${HADOOP_HOME}/bin:${HADOOP_INSTALL}/sbin:${HIVE_HOME}/bin:${PATH}
ENV CLASSPATH=${HADOOP_HOME}/lib/*:HIVE_HOME/lib/*:.
ENV LD_LIBRARY_PATH=${HADOOP_HOME}/lib/native
ENV MYSQL_JDBC_DRIVER_VERSION=${MYSQL_JDBC_DRIVER_VERSION}

################################################################################
# add the above env for all users
RUN ARCH=$(uname -m) && \
  if [ "$ARCH" = "aarch64" ] || [ "$ARCH" = "arm64" ]; then \
    ln -s /usr/lib/jvm/java-8-openjdk-arm64 ${JAVA_HOME}; \
  else \
    ln -s /usr/lib/jvm/java-8-openjdk-amd64 ${JAVA_HOME}; \
  fi

RUN echo "JAVA_HOME=${JAVA_HOME}" >> /etc/environment
RUN echo "HADOOP_HEAPSIZE=${HADOOP_HEAPSIZE}" >> /etc/environment
RUN echo "HADOOP_HOME=${HADOOP_HOME}" >> /etc/environment
RUN echo "HADOOP_INSTALL=${HADOOP_INSTALL}" >> /etc/environment
RUN echo "HADOOP_MAPRED_HOME=${HADOOP_MAPRED_HOME}" >> /etc/environment
RUN echo "HADOOP_COMMON_HOME=${HADOOP_COMMON_HOME}" >> /etc/environment
RUN echo "HADOOP_HDFS_HOME=${HADOOP_HDFS_HOME}" >> /etc/environment
RUN echo "HADOOP_CONF_DIR=${HADOOP_CONF_DIR}" >> /etc/environment
RUN echo "HADOOP_CLASSPATH=${JAVA_HOME}/lib/tools.jar" >> /etc/environment
RUN echo "YARN_HOME=${YARN_HOME}" >> /etc/environment
RUN echo "HIVE_HOME=${HIVE_HOME}" >> /etc/environment
RUN echo "ZK_HOME=${ZK_HOME}" >> /etc/environment
RUN echo "PATH=${PATH}" >> /etc/environment
RUN echo "CLASSPATH=${CLASSPATH}" >> /etc/environment
RUN echo "LD_LIBRARY_PATH=${LD_LIBRARY_PATH}" >> /etc/environment

################################################################################
# install zookeeper
ADD packages/zookeeper-${ZOOKEEPER_VERSION}.tar.gz /opt/
RUN ln -s /opt/zookeeper-${ZOOKEEPER_VERSION} ${ZK_HOME}
RUN mkdir ${ZK_HOME}/data

################################################################################
# install ranger hadoop plugin
ADD packages/ranger-${RANGER_VERSION}-hdfs-plugin.tar.gz /opt/
RUN ln -s /opt/ranger-${RANGER_VERSION}-hdfs-plugin ${RANGER_HDFS_PLUGIN_HOME}

################################################################################
# install ranger hive plugin
ADD packages/ranger-${RANGER_VERSION}-hive-plugin.tar.gz /opt/
RUN ln -s /opt/ranger-${RANGER_VERSION}-hive-plugin ${RANGER_HIVE_PLUGIN_HOME}

################################################################################
# install hadoop2 and hadoop3
ADD packages/hadoop-${HADOOP2_VERSION}.tar.gz /opt/
RUN ln -s /opt/hadoop-${HADOOP2_VERSION} ${HADOOP2_HOME}
ADD packages/hadoop-${HADOOP3_VERSION}.tar.gz /opt/
RUN ln -s /opt/hadoop-${HADOOP3_VERSION} ${HADOOP3_HOME}

# Add hadoop configuration to temporary directory
ADD core-site.xml ${HADOOP_TMP_CONF_DIR}/core-site.xml
ADD hadoop-env.sh ${HADOOP_TMP_CONF_DIR}/hadoop-env.sh
ADD hdfs-site.xml ${HADOOP_TMP_CONF_DIR}/hdfs-site.xml
ADD mapred-site.xml ${HADOOP_TMP_CONF_DIR}/mapred-site.xml
ADD check-status.sh /tmp/check-status.sh

################################################################################
# install hive2 and hive3
ADD packages/apache-hive-${HIVE2_VERSION}-bin.tar.gz /opt/
RUN ln -s /opt/apache-hive-${HIVE2_VERSION}-bin ${HIVE2_HOME}
ADD packages/apache-hive-${HIVE3_VERSION}-bin.tar.gz /opt/
RUN ln -s /opt/apache-hive-${HIVE3_VERSION}-bin ${HIVE3_HOME}

# Add hive configuration to temporary directory
ADD hive-site.xml ${HIVE_TMP_CONF_DIR}/hive-site.xml

################################################################################
# add mysql jdbc driver
ADD packages/mysql-connector-java-${MYSQL_JDBC_DRIVER_VERSION}.tar.gz /opt/

################################################################################
# add users and groups
RUN groupadd hdfs && groupadd hadoop && groupadd hive && groupadd mapred

RUN useradd -g hadoop datastrato && echo "datastrato:ds123" | chpasswd && adduser datastrato sudo
RUN usermod -s /bin/bash datastrato

RUN usermod -a -G hdfs datastrato
RUN usermod -a -G hadoop datastrato
RUN usermod -a -G hive datastrato
RUN usermod -a -G mapred datastrato

RUN mkdir /home/datastrato
RUN chown -R datastrato:hadoop /home/datastrato

################################################################################
# removed install packages and cache
RUN rm -rf /tmp/packages
RUN rm -rf /var/lib/apt/lists/*

################################################################################
# expose port
EXPOSE 2181 3306 9000 9083 10000 10002 50070 50075 50010

################################################################################
# create startup script and set ENTRYPOINT
WORKDIR /
ADD start.sh /usr/local/sbin
ENTRYPOINT ["/bin/bash", "/usr/local/sbin/start.sh"]
