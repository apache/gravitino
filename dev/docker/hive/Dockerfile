#
# Copyright 2023 Datastrato Pvt Ltd.
# This software is licensed under the Apache License version 2.
#

FROM ubuntu:16.04
LABEL maintainer="support@datastrato.com"

ARG HADOOP_PACKAGE_NAME
ARG HIVE_PACKAGE_NAME
ARG JDBC_DIVER_PACKAGE_NAME
ARG DEBIAN_FRONTEND=noninteractive

WORKDIR /

################################################################################
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
  mysql-server \
  && rm -rf /var/lib/apt/lists/*

#################################################################################
## setup ssh
RUN mkdir /root/.ssh
RUN cat /dev/zero | ssh-keygen -q -N "" > /dev/null && cat /root/.ssh/id_rsa.pub > /root/.ssh/authorized_keys

COPY packages /tmp/packages

################################################################################
# set environment variables
ENV JAVA_HOME=/usr/local/jdk
ENV HIVE_HOME=/usr/local/hive
ENV HADOOP_HOME=/usr/local/hadoop
ENV HADOOP_HEAPSIZE=128
ENV HADOOP_INSTALL=${HADOOP_HOME}
ENV HADOOP_MAPRED_HOME=${HADOOP_INSTALL}
ENV HADOOP_COMMON_HOME=${HADOOP_INSTALL}
ENV HADOOP_HDFS_HOME=${HADOOP_INSTALL}
ENV HADOOP_CONF_DIR=${HADOOP_HOME}/etc/hadoop
ENV YARN_HOME=${HADOOP_INSTALL}

ENV PATH=${JAVA_HOME}/bin:${HADOOP_HOME}/bin:${HADOOP_INSTALL}/sbin:${HIVE_HOME}/bin:${PATH}
ENV CLASSPATH=${HADOOP_HOME}/lib/*:HIVE_HOME/lib/*:.
ENV LD_LIBRARY_PATH=${HADOOP_HOME}/lib/native

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
RUN echo "PATH=${PATH}" >> /etc/environment
RUN echo "CLASSPATH=${CLASSPATH}" >> /etc/environment
RUN echo "LD_LIBRARY_PATH=${LD_LIBRARY_PATH}" >> /etc/environment

################################################################################
# install hadoop
RUN mkdir ${HADOOP_HOME}
RUN tar -xz -C ${HADOOP_HOME} --strip-components 1 -f /tmp/packages/${HADOOP_PACKAGE_NAME}

# replace configuration templates
RUN rm -f ${HADOOP_CONF_DIR}/core-site.xml
RUN rm -f ${HADOOP_CONF_DIR}/hadoop-env.sh
RUN rm -f ${HADOOP_CONF_DIR}/yarn-env.sh
RUN rm -f ${HADOOP_CONF_DIR}/hdfs-site.xml
RUN rm -f ${HADOOP_CONF_DIR}/mapred-site.xml
RUN rm -f ${HADOOP_CONF_DIR}/yarn-site.xml

ADD core-site.xml ${HADOOP_CONF_DIR}/core-site.xml
ADD hadoop-env.sh ${HADOOP_CONF_DIR}/hadoop-env.sh
ADD yarn-env.sh ${HADOOP_CONF_DIR}/yarn-env.sh
ADD hdfs-site.xml ${HADOOP_CONF_DIR}/hdfs-site.xml
ADD mapred-site.xml ${HADOOP_CONF_DIR}/mapred-site.xml
ADD yarn-site.xml ${HADOOP_CONF_DIR}/yarn-site.xml
ADD check-status.sh /tmp/check-status.sh

# format HFS
RUN ${HADOOP_HOME}/bin/hdfs namenode -format -nonInteractive

################################################################################
# install hive
RUN mkdir ${HIVE_HOME}
RUN tar -xz -C ${HIVE_HOME} --strip-components 1 -f /tmp/packages/${HIVE_PACKAGE_NAME}
ADD hive-site.xml ${HIVE_HOME}/conf/hive-site.xml

################################################################################
# install MySQL
ENV MYSQL_PWD=ds123
RUN echo "mysql-server mysql-server/root_password password ${MYSQL_PWD}" | debconf-set-selections
RUN echo "mysql-server mysql-server/root_password_again password ${MYSQL_PWD}" | debconf-set-selections

RUN chown -R mysql:mysql /var/lib/mysql
RUN usermod -d /var/lib/mysql/ mysql
RUN sed -i "s/.*bind-address.*/bind-address = 0.0.0.0/" /etc/mysql/mysql.conf.d/mysqld.cnf

################################################################################
# add mysql jdbc driver
RUN tar -xz -C ${HIVE_HOME}/lib --strip-components 1 -f /tmp/packages/${JDBC_DIVER_PACKAGE_NAME}

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
# removed install packages
RUN rm -rf /tmp/packages

################################################################################
# expose port
EXPOSE 3306 9000 9083 10000 10002 50070 50075 50010

################################################################################
# create startup script and set ENTRYPOINT
WORKDIR /
ADD start.sh /usr/local/sbin
ENTRYPOINT ["/bin/bash", "/usr/local/sbin/start.sh"]
