#
# Copyright 2024 Datastrato Pvt Ltd.
# This software is licensed under the Apache License version 2.
#

# copy jdk for building
#ADD https://cnbj1-fds.api.xiaomi.net/gravitino/amazon-corretto-17.0.10.8.1-linux-x64.tar.gz ./amazon-corretto-17.0.10.8.1-linux-x64.tar.gz
#ADD https://cnbj1-fds.api.xiaomi.net/gravitino/jdk-8u152-linux-x64.tar.gz ./jdk-8u152-linux-x64.tar.gz
#RUN tar -zxvf ./amazon-corretto-17.0.10.8.1-linux-x64.tar.gz
#RUN tar -zxvf ./jdk-8u152-linux-x64.tar.gz

FROM micr.cloud.mioffice.cn/container/xiaomi_centos7:openjdk1.8
ENV TZ "Asia/Shanghai"
ARG HERA_AGENT_PATH=/home/work/app/mitelemetry/agent/opentelemetry-javaagent-all.jar
ARG MYSQL_DRIVER_PATH=/opt/gravitino/libs/mysql-connector-java-5.1.49.jar

COPY entrypoint.sh /opt/entrypoint.sh
COPY distribution/package /opt/gravitino
ADD https://pkgs.d.xiaomi.net/artifactory/aliyun-maven-central/mysql/mysql-connector-java/5.1.49/mysql-connector-java-5.1.49.jar $MYSQL_DRIVER_PATH
ADD https://pkgs.d.xiaomi.net/artifactory/releases/io/opentelemetry/javaagent/opentelemetry-javaagent/1.13.1-milatest/opentelemetry-javaagent-1.13.1-milatest.jar $HERA_AGENT_PATH

# aliyun yum source
RUN yum install -y wget
RUN wget -O /etc/yum.repos.d/CentOS-Base.repo http://mirrors.aliyun.com/repo/Centos-7.repo
RUN yum clean all && yum makecache fast

# Install Kerberos client
RUN yum install -y krb5-workstation krb5-libs

RUN chmod 644 /etc/krb5.conf
RUN chmod +x /opt/entrypoint.sh
RUN chmod -R +x /opt/gravitino

ENTRYPOINT ["/opt/entrypoint.sh"]