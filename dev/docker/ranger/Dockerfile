#
# Copyright 2023 Datastrato Pvt Ltd.
# This software is licensed under the Apache License version 2.
#
# Apache Ranger compile Docker image
FROM debian:buster as compile-ranger
LABEL maintainer="support@datastrato.com"

ARG RANGER_VERSION=2.4.0
# Multiple plugins can be passed using commas, e.g. `plugin-trino,plugin-hive`
ARG RANGER_PLUGINS=plugin-trino

WORKDIR /root

RUN apt-get -q update && \
    apt-get install -y -q python python3 gcc mariadb-server vim curl wget openjdk-11-jdk git procps

RUN wget https://repo.maven.apache.org/maven2/org/apache/maven/apache-maven/3.6.3/apache-maven-3.6.3-bin.tar.gz && \
    tar zxvf apache-maven-3.6.3-bin.tar.gz && \
    ln -s /root/apache-maven-3.6.3/bin/mvn /usr/local/bin/mvn

ENV JAVA_HOME=/usr/local/jdk
RUN ARCH=$(uname -m) && \
    if [ "$ARCH" = "aarch64" ] || [ "$ARCH" = "arm64" ]; then \
    ln -s /usr/lib/jvm/java-11-openjdk-arm64 ${JAVA_HOME}; \
    else \
    ln -s /usr/lib/jvm/java-11-openjdk-amd64 ${JAVA_HOME}; \
    fi

RUN wget https://downloads.apache.org/ranger/${RANGER_VERSION}/apache-ranger-${RANGER_VERSION}.tar.gz && \
    tar zxvf apache-ranger-${RANGER_VERSION}.tar.gz && \
    ln -s apache-ranger-${RANGER_VERSION} apache-ranger && \
    cd apache-ranger && \
    mvn -pl ${RANGER_PLUGINS},jisql,agents-audit,agents-common,agents-cred,agents-installer,credentialbuilder,embeddedwebserver,security-admin,ranger-util,ranger-plugin-classloader,ranger-tools,distro -am -DskipTests=true compile package

# Apache Ranger Admin runtime Docker image
FROM debian:buster
LABEL maintainer="support@datastrato.com"

ARG RANGER_VERSION=2.4.0
# Multiple plugins can be passed using commas, e.g. `plugin-trino,plugin-hive`
ARG RANGER_PLUGINS=plugin-trino
ENV RANGER_PASSWORD=rangerR0cks!

WORKDIR /root

COPY init-mysql.sql.template /tmp/
COPY start-ranger-services.sh /tmp/
RUN chmod +x /tmp/start-ranger-services.sh

RUN apt-get -q update && \
    apt-get install -y -q python python3 gcc mariadb-server vim curl wget openjdk-11-jdk git procps

ENV JAVA_HOME=/usr/local/jdk
RUN ARCH=$(uname -m) && \
    if [ "$ARCH" = "aarch64" ] || [ "$ARCH" = "arm64" ]; then \
    ln -s /usr/lib/jvm/java-11-openjdk-arm64 ${JAVA_HOME}; \
    else \
    ln -s /usr/lib/jvm/java-11-openjdk-amd64 ${JAVA_HOME}; \
    fi

COPY --from=compile-ranger /root/apache-ranger/target/ranger-${RANGER_VERSION}-admin.tar.gz /opt
RUN cd /opt && \
    tar zxvf ranger-${RANGER_VERSION}-admin.tar.gz && \
    ln -s ranger-${RANGER_VERSION}-admin ranger-admin

# Initialize Ranger envirioment
RUN curl -L https://search.maven.org/remotecontent?filepath=mysql/mysql-connector-java/8.0.28/mysql-connector-java-8.0.28.jar --output /opt/ranger-admin/ews/webapp/WEB-INF/lib/mysql-connector-java-8.0.28.jar && \
    cp /opt/ranger-admin/ews/webapp/WEB-INF/lib/mysql-connector-java-8.0.28.jar /opt/ranger-admin/jisql/lib/ && \
    curl -L https://repo1.maven.org/maven2/com/googlecode/log4jdbc/log4jdbc/1.2/log4jdbc-1.2.jar --output /opt/ranger-admin/ews/webapp/WEB-INF/lib/log4jdbc-1.2.jar && \
    cp -r /opt/ranger-admin/ews/webapp/WEB-INF/classes/conf.dist/ /opt/ranger-admin/ews/webapp/WEB-INF/classes/conf && \
    mkdir /opt/ranger-admin/ews/logs

# Clean up
RUN rm -rf /var/lib/apt/lists/*

EXPOSE 6080

ENTRYPOINT ["/bin/bash", "-c", "/tmp/start-ranger-services.sh"]
