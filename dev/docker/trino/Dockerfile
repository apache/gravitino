#
# Copyright 2023 Datastrato.
# This software is licensed under the Apache License version 2.
#
FROM trinodb/trino:426
LABEL maintainer="support@datastrato.com"

# Only mysql, hudi, iceberg, mariadb, jmx, memory, tpch, tpcds, postgresql, hive plugin are kept
RUN rm -rf /usr/lib/trino/plugin/accumulo  \
    && rm -rf /usr/lib/trino/plugin/blackhole \
    && rm -rf /usr/lib/trino/plugin/delta-lake \
    && rm -rf /usr/lib/trino/plugin/example-http \
    && rm -rf /usr/lib/trino/plugin/geospatial \
    && rm -rf /usr/lib/trino/plugin/kafka \
    && rm -rf /usr/lib/trino/plugin/local-file \
    && rm -rf /usr/lib/trino/plugin/ml \
    && rm -rf /usr/lib/trino/plugin/mysql-event-listener \
    && rm -rf /usr/lib/trino/plugin/phoenix5 \
    && rm -rf /usr/lib/trino/plugin/prometheus \
    && rm -rf /usr/lib/trino/plugin/redshift \
    && rm -rf /usr/lib/trino/plugin/singlestore \
    && rm -rf /usr/lib/trino/plugin/thrift \
    && rm -rf /usr/lib/trino/plugin/atop \
    && rm -rf /usr/lib/trino/plugin/cassandra \
    && rm -rf /usr/lib/trino/plugin/druid \
    && rm -rf /usr/lib/trino/plugin/exchange-filesystem \
    && rm -rf /usr/lib/trino/plugin/google-sheets \
    && rm -rf /usr/lib/trino/plugin/http-event-listener \
    && rm -rf /usr/lib/trino/plugin/ignite \
    && rm -rf /usr/lib/trino/plugin/kinesis \
    && rm -rf /usr/lib/trino/plugin/mongodb \
    && rm -rf /usr/lib/trino/plugin/oracle \
    && rm -rf /usr/lib/trino/plugin/pinot \
    && rm -rf /usr/lib/trino/plugin/raptor-legacy \
    && rm -rf /usr/lib/trino/plugin/resource-group-managers \
    && rm -rf /usr/lib/trino/plugin/sqlserver \
    && rm -rf /usr/lib/trino/plugin/bigquery \
    && rm -rf /usr/lib/trino/plugin/clickhouse \
    && rm -rf /usr/lib/trino/plugin/elasticsearch \
    && rm -rf /usr/lib/trino/plugin/exchange-hdfs \
    && rm -rf /usr/lib/trino/plugin/kudu \
    && rm -rf /usr/lib/trino/plugin/password-authenticators \
    && rm -rf /usr/lib/trino/plugin/redis \
    && rm -rf /usr/lib/trino/plugin/session-property-managers \
    && rm -rf /usr/lib/trino/plugin/teradata-functions

COPY --chown=trino:trino packages/trino/conf /etc/trino
COPY --chown=trino:trino packages/gravitino-trino-connector/mysql-connector-java-8.0.27.jar /usr/lib/trino/plugin/iceberg/
COPY --chown=trino:trino packages/gravitino-trino-connector/postgresql-42.7.0.jar /usr/lib/trino/plugin/iceberg/
COPY --chown=trino:trino packages/gravitino-trino-connector /usr/lib/trino/plugin/gravitino

# Use ARGs to update trino conf and start Trino server
ARG GRAVITINO_HOST_IP
ARG GRAVITINO_HOST_PORT
ARG GRAVITINO_METALAKE_NAME
ARG HIVE_HOST_IP
ENTRYPOINT ["/bin/bash", "-c", "/etc/trino/update-trino-conf.sh && /usr/lib/trino/bin/run-trino"]
