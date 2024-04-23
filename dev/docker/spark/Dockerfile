#
# Copyright 2024 Datastrato Pvt Ltd.
# This software is licensed under the Apache License version 2.
#
FROM spark:3.4.1-scala2.12-java11-ubuntu
LABEL maintainer="support@datastrato.com"

#USER root

COPY packages/gravitino-spark-connector-runtime-* /opt/spark/jars/
COPY start-gravitino-sql.sh /opt/spark/
