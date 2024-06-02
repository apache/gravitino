#
# Copyright 2023 Datastrato Pvt Ltd.
# This software is licensed under the Apache License version 2.
#
FROM trinodb/trino:435
LABEL maintainer="support@datastrato.com"

USER root

COPY --chown=trino:trino packages/trino/conf /etc/trino
COPY --chown=trino:trino packages/gravitino-trino-connector/mysql-connector-java-8.0.27.jar /usr/lib/trino/plugin/iceberg/
COPY --chown=trino:trino packages/gravitino-trino-connector/postgresql-42.7.0.jar /usr/lib/trino/plugin/iceberg/

RUN mkdir /tmp/gravitino
COPY --chown=trino:trino packages/gravitino-trino-connector /tmp/gravitino

ARG IMAGE_NAME
RUN if [ "$IMAGE_NAME" = "datastrato/trino" ] ; then \
    mv /tmp/gravitino /usr/lib/trino/plugin/; \
    else echo "Copying files for other images"; \
    fi


# The following ENV are needed to update configuration file before starting Trino server, you need to
# pass these ENV variables to the container when starting it like `docker run --env GRAVITINO_HOST_IP=xxx --env GRAVITINO_HOST_PORT=xxxx`
#ENV GRAVITINO_HOST_IP
#ENV GRAVITINO_HOST_PORT
#ENV GRAVITINO_METALAKE_NAME
#ENV HADOOP_USER_NAME

ENTRYPOINT ["/bin/bash", "-c", "/etc/trino/update-trino-conf.sh && /usr/lib/trino/bin/run-trino"]
