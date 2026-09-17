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

# ==============================================================================
# Apache Gravitino Spark Connector — Red Hat UBI 10 Certified Image
# Open source, Apache-2.0 only. Contains the Apache Gravitino Spark connector
# runtime jars for every supported Spark version and Scala variant present in
# the source tree.
# Used as a Kubernetes init-container to inject connector jars into Spark pods.
#
# The set of Spark version / Scala combinations is discovered at build time
# from the Gravitino source tree (see spark-connectors-dependency.sh), so this
# image tracks whatever the checked-out branch supports without edits here.
#
# The Spark and Scala versions are selected at runtime via SPARK_VERSION and
# SCALA_VERSION env vars.
#
# Red Hat Certification Compliance:
#   - Based on UBI 10 (registry.access.redhat.com)
#   - Required LABELs present
#   - /licenses directory with the Apache License and NOTICE
#   - Runs as non-root user (UID 1000, GID 0)
#   - OpenShift arbitrary UID compatible (GID 0 group permissions)
# ==============================================================================

ARG UBI_MINIMAL_TAG=10.2
ARG IMAGE_VERSION=1.3.0-SNAPSHOT
ARG IMAGE_RELEASE=1

FROM registry.access.redhat.com/ubi10/ubi-minimal:${UBI_MINIMAL_TAG}
ARG IMAGE_VERSION
ARG IMAGE_RELEASE

# --- Red Hat certification required LABELs ---
LABEL name="gravitino-spark-connector" \
      vendor="The Apache Software Foundation" \
      version="${IMAGE_VERSION}" \
      release="${IMAGE_RELEASE}" \
      summary="Apache Gravitino Spark Connector" \
      description="Apache Gravitino Spark connector runtime jars for all supported Spark versions and Scala variants, for Kubernetes init-container deployment. The version is selected at runtime via the SPARK_VERSION and SCALA_VERSION environment variables." \
      maintainer="Apache Gravitino <dev@gravitino.apache.org>"

# Default Spark and Scala version (can be overridden at runtime)
ENV SPARK_VERSION=3.5
ENV SCALA_VERSION=2.12

# Copy pre-built connector jars (prepared by spark-connectors-dependency.sh)
COPY packages/connectors /connectors

# Entrypoint script for version selection
COPY --chmod=755 copy-connector.sh /copy-connector.sh

# --- Red Hat certification: licenses directory (Apache-2.0 only) ---
COPY licenses /licenses

# --- OpenShift arbitrary UID compatibility ---
RUN chgrp -R 0 /connectors && chmod -R g=u /connectors

USER 1000

ENTRYPOINT ["/copy-connector.sh"]
