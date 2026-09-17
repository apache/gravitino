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
# Apache Gravitino Flink Connector — Red Hat UBI 10 Certified Image
# Open source, Apache-2.0 only. Contains the Apache Gravitino Flink connector
# runtime jars for every supported Flink version present in the source tree
# (Scala 2.12; Flink does not support Scala 2.13).
# Used as a Kubernetes init-container to inject connector jars into Flink pods.
#
# The set of Flink versions is discovered at build time from the Gravitino
# source tree (see flink-connectors-dependency.sh), so this image tracks
# whatever versions the checked-out branch supports without edits here.
#
# The Flink version is selected at runtime via the FLINK_VERSION env var.
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
LABEL name="gravitino-flink-connector" \
      vendor="The Apache Software Foundation" \
      version="${IMAGE_VERSION}" \
      release="${IMAGE_RELEASE}" \
      summary="Apache Gravitino Flink Connector (Scala 2.12)" \
      description="Apache Gravitino Flink connector runtime jars for all supported Flink versions (Scala 2.12), for Kubernetes init-container deployment. The version is selected at runtime via the FLINK_VERSION environment variable." \
      maintainer="Apache Gravitino <dev@gravitino.apache.org>"

# Default Flink version (can be overridden at runtime)
ENV FLINK_VERSION=1.20

# Copy pre-built connector jars (prepared by flink-connectors-dependency.sh)
COPY packages/connectors /connectors

# Entrypoint script for version selection
COPY --chmod=755 copy-connector.sh /copy-connector.sh

# --- Red Hat certification: licenses directory (Apache-2.0 only) ---
COPY licenses /licenses

# --- OpenShift arbitrary UID compatibility ---
RUN chgrp -R 0 /connectors && chmod -R g=u /connectors

USER 1000

ENTRYPOINT ["/copy-connector.sh"]
