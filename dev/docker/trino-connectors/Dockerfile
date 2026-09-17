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
# Apache Gravitino Trino Connector — UBI 10-based, certification-oriented image
# Contains the Apache Gravitino Trino connector plugin for every supported
# Trino/Starburst version range present in the source tree. Each band is a full
# Trino plugin directory (connector jar + its dependency jars + LICENSE +
# NOTICE + README). The connector code is Apache-2.0; the bundled dependency
# jars are third-party open source (see licenses/THIRD_PARTY_LICENSES.txt).
# Used as a Kubernetes init-container to inject connector jars into Trino pods.
#
# The set of version ranges is discovered at build time from the Gravitino
# source tree (see trino-connectors-dependency.sh), so this image tracks
# whatever versions the checked-out branch supports without edits here.
#
# The Trino version range is selected at runtime via the TRINO_VERSION env var.
#
# Red Hat UBI / certification-oriented properties:
#   - Based on UBI 10 (registry.access.redhat.com)
#   - Required LABELs present
#   - /licenses directory with LICENSE, NOTICE and THIRD_PARTY_LICENSES.txt
#   - Runs as non-root user (UID 1000, GID 0) with a passwd entry
#   - OpenShift arbitrary UID compatible (GID 0 group permissions)
# ==============================================================================

ARG UBI_MINIMAL_TAG=10.2
# IMAGE_VERSION is injected at build time from gradle.properties by
# build-docker.sh / the release workflow. The default below is kept in sync
# with the current gradle.properties version as a fallback for uninjected
# local builds.
ARG IMAGE_VERSION=2.0.0-SNAPSHOT
ARG IMAGE_RELEASE=1

FROM registry.access.redhat.com/ubi10/ubi-minimal:${UBI_MINIMAL_TAG}
ARG IMAGE_VERSION
ARG IMAGE_RELEASE

# --- Red Hat certification required LABELs ---
LABEL name="gravitino-trino-connector" \
      vendor="The Apache Software Foundation" \
      version="${IMAGE_VERSION}" \
      release="${IMAGE_RELEASE}" \
      summary="Apache Gravitino Trino/Starburst Connector" \
      description="Apache Gravitino Trino connector for all supported Trino/Starburst version ranges, for Kubernetes init-container deployment. Supports open-source Trino and Starburst. The version range is selected at runtime via the TRINO_VERSION environment variable." \
      maintainer="Apache Gravitino <dev@gravitino.apache.org>"

# Default Trino version (can be overridden at runtime, e.g. 478, 452, 440)
ENV TRINO_VERSION=478

# Copy pre-built connector jars (prepared by trino-connectors-dependency.sh)
COPY packages/connectors /connectors

# Entrypoint script for version selection
COPY --chmod=755 copy-connector.sh /copy-connector.sh

# --- licenses directory (LICENSE + NOTICE + third-party summary) ---
COPY licenses /licenses

# --- Drop source-control artifacts, OpenShift arbitrary UID compatibility,
#     and a passwd entry for UID 1000 ---
RUN rm -f /licenses/.gitignore \
    && chgrp -R 0 /connectors /licenses \
    && chmod -R g=u /connectors /licenses \
    && echo "gravitino:x:1000:0:Gravitino user:/:/sbin/nologin" >> /etc/passwd

USER 1000

ENTRYPOINT ["/copy-connector.sh"]
