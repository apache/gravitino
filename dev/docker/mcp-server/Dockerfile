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
FROM python:3.10

LABEL maintainer="dev@gravitino.apache.org"

WORKDIR /opt/mcp-server

COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

COPY --chmod=775 packages/mcp-server /opt/mcp-server

COPY --chmod=775 start-mcp-server.sh /opt/mcp-server

# uv defaults its cache to $HOME/.cache/uv, which a non-root user cannot create.
ENV UV_CACHE_DIR=/opt/mcp-server/.cache/uv

RUN uv venv

RUN uv sync

RUN uv pip install -e .

RUN chmod -R 775 /opt/mcp-server \
    && useradd -u 1000 -g 0 -M -s /sbin/nologin gravitino

USER 1000

ENTRYPOINT ["/bin/bash", "start-mcp-server.sh"]
