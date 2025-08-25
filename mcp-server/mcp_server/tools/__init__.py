# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from fastmcp import FastMCP

from mcp_server.tools.catalog import load_catalog_tools
from mcp_server.tools.fileset import load_fileset_tools
from mcp_server.tools.job import load_job_tool
from mcp_server.tools.metadata import load_metadata_tool
from mcp_server.tools.model import load_model_tools
from mcp_server.tools.policy import load_policy_tools
from mcp_server.tools.schema import load_schema_tools
from mcp_server.tools.statistic import load_statistic_tools
from mcp_server.tools.table import load_table_tools
from mcp_server.tools.tag import load_tag_tool
from mcp_server.tools.topic import load_topic_tools


def load_tools(mcp: FastMCP):
    load_job_tool(mcp)
    load_catalog_tools(mcp)
    load_schema_tools(mcp)
    load_table_tools(mcp)
    load_topic_tools(mcp)
    load_model_tools(mcp)
    load_fileset_tools(mcp)
    load_tag_tool(mcp)
    load_metadata_tool(mcp)
    load_statistic_tools(mcp)
    load_policy_tools(mcp)
