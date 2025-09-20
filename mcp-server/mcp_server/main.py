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

import argparse
import logging

from mcp_server.core.setting import DefaultSetting, Setting
from mcp_server.server import GravitinoMCPServer


def do_main():
    args = _parse_args()
    setting = Setting(
        metalake=args.metalake,
        gravitino_uri=args.gravitino_uri,
        tags=args.include_tool_tags,
        transport=args.transport,
        mcp_url=args.mcp_url,
    )
    _init_logging(setting)
    logging.info("Gravitino MCP server setting: %s", setting)
    server = GravitinoMCPServer(setting)
    server.run()


def _init_logging(setting: Setting):
    logging.basicConfig(
        filename="gravitino-mcp.log",
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def _comma_separated_set(value) -> set:
    if not value:
        return set()
    return set(item.strip() for item in value.split(",") if item.strip())


def _parse_args():
    parser = argparse.ArgumentParser(
        description="Gravitino Mcp server",
        epilog="Example: uv run mcp_server --metalake test --gravitino-uri http://127.0.0.1:8090",
    )

    parser.add_argument(
        "--metalake",
        type=str,
        required=True,
        help="Gravitino metalake name.",
    )
    parser.add_argument(
        "--gravitino-uri",
        type=str,
        default=DefaultSetting.default_gravitino_uri,
        help=f"The uri of Gravitino server. (default: {DefaultSetting.default_gravitino_uri})",
    )

    parser.add_argument(
        "--include-tool-tags",
        type=_comma_separated_set,
        default=set(),
        help="The tool tags to include, separated by commas, support tags:[catalog, "
        "schema, table, topic, model, fileset, tag, policy]. default: empty, "
        "all tools will be included).",
    )

    parser.add_argument(
        "--transport",
        type=str,
        choices=["stdio", "http"],
        default=DefaultSetting.default_transport,
        help=f"Transport protocol type: stdio (local), http (Streamable HTTP). "
        f"(default: {DefaultSetting.default_transport})",
    )

    parser.add_argument(
        "--mcp-url",
        type=str,
        default=DefaultSetting.default_mcp_url,
        help=f"The url of MCP server if using http transport. (default: {DefaultSetting.default_mcp_url})",
    )

    args = parser.parse_args()
    return args


if __name__ == "__main__":
    do_main()
