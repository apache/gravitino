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

from mcp_server.core.setting import Setting
from mcp_server.server import GravitinoMCPServer


def do_main():
    args = _parse_args()
    setting = Setting(args.metalake, args.uri, args.include_tool_tags)
    init_logging(setting)
    logging.info(f"Gravitino MCP server setting: {setting}")
    server = GravitinoMCPServer(setting)
    server.run()


def init_logging(setting: Setting):
    logging.basicConfig(
        filename="gravitino-mcp.log",
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def _comma_separated_set(value) -> set:
    print(f"value={value}")
    if not value:
        return set()
    return set(item.strip() for item in value.split(",") if item.strip())


def _parse_args():
    parser = argparse.ArgumentParser(
        description="Gravitino Mcp server",
        epilog="Example: uv run mcp_server --metalake test --uri http://127.0.0.1:8090",
    )

    parser.add_argument(
        "--metalake",
        type=str,
        required=True,
        help="Gravitino metalake name",
    )
    parser.add_argument(
        "--uri",
        type=str,
        default="http://127.0.0.1:8090",
        help="The uri of Gravitino server",
    )

    parser.add_argument(
        "--include-tool-tags",
        type=_comma_separated_set,
        default=set(),
        help="The tool tags to include, separated by commas, support tags:[metalake, schema, table]. If not specified, all tools will be included.",
    )

    args = parser.parse_args()
    return args


if __name__ == "__main__":
    do_main()
