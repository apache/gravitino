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
import os

from mcp_server.core.setting import DefaultSetting, Setting
from mcp_server.server import GravitinoMCPServer
from mcp_server.tools import SUPPORTED_TOOL_TAGS


def do_main():
    args = _parse_args()
    setting = Setting(
        metalake=args.metalake,
        gravitino_uri=args.gravitino_uri,
        tags=args.include_tool_tags,
        transport=args.transport,
        mcp_url=args.mcp_url,
        token=args.token,
        tls_cert=args.tls_cert,
        tls_key=args.tls_key,
        oauth_token_endpoint=args.oauth_token_endpoint,
        oauth_client_id=args.oauth_client_id,
        oauth_client_secret=args.oauth_client_secret,
        oauth_scope=args.oauth_scope,
    )
    _init_logging(setting)
    try:
        setting.validate_oauth()
    except ValueError as exc:
        logging.error("%s", exc)
        raise SystemExit(1) from None
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
    # Separate file handler for structured audit records (one JSON line per entry).
    audit_handler = logging.FileHandler("gravitino-mcp-audit.log")
    audit_handler.setLevel(logging.INFO)
    audit_handler.setFormatter(logging.Formatter("%(message)s"))
    logging.getLogger("gravitino.mcp.audit").addHandler(audit_handler)
    logging.getLogger("gravitino.mcp.audit").propagate = False


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
        help="The tool tags to include, separated by commas, support tags:"
        f"[{', '.join(sorted(SUPPORTED_TOOL_TAGS))}]. default: empty, "
        "all tools will be included.",
    )

    parser.add_argument(
        "--transport",
        type=str,
        choices=["stdio", "http", "streamable-http"],
        default=DefaultSetting.default_transport,
        help="Transport protocol type: stdio (local), http / streamable-http "
        "(networked Streamable HTTP; the two names are equivalent). "
        f"(default: {DefaultSetting.default_transport})",
    )

    parser.add_argument(
        "--mcp-url",
        type=str,
        default=DefaultSetting.default_mcp_url,
        help="The url of MCP server if using http transport, http:// or https://. "
        f"(default: {DefaultSetting.default_mcp_url})",
    )

    parser.add_argument(
        "--token",
        type=str,
        default=os.environ.get("GRAVITINO_TOKEN", ""),
        help="Static credential used as the Authorization header when "
        "authenticating to Gravitino. A bare token is treated as an OAuth2 Bearer "
        "token; a value containing a valid scheme and credential, such as "
        "'Basic <base64>', is sent as an Authorization credential. In stdio mode "
        "it is sent on every request; in HTTP mode it is only the fallback when an "
        "incoming request carries no Authorization header "
        "(per-request identity takes priority). "
        "Can also be set via the GRAVITINO_TOKEN environment variable. "
        "When omitted, requests are sent without authentication. "
        "Takes precedence over OAuth client-credentials flags.",
    )

    parser.add_argument(
        "--oauth-token-endpoint",
        type=str,
        default=os.environ.get("GRAVITINO_OAUTH_TOKEN_ENDPOINT", ""),
        help="OAuth2 token endpoint for client-credentials (service identity). "
        "Requires --oauth-client-id and --oauth-client-secret. "
        "Can also be set via GRAVITINO_OAUTH_TOKEN_ENDPOINT.",
    )
    parser.add_argument(
        "--oauth-client-id",
        type=str,
        default=os.environ.get("GRAVITINO_OAUTH_CLIENT_ID", ""),
        help="OAuth2 client id for client-credentials. "
        "Can also be set via GRAVITINO_OAUTH_CLIENT_ID.",
    )
    parser.add_argument(
        "--oauth-client-secret",
        type=str,
        default=os.environ.get("GRAVITINO_OAUTH_CLIENT_SECRET", ""),
        help="OAuth2 client secret for client-credentials. "
        "Can also be set via GRAVITINO_OAUTH_CLIENT_SECRET.",
    )
    parser.add_argument(
        "--oauth-scope",
        type=str,
        default=os.environ.get("GRAVITINO_OAUTH_SCOPE", ""),
        help="Optional OAuth2 scope for client-credentials. "
        "Can also be set via GRAVITINO_OAUTH_SCOPE.",
    )

    parser.add_argument(
        "--tls-cert",
        type=str,
        default="",
        help="Path to the TLS certificate (PEM) for serving the HTTP endpoint "
        "over HTTPS. Requires --tls-key. When omitted, the endpoint serves plain HTTP.",
    )

    parser.add_argument(
        "--tls-key",
        type=str,
        default="",
        help="Path to the TLS private key (PEM) for serving the HTTP endpoint "
        "over HTTPS. Requires --tls-cert.",
    )

    args = parser.parse_args()
    return args


if __name__ == "__main__":
    do_main()
