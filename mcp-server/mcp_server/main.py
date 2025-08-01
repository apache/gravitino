import argparse
import logging

from mcp_server.core.setting import Setting
from mcp_server.server import GravitinoMCPServer


def do_main():
    args = parse_args()
    setting = Setting(args.metalake, args.uri, args.mode)
    init_logging(setting)
    server = GravitinoMCPServer(setting)
    server.run()


def init_logging(setting: Setting):
    logging.basicConfig(
        filename="gravitino-mcp.log",
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def parse_args():
    parser = argparse.ArgumentParser(
        description="Gravitino Mcp server",
        epilog="Example: uv run mcp_server add 5 3 --verbose",
    )

    parser.add_argument(
        "--mode", choices=["stdio", "http"], default="stdio", help="start up mode"
    )

    parser.add_argument(
        "--metalake",
        type=str,
        default="test",
        required=False,
        help="Gravitino metalake name",
    )
    parser.add_argument(
        "--uri",
        type=str,
        default="http://127.0.0.1:8090",
        help="The uri of Gravitino server",
    )

    args = parser.parse_args()
    return args


if __name__ == "__main__":
    do_main()
