import argparse


def create_parser():
    """创建参数解析器"""
    parser = argparse.ArgumentParser(
        prog="imgtool",
        description="图像处理工具",
        epilog="示例: imgtool convert -s 80 input.jpg output.png",
    )

    # 必需的子命令
    subparsers = parser.add_subparsers(
        dest="command", title="可用命令", metavar="COMMAND", required=True
    )

    # convert 子命令
    conv_parser = subparsers.add_parser("convert", help="格式转换")
    conv_parser.add_argument("input", help="输入文件路径")
    conv_parser.add_argument("output", help="输出文件路径")
    conv_parser.add_argument(
        "-s",
        "--size",
        type=int,
        choices=range(1, 101),
        metavar="1-100",
        help="缩放百分比 (1-100)",
    )
    conv_parser.add_argument(
        "-q", "--quality", type=int, default=85, help="输出质量 (0-100，默认: 85)"
    )

    # resize 子命令
    resize_parser = subparsers.add_parser("resize", help="调整尺寸")
    resize_parser.add_argument("files", nargs="+", help="输入文件列表")
    resize_group = resize_parser.add_mutually_exclusive_group(required=True)
    resize_group.add_argument("-w", "--width", type=int, help="目标宽度（保持比例）")
    resize_group.add_argument("-h", "--height", type=int, help="目标高度（保持比例）")
    resize_parser.add_argument("--output-dir", required=True, help="输出目录")

    # 全局参数
    for p in [conv_parser, resize_parser]:
        p.add_argument(
            "-v",
            "--verbose",
            action="count",
            default=0,
            help="详细级别 (-v, -vv, -vvv)",
        )
        p.add_argument(
            "--dry-run", action="store_true", help="模拟运行，不实际修改文件"
        )

    return parser


if __name__ == "__main__":
    parser = create_parser()
    args = parser.parse_args()

    print(f"命令: {args.command}")
    print(f"参数: {vars(args)}")

    if args.verbose >= 1:
        print("详细模式激活")
        if args.verbose >= 2:
            print("调试信息: ...")

    # 实际应用中，这里根据参数执行相应操作...
    if args.dry_run:
        print("(模拟运行 - 无实际修改)")
