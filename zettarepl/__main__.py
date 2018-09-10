# -*- coding=utf-8 -*-
import argparse
import logging

import coloredlogs

from .commands.run import run

logger = logging.getLogger(__name__)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument("-l", "--logging-level", type=logging.getLevelName, default=logging.DEBUG,
                        choices=[i[1] for i in sorted(logging._levelToName.items(), key=lambda i: i[0])
                                 if i[1] != "NOTSET"])
    parser.add_argument("--logging-color", action="store_true")

    subparsers = parser.add_subparsers(title="subcommands")

    run_parser = subparsers.add_parser("run")
    run_parser.add_argument("definition_path", type=argparse.FileType("r"))
    run_parser.add_argument("--once", action="store_true")
    run_parser.set_defaults(func=run)

    args = parser.parse_args()

    logging_format = "[%(asctime)s] %(levelname)-8s [%(threadName)s] [%(name)s] %(message)s"
    logging.basicConfig(level=args.logging_level, format=logging_format)
    if args.logging_color:
        coloredlogs.install(level=args.logging_level, fmt=logging_format)
    args.func(args)
