import argparse
import os
from enum import StrEnum, auto
from pathlib import Path

from cactus_client.error import ConfigException
from cactus_client.model.config import (
    CONFIG_CWD,
    CONFIG_HOME,
    GlobalConfig,
    load_config,
)

COMMAND_NAME = "setup"


class SetupCommand(StrEnum):
    INIT = auto()


def add_sub_commands(subparsers: argparse._SubParsersAction) -> None:
    """Adds the sub command options for the setup module"""

    server_parser = subparsers.add_parser(
        COMMAND_NAME, help="For initial setup of working directories / config locations"
    )
    server_parser.add_argument(
        "-g",
        "--global-cfg",
        help="Configuration will be stored in your home directory (commands will work anywhere)",
        required=False,
        action="store_true",
        default=False,
    )
    server_parser.add_argument(
        "-l",
        "--local-cfg",
        help="Configuration will be stored in the current working directory (always run commands from here)",
        required=False,
        action="store_true",
        default=False,
    )
    server_parser.add_argument(
        "-r",
        "--reset",
        help="Delete any existing config in the current/home directory (will not touch existing output files)",
        required=False,
        action="store_true",
        default=False,
    )

    server_parser.add_argument(
        "working_dir", help="The directory to initialise as a working directory (will be created if it DNE)"
    )


def run_action(args: argparse.Namespace) -> None:  # noqa: C901

    reset: bool = args.reset
    local_cfg: bool = args.local_cfg
    global_cfg: bool = args.global_cfg
    working_dir = Path(args.working_dir)

    # Do setup of the cactus config file
    if global_cfg is local_cfg:
        print("Please select EXACTLY one of -g/--global-config and -l/--local-config")
        return

    if reset:
        if CONFIG_CWD.exists():
            print(f"Removing {CONFIG_CWD}")
            os.remove(CONFIG_CWD)

        if CONFIG_HOME.exists():
            print(f"Removing {CONFIG_HOME}")
            os.remove(CONFIG_HOME)

    cfg_file = CONFIG_CWD if local_cfg else CONFIG_HOME

    # Do working dir setup
    if working_dir.exists():
        if not working_dir.is_dir():
            print(f"'{working_dir}' exists but is not a directory. Aborting.")
            return
        else:
            print(f"Working directory {working_dir} already exists. No action taken.")
    else:
        print(f"Creating working directory '{working_dir}'")
        working_dir.mkdir(parents=True)

    # Now try and update the config to use this working dir
    try:
        config, cfg_file = load_config(str(cfg_file.absolute()))
    except ConfigException:
        print(f"Config file {cfg_file} is not readable / doesn't exist. It will be overridden.")
        config = GlobalConfig(output_dir=str(working_dir.absolute()))

    print(f"Writing updated working directory '{config.output_dir}' to '{cfg_file.absolute()}'")
    config.to_yaml_file(cfg_file)
