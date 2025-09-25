import argparse
from enum import StrEnum, auto

import cactus_client.cli.client as client
import cactus_client.cli.run as run
import cactus_client.cli.server as server
import cactus_client.cli.setup as setup

# cactus config server dcap https://foo.bar/dcap

# cactus config client client1 certificate -f cert.pem
# cactus config client client1 key -f cert.key
# cactus config client client1 type aggregator|device
# cactus config client client1 lfdi ABC123
# cactus config client client1 sfdi 456789
# cactus config client client1 pen 456789
# cactus config client client1 pin 123456
# cactus config client client1 maxW 6000
# cactus config client client1 notificationuri https://localhost:1234/foo/bar

# cactus test run --report "output.pdf" --version v1.2  S-ALL-01 client1
# cactus test list


root_parser = argparse.ArgumentParser(prog="cactus", description="CSIP-Aus server test harness implementation.")
root_subparsers = root_parser.add_subparsers(dest="command")

setup.add_sub_commands(root_subparsers)
client.add_sub_commands(root_subparsers)
server.add_sub_commands(root_subparsers)
run.add_sub_commands(root_subparsers)


def cli_entrypoint() -> None:
    args = root_parser.parse_args()

    match (args.command):
        case client.COMMAND_NAME:
            client.run_action(args)
        case server.COMMAND_NAME:
            server.run_action(args)
        case run.COMMAND_NAME:
            run.run_action(args)
        case setup.COMMAND_NAME:
            setup.run_action(args)
        case _:
            root_parser.print_help()
