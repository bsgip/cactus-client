import argparse

import cactus_client.cli.client as client
import cactus_client.cli.run as run
import cactus_client.cli.server as server
import cactus_client.cli.setup as setup
import cactus_client.cli.tests as tests

root_parser = argparse.ArgumentParser(prog="cactus", description="CSIP-Aus server test harness implementation.")
root_subparsers = root_parser.add_subparsers(dest="command")

setup.add_sub_commands(root_subparsers)
client.add_sub_commands(root_subparsers)
server.add_sub_commands(root_subparsers)
run.add_sub_commands(root_subparsers)
tests.add_sub_commands(root_subparsers)


def cli_entrypoint() -> None:
    """Handle command line arguments - call out to the appropriate CLI sub command"""
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
        case tests.COMMAND_NAME:
            tests.run_action(args)
        case _:
            root_parser.print_help()


if __name__ == "__main__":
    cli_entrypoint()  # This really only exists for debugging
