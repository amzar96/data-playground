import logging
import argparse
from common.configs import Config
from common.logging import get_logger
from services.postgres import PostgresHandler


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    log_level = logging.DEBUG if args.debug else logging.INFO
    logger = get_logger(log_level=log_level)

    configs = Config()
    postgres_handler = PostgresHandler(configs)
