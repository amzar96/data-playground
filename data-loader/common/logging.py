import sys
import os
import logging

FORMATTER = logging.Formatter(
    "%(asctime)s - %(name)s - %(levelname)s - %(filename)s - %(lineno)d - %(funcName)s - %(message)s"
)


def get_console_handler(log_level=logging.INFO):
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(FORMATTER)
    console_handler.setLevel(log_level)
    return console_handler


def get_logger(logger_name=__name__, log_level=logging.DEBUG):
    logger = logging.getLogger(logger_name)
    logger.setLevel(log_level)
    logger.addHandler(get_console_handler(log_level))
    logger.propagate = False
    return logger
