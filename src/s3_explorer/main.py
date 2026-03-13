#!/usr/bin/env python3

"""Main script."""

import logging
import pathlib

import hydra
import structlog

from s3_explorer.app import create_app


def configure_logger(logging_level=logging.NOTSET):
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.StackInfoRenderer(),
            structlog.dev.set_exc_info,
            structlog.processors.TimeStamper(fmt="%Y-%m-%d %H:%M:%S", utc=False),
            structlog.dev.ConsoleRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(logging_level),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=False,
    )
    return structlog.get_logger()


@hydra.main(
    version_base=None,
    config_path=str(pathlib.Path.cwd() / "conf"),
    config_name="config",
)
def start(cfg) -> None:
    debug = "debug" in cfg and cfg.debug
    logger = configure_logger(logging.DEBUG if debug else logging.INFO)
    logger.debug("Configuration", config=cfg)
    app = create_app(cfg, logger)
    app.run(debug=debug)


if __name__ == "__main__":
    start()
