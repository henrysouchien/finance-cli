"""Run the finance gateway server."""
import logging
import os

import uvicorn

from finance_cli.config import load_dotenv
from finance_cli.logging_config import setup_logging

from .config import load_settings
from .server import create_app

_VALID_LOG_LEVELS = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}


def main() -> None:
    load_dotenv()
    setup_logging()
    raw_level = os.getenv("FINANCE_CLI_LOG_LEVEL", "INFO").strip().upper()
    level = raw_level if raw_level in _VALID_LOG_LEVELS else "INFO"
    logging.getLogger("finance_cli").setLevel(getattr(logging, level))
    settings = load_settings()
    app = create_app(settings)
    uvicorn.run(app, host=settings.host, port=settings.port)


if __name__ == "__main__":
    main()
