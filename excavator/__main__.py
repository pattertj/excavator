import logging
import logging.config

from excavator.excavator import Excavator

if __name__ == "__main__":
    # Create Logging
    logging.config.fileConfig(
        "logConfig.ini",
        defaults={"logfilename": "excavator.log"},
        disable_existing_loggers=False,
    )

    # Setup our scraper
    excavator = Excavator()

    # Start scraping
    excavator.dig()
