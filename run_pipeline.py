import subprocess
import sys
from datetime import datetime
from config import logger

STEPS = [
    "fetch_stocks.py",
    "load_bronze.py",
    "load_silver.py",
    "load_dimensions.py",
    "load_gold.py",
]


def run(script):
    logger.info(f"Starting {script}...")
    result = subprocess.run([sys.executable, script], capture_output=False)
    if result.returncode != 0:
        logger.error(f"{script} failed — pipeline aborted.")
        sys.exit(1)
    logger.info(f"{script} completed.")


start = datetime.now()
logger.info(f"Pipeline started.")

for step in STEPS:
    run(step)

duration = (datetime.now() - start).seconds
logger.info(f"Pipeline completed in {duration} seconds.")