import subprocess
import sys
from datetime import datetime

def run(script):
    print(f"\n{'='*40}")
    print(f"Running {script}...")
    print(f"{'='*40}")
    result = subprocess.run([sys.executable, script], capture_output=False)
    if result.returncode != 0:
        print(f"ERROR: {script} failed!")
        sys.exit(1)

start = datetime.now()
print(f"Pipeline started at {start.strftime('%Y-%m-%d %H:%M:%S')}")

run("fetch_stocks.py")
run("load_bronze.py")
run("load_silver.py")
run("load_dimensions.py")
run("load_gold.py")

end = datetime.now()
duration = (end - start).seconds
print(f"\nPipeline completed in {duration} seconds!")
