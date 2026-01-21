import os
import subprocess
from dagster import op, job, schedule, DefaultScheduleStatus, RunRequest

# Constants
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SCRAPER_SCRIPT = os.path.join(BASE_DIR, "src", "scraper.py")
LOADER_SCRIPT = os.path.join(BASE_DIR, "scripts", "load_to_postgres.py")
YOLO_SCRIPT = os.path.join(BASE_DIR, "src", "yolo_detect.py")
YOLO_LOADER_SCRIPT = os.path.join(BASE_DIR, "scripts", "load_yolo_to_postgres.py")
DBT_PROJECT_DIR = os.path.join(BASE_DIR, "medical_warehouse")

@op
def scrape_telegram_data():
    """Runs the Telegram scraper script."""
    result = subprocess.run(["python", SCRAPER_SCRIPT], capture_output=True, text=True)
    if result.returncode != 0:
        raise Exception(f"Scraper failed with error: {result.stderr}")
    return result.stdout

@op
def load_raw_to_postgres(scraper_output):
    """Loads scraped JSON data into PostgreSQL."""
    result = subprocess.run(["python", LOADER_SCRIPT], capture_output=True, text=True)
    if result.returncode != 0:
        raise Exception(f"Loader failed with error: {result.stderr}")
    return result.stdout

@op
def run_yolo_enrichment(scraper_output):
    """Runs YOLO object detection and loads results into PostgreSQL."""
    # Run detection
    detect_result = subprocess.run(["python", YOLO_SCRIPT], capture_output=True, text=True)
    if detect_result.returncode != 0:
        raise Exception(f"YOLO detection failed: {detect_result.stderr}")
    
    # Load detection results
    load_result = subprocess.run(["python", YOLO_LOADER_SCRIPT], capture_output=True, text=True)
    if load_result.returncode != 0:
        raise Exception(f"YOLO loader failed: {load_result.stderr}")
    
    return f"{detect_result.stdout}\n{load_result.stdout}"

@op
def run_dbt_transformations(load_raw_output, yolo_output):
    """Executes dbt models to transform raw data into marts."""
    result = subprocess.run(["dbt", "run", "--project-dir", DBT_PROJECT_DIR], capture_output=True, text=True)
    if result.returncode != 0:
        raise Exception(f"dbt run failed: {result.stderr}")
    return result.stdout

@job
def medical_warehouse_pipeline():
    """Defines the job graph for the medical warehouse data pipeline."""
    scraped_data = scrape_telegram_data()
    raw_loaded = load_raw_to_postgres(scraped_data)
    yolo_enriched = run_yolo_enrichment(scraped_data)
    run_dbt_transformations(raw_loaded, yolo_enriched)

@schedule(job=medical_warehouse_pipeline, cron_schedule="0 0 * * *", default_status=DefaultScheduleStatus.STOPPED)
def daily_medical_warehouse_schedule():
    """Schedule to run the pipeline daily at midnight."""
    return RunRequest()
