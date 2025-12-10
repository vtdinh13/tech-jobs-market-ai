
import argparse
import logging
from typing import Mapping, List
import os
from datetime import datetime, timedelta, timezone
# import time

import requests
import psycopg

from .utils import create_table_sql, insert_sql

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)

class AdzunaClient:

    base_url = "https://api.adzuna.com/v1/api/jobs"

    def __init__(self, app_id:str, app_key:str, country:str = "be", results_per_page:int=50):
        
        self.app_id = app_id 
        self.app_key = app_key 
        self.country = country
        self.results_per_page = results_per_page
        self.db_conn = {
            "host": os.getenv("DB_HOST", "localhost"),
            "port": int(os.getenv("DB_PORT", "5434")),
            "dbname": os.getenv("DB_NAME", "jobs"),
            "user": os.getenv("DB_USER", "jobs"),
            "password": os.getenv("DB_PASSWORD", "jobs"),
        }

    

    def fetch_data(self, job_titles:str, page:int):
        """Call the Adzuna API for a given job title and page."""

        params = {
            "app_id": self.app_id,
            "app_key": self.app_key,
            "results_per_page": self.results_per_page,
            "what": job_titles,
            "sort_by": "date"

        }

    
        url = f"{self.base_url}/{self.country}/search/{page}"
        logger.info("Requesting %s", url)
        try:
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
            return data
        except requests.RequestException as e:
            logger.error("Request failed for %s page %s: %s", job_titles, page, e)
            # time.sleep(10)

    @staticmethod
    def extract_row(job: Mapping) -> tuple:
            """Map a job payload into the tuple expected by the database schema."""
            row = (
                job.get("id"),
                job.get("company", {}).get("display_name"),
                job.get("location", {}).get("display_name"),
                job.get("title"),
                job.get("latitude"),
                job.get("longitude"),
                job.get("redirect_url"),
                job.get("description"),
                job.get("category", {}).get("tag"),
                job.get("contract_time"),
                job.get("created"),
            )
            return row

    def save_to_db(self, data: Mapping[str, object]):
        """Insert fetched jobs into Postgres, creating the table if necessary."""
        
        jobs = data.get("results", []) or []
        if not jobs:
            logger.info("No jobs to insert for payload metadata=%s", data.get("count"))
            return 0
        
        rows = []
        for job in jobs:
            row = self.extract_row(job)
            if row:
                rows.append(row)

        if not rows:
            logger.info("All rows skipped for this page")
            return 0
        with psycopg.connect(**self.db_conn) as conn, conn.cursor() as cur:
            cur.execute(create_table_sql)
            cur.executemany(insert_sql, rows)
            conn.commit()

        logger.info("Inserted %s jobs", len(jobs))
        return len(rows)
    
    def fetch_and_save(self, job_titles:str, pages:int):
        """Fetch all requested pages for one title, filtering old jobs before saving."""
        cutoff = datetime.now(timezone.utc) - timedelta(days=365) 
        total = 0

        for page in range(100, pages+1):
            data = self.fetch_data(job_titles, page)
            if not data:
                continue

            raw_jobs = data.get("results", []) or []
            fresh_jobs = []
            for job in raw_jobs:
                created_str = job.get("created")
                if not created_str:
                    continue
                created_dt = datetime.fromisoformat(created_str.replace("Z", "+00:00"))
                if created_dt >= cutoff:
                    fresh_jobs.append(job)
                else:
                    break  # sorted newest→oldest, so everything after is older

            if not fresh_jobs:
                logger.info("No jobs newer than %s on page %s; stopping", cutoff, page)
                break
            
            total += self.save_to_db({"results": fresh_jobs})

        return total

def main(job_titles:List[str], pages:int):

    app_id =  os.getenv("ADZUNA_APP_ID")
    app_key = os.getenv("ADZUNA_API_KEY")
    client = AdzunaClient(app_id, app_key)
    total = 0
    for title in job_titles:
        logger.info(f"Fetching jobs for job title '%s'", title)
        total += client.fetch_and_save(title, pages)
    logger.info("Completed importing %s jobs across %s queries", total, len(job_titles))


def parse_args():
    parser = argparse.ArgumentParser(description="Ingest Adzuna jobs for one or more titles.")
    parser.add_argument("job_titles", nargs="+", help="Job titles to query.")
    parser.add_argument("--pages", type=int, default=1, help="Number of pages to fetch per title.")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    main(job_titles=args.job_titles, pages=args.pages)
