from dotenv import load_dotenv
from psycopg2 import pool
import os
from datetime import datetime
from sparql import ingest_imdbIds_by_year
from logger import send_log_async, service_wake_up

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")

service_wake_up()

db_pool = pool.SimpleConnectionPool(
    minconn=1,
    maxconn=10,
    dsn=DATABASE_URL
)

start_year = 2000
current_year = datetime.now().year


def ingest(start_year, current_year):
    year = start_year

    while year <= current_year:
        try:
            ingest_imdbIds_by_year(year, db_pool)
            send_log_async(
                "info",
                "main.py -> ingest()",
                year,
                f"ingested all imdb ids for year {year}"
            )

        except Exception as e:
            send_log_async(
                "error",
                "main.py -> ingest()",
                year,
                str(e)
            )

        year += 1


ingest(start_year, current_year)

db_pool.closeall()