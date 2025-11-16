import os
import logging
from datetime import date, timedelta
import pandas as pd
import sys
from pgdb import PGDataBase
from ConfigLoader import ConfigLoader
from LoggerManager import LoggerManager
from APIClient import APIClient


logger_manager = LoggerManager(log_dir='logs', keep_days=7)
logging.info("Log file cleanup finished.")

dirname = os.path.dirname(__file__)
config_loader = ConfigLoader(os.path.join(dirname, "config.json"))
config = config_loader.config

if config is None or \
   not config.get('database') or \
   not config.get('api_endpoint'):
    logging.error("Failed to load essential configuration sections. Exiting.")
    sys.exit(1)

db_params = config['database']
api_url = config['api_endpoint'].get('url')
if not api_url:
    logging.error("API URL is missing in config. Exiting.")
    sys.exit(1)

start_date = date(2023, 1, 1)
end_date = date(2023, 12, 31)

logging.info(f"Start full history load from {start_date} to {end_date}")

client = APIClient(timeout=60)

cols = ["client_id","gender","product_id","quantity","price_per_item","discount_per_item","total_price","purchase_datetime_full"]

insert_sql = """
INSERT INTO public.sales
(client_id, gender, product_id, quantity, price_per_item, discount_per_item, total_price, date_time)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
"""

total_inserted = 0
current = start_date

while current <= end_date:
    day_str = current.strftime("%Y-%m-%d")
    logging.info(f"Requesting data for {day_str}")

    try:
        params = {"date": day_str}
        df = client.fetch_df(api_url, params=params)

        if not isinstance(df, pd.DataFrame) or df.empty:
            logging.info(f"{day_str}: no data returned from API.")
            current += timedelta(days=1)
            continue

        df = df.copy()
        df["purchase_datetime_full"] = pd.to_datetime(df["purchase_datetime_full"], errors="coerce")
        df = df.dropna(subset=["purchase_datetime_full"])

        params_seq = [
            (
                str(r.client_id),
                str(r.gender),
                str(r.product_id),
                int(r.quantity),
                float(r.price_per_item),
                float(r.discount_per_item),
                float(r.total_price),
                r.purchase_datetime_full.to_pydatetime(),
            )
            for r in df[cols].itertuples(index=False)
        ]

        if not params_seq:
            logging.info(f"{day_str}: no valid rows after cleaning.")
            current += timedelta(days=1)
            continue

        try:
            with PGDataBase(
                host=db_params['host'],
                database=db_params['dbname'],
                user=db_params['user'],
                password=db_params['password'],
            ) as database:
                database.post_many(insert_sql, params_seq)

            inserted = len(params_seq)
            total_inserted += inserted
            logging.info(f"{day_str}: inserted {inserted} rows.")
        except Exception as e:
            logging.error(f"{day_str}: DB insert error: {e}")

    except Exception as e:
        logging.error(f"{day_str}: API error: {e}")

    current += timedelta(days=1)

logging.info(f"Full history load finished. Total inserted: {total_inserted}")
