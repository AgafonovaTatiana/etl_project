import os
from datetime import date, timedelta
import pandas as pd
import numpy as np
import logging
import sys
from pgdb import PGDataBase
from ConfigLoader import ConfigLoader
from LoggerManager import LoggerManager
from APIClient import APIClient

prev_day = (date.today()-timedelta(days=1)).strftime("%Y-%m-%d")

#Логи
logger_manager = LoggerManager(log_dir='logs', keep_days=7)
logging.info("Log file cleanup finished.")

#Чтение данных из конфига
dirname = os.path.dirname(__file__)
config_loader = ConfigLoader(os.path.join(dirname, "config.json"))
config = config_loader.config
if config is None or \
   not config.get('database') or \
   not config.get('api_endpoint'): 
    logging.error("Failed to load essential configuration sections. Exiting.")
    sys.exit(1)

#Скачивание данных в df по АПИ
api_url = config['api_endpoint'].get('url')
client = APIClient(timeout=60)
params = {'date': prev_day}
df = client.fetch_df(api_url, params=params)
logging.info(f"DataFrame shape: {df.shape if isinstance(df, pd.DataFrame) else 'N/A'}")

#Загрузка в БД
cols = ["client_id", "gender", "product_id", "quantity", "price_per_item", "discount_per_item","total_price", "purchase_datetime_full"]

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
        r.purchase_datetime_full.to_pydatetime(),  # python datetime
    )
    for r in df[cols].itertuples(index=False)
]

sql = """
INSERT INTO public.sales
(client_id, gender, product_id, quantity, price_per_item, discount_per_item, total_price, date_time)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
"""

db_params = config['database']
inserted = 0
try:
    with PGDataBase(
        host=db_params['host'],
        database=db_params['dbname'],
        user=db_params['user'],
        password=db_params['password'],
    ) as database:
        database.post_many(sql, params_seq)
        inserted = len(params_seq)

    logging.info(f"В БД успешно загружено {inserted} строк.")
except Exception as e:
    logging.error(f"Ошибка при вставке данных: {e}")