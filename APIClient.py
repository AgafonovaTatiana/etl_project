import requests
import logging
import pandas as pd

class APIClient:
    def __init__(self, timeout=60):
        self.timeout = timeout
    
    def fetch_raw(self, full_url, params=None):
        logging.info(f"API request: {full_url} | params={params}")
        try:
            r = requests.get(full_url, params=params, timeout=self.timeout)
            logging.info(f"API response: status={r.status_code}, content-type={r.headers.get('Content-Type')}")
            r.raise_for_status()
            data = r.json()
            if data is None:
                return []
            if isinstance(data, list):
                return data
            if isinstance(data, dict) and isinstance(data.get("data"), list):
                return data["data"]
            return []
        except requests.RequestException as e:
            logging.error(f"fetch_raw: {e}")
            return []
        except Exception as e:
            logging.error(f"fetch_raw unexpected: {e}")
            return []
        
    
    def fetch_df(self, full_url, params=None):
        records = self.fetch_raw(full_url, params)
        df = pd.DataFrame(records)
        if df.empty:
            return df
        date_col = "purchase_datetime"
        sec_col  = "purchase_time_as_seconds_from_midnight"
        if date_col in df.columns and sec_col in df.columns:
            df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
            df["_purchase_time"] = pd.to_timedelta(df[sec_col], unit="s")
            df["purchase_datetime_full"] = df[date_col] + df["_purchase_time"]
            df = df.drop(columns=[date_col, sec_col, "_purchase_time"], errors="ignore")

        return df
