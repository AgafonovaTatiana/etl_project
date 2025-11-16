import os
from datetime import date, timedelta, datetime
import logging
import glob

class LoggerManager:
    """
    Manages logging setup and cleanup.
    Sets up logging to a file based on the current date and cleans up logs older than 7 days.
    """
    def __init__(self, log_dir='logs', keep_days=7):
        self.log_dir = log_dir
        self.keep_days = keep_days
        self._setup_logging()

    def _setup_logging(self):
        if not os.path.exists(self.log_dir):
            os.makedirs(self.log_dir)
        
        current_date_str = datetime.now().strftime('%Y-%m-%d')
        log_file_path = os.path.join(self.log_dir, f'{current_date_str}.log')
        
        self._cleanup_old_logs()

        for handler in logging.root.handlers[:]:
            logging.root.removeHandler(handler)
        
        logging.basicConfig(
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', 
            level=logging.INFO, 
            handlers=[logging.FileHandler(log_file_path, 'a', 'utf-8')]
            )

    def _cleanup_old_logs(self):
        now = datetime.now()
        for file in glob.glob(os.path.join(self.log_dir, '*.log')):
            file_date_str = os.path.basename(file).split('.')[0]
            try:
                file_date = datetime.strptime(file_date_str, '%Y-%m-%d')
                if now - file_date > timedelta(days=self.keep_days):
                    os.remove(file)
            except ValueError:
                logging.warning(f"Skipping log file with unexpected name format: {file}")
            except Exception as e:
                logging.error(f"Error deleting log file {file}: {e}")