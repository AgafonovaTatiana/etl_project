# pgdb.py
import psycopg2
import logging

class PGDataBase:
    def __init__(self, host, database, user, password):
        self.connection = psycopg2.connect(host=host, database=database, user=user, password=password)
        self.connection.autocommit = False
        self.cursor = self.connection.cursor()
        logging.info('Подключение к базе данных установлено.')

    def post(self, query, params=None):
        """Выполняет один запрос. params — это tuple/list для cursor.execute."""
        try:
            if params is None:
                self.cursor.execute(query)
            else:
                self.cursor.execute(query, params)
            self.connection.commit()
        except Exception as err:
            self.connection.rollback()
            logging.error(f'Ошибка при выполнении запроса: {err}')
            raise

    def post_many(self, query, seq_of_params):
        """Батч-вставка. Можно заменить на executemany."""
        try:
            self.cursor.executemany(query, seq_of_params)
            self.connection.commit()
        except Exception as err:
            self.connection.rollback()
            logging.error(f'Ошибка при выполнении батч-вставки: {err}')
            raise

    def close(self):
        try:
            if getattr(self, 'cursor', None):
                self.cursor.close()
            if getattr(self, 'connection', None):
                self.connection.close()
            logging.info('Подключение к базе данных закрыто.')
        except Exception as err:
            logging.warning(f'Ошибка при закрытии соединения с БД: {err}')

    def __enter__(self): return self
    def __exit__(self, exc_type, exc_val, exc_tb): self.close()
