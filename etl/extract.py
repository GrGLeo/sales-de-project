import os
import sys
import requests
from sqlalchemy import create_engine
from utils.logs import Logger
from utils import flat_json

class Extractor:
    def __init__(self, limit=None, dt_partition=None):
        self.logger = Logger('logger')
        self.limit = limit
        self.dt_partition = dt_partition
        self.engine = self._get_connection()
        pass
    
    def extract(self):
        df = self._get_day_sales()
        return df
        #df.to_sql(name='raw', con=self.engine, if_exists='append', index=False)
    
    def _get_day_sales(self):
        url = os.getenv('API_URL')
        param = {
            'limit': self.limit,
            'dt_partition': self.dt_partition,
        }
        response = requests.get(url, params=param, timeout=60)
        if response.status_code != 200:
            self.logger.critical('response %s',response)
            sys.exit()
        content = response.json()
        return flat_json(content)

    def _get_connection(self):
        connection_url = os.getenv('POSTGRES')
        engine = create_engine(connection_url)
        return engine
    
ext = Extractor(limit=10_000)
df = ext.extract()
print(sys.getsizeof(df))
