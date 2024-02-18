import os
import sys
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from utils.logs import Logger


# TODO check data availability
# TODO pull and transform data
# TODO write data into user table

class Feeder:
    def __init__(self):
        self.logger = Logger('logger')
        self.session = self._get_session()
        self.table = None
        self.status = 'FAILED'
        
    def readiness(self):
        return True
    
    def extract(self):
        pass
    
    def transform(self):
        pass
    
    def write(self):
        self.df_transform.to_sql(self.__name__.lower(), con=self.session.bind, if_exists='append', index=False)
    
    def _step(self,func):
        self.logger.info('Starting %s',func.__name__)
        func()
        self.logger.info('%s complete', func.__name__)
        
    def compute(self):
        self.logger.info('Starting %s table computing', self.__name__)
        ready = self.readiness()
        if ready:
            self._step(self.extract)
            self._step(self.transform)
            self._step(self.write)
            self.status = 'SUCESS'
        self.logger.write(self.status,self.df_transform.shape[0], self.__name__)
        
        
    @staticmethod
    def _get_session():
        connection_url = os.getenv('POSTGRES')
        engine = create_engine(connection_url)
        Session = sessionmaker(bind=engine)
        session = Session()
        return session
    
    @property
    def __name__(self):
        return self.table.__name__.upper()
    