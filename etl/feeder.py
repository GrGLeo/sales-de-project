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
        
    def readiness(self):
        return True
    
    def extract(self):
        pass
    
    def transform(self):
        pass
    
    def write(self):
        pass
    
    def _step(self,func):
        self.logger.info('Starting %s',func.__name__)
        func()
        self.logger.info('%s complete', func.__name__)
        
    def compute(self):
        self.logger.info('Starting %s table computing', self.table.__tablename__.upper())
        ready = self.readiness()
        if ready:
            self._step(self.extract)
            self._step(self.transform)
            self._step(self.write)
        
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
    