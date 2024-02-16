import os
import sys
import logging
import colorlog
from sqlalchemy import create_engine, func as F
from sqlalchemy.orm import sessionmaker
from models.tables import Base, Info

class Logger(logging.Logger):
    def __init__(self, name, level=logging.NOTSET):
        super().__init__(name, level)
        file_handler = logging.FileHandler('example.log')
        file_handler.setLevel(logging.WARNING)
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        formatter = colorlog.ColoredFormatter(
            '%(log_color)s%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            log_colors={
                'DEBUG': 'cyan',
                'INFO': 'green',
                'WARNING': 'yellow',
                'ERROR': 'red',
                'CRITICAL': 'bold_red',
            }
        )
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)

        self.addHandler(file_handler)
        self.addHandler(console_handler)
        self.engine = self._get_connection()

    def write(self,count,name):
        Base.metadata.bind = self.engine
        Session = sessionmaker(bind=self.engine)
        session = Session()
        id = session.query(F.count(Info.ID)).scalar()
        new_record = Info(ID=id, TABLE=name.upper(),ROW=count)
        session.add(new_record)
        session.commit()
        session.close()

    def _get_connection(self):
        try:
            connection_url = os.getenv('POSTGRES')
            engine = create_engine(connection_url)
            return engine
        except Exception:
            self.critical('No connection to PostgreSQL')
            sys.exit()

logger = Logger('log')
