import datetime
from etl.feeder import Feeder



class FeedUser(Feeder):
    def __init__(self):
        super().__init__()
        self.dt_partition = datetime.datetime.today()
        self.engine = self._get_connection()
        
    def readiness(self):
        