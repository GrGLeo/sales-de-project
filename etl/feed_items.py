import datetime
import pandas as pd
from models.tables import Item
from etl.feeder import Feeder


class FeedItem(Feeder):
    def __init__(self, dt_partition = None):
        super().__init__()
        if not dt_partition:
            self.dt_partition = datetime.date.today()
        else:
            self.dt_partition = dt_partition
        self.session = self._get_session()
        self.table = Item

    def readiness(self):
        return