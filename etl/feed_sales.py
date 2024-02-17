import datetime
from sqlalchemy import func, text
import pandas as pd 
from etl.feeder import Feeder
from models.tables import Raw, Sales


class FeedSales(Feeder):
    def __init__(self, dt_partition = None):
        super().__init__()
        if not dt_partition:
            self.dt_partition = datetime.date.today()
        else:
            self.dt_partition = dt_partition
        self.table = Sales
        self.session = self._get_session()
    
    def readiness(self):
        distinct_dates = self.session.query(func.date(Raw.date)).distinct().all()
        distinct_dates = [date_tuple[0] for date_tuple in distinct_dates]
        if self.dt_partition in distinct_dates:
            return True
        self.logger.warning('%s partition not alimented', self.dt_partition)
        return False
    
    def extract(self):
        query = '''SELECT * FROM raw WHERE date LIKE :dt_partition'''
        result = self.session.execute(text(query), {'dt_partition': f'{self.dt_partition}%'})
        self.df = pd.DataFrame(result.fetchall(), columns=result.keys())
    
    def transform(self):
        self.logger.info('Computing Sales table')
        df_sales = self.df[['uid4','user_lastname','user_firstname','birth_date','item_id',\
            'price_payed','taxes','quantity','date']]
        df_sales['user_name'] = self.df['user_firstname'] + ' ' + self.df['user_lastname']
        df_sales['mt_taxes'] = self.df['price_payed'] * (self.df['taxes'] / 100)
        df_sales['mt_ttc'] = self.df['price_payed'] + df_sales['mt_taxes']
        df_sales['mt_total_ht'] = self.df['price_payed'] * self.df['quantity']
        df_sales['mt_total_ttc'] = df_sales['mt_ttc'] * self.df['quantity']

        mapper = {
            'uid4':'NU_SALE',
            'user_name':'LB_NAME',
            'birth_date':'DT_BIRTH',
            'item_id':'NU_ITEM',
            'price_payed':'MT_HT',
            'mt_taxes':'MT_TAXES',
            'quantity':'NB_ITEMS',
            'mt_ttc':'MT_TTC',
            'mt_total_ht':'MT_TOTAL_HT',
            'mt_total_ttc':'MT_TOTAL_TTC',
            'date':'DT_PARTITION'
        }

        df_sales.rename(mapper, axis=1, inplace=True)
        selection = list(mapper.values())
        self.df_sales = df_sales[selection]
    
    def write(self):
        self.df_sales.to_sql('sales', con=self.session.bind, if_exists='append', index=False)
        self.logger.info('Insert %s rows', self.df_sales.shape[0])