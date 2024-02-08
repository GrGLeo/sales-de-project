import requests
import os
import pandas as pd
import json
from datetime import datetime

from utils import flat_json

class Feeder:
    def __init__(self, dt_partition=None, limit=None,):
        self.item_path = os.getenv('JSON_ITEM_PATH')
        self.dt_partition = dt_partition
        self.limit = limit
        self.df = self.get_days_sales()

    def get_days_sales(self):
        url = os.getenv('API_URL')
        param = {
            'limit' : self.limit,
            'dt_partition' : self.dt_partition
        }
        response = requests.get(url, params=param)
        # TODO safeguard in case response != 200
        content = response.json()
        df = flat_json(content)
        df = pd.DataFrame(df)
        return df
        # TODO push zipped json to bucket

    def alim_user(self):
        self.df_user = self.df[['user_lastname','user_firstname','department','sexe','birth_date']]
        self.df_user.loc[:,'birth_date'] = pd.to_datetime(self.df_user['birth_date'])
        self.df_user.loc[:,'age'] = self.df_user['birth_date'].apply(lambda x : datetime.today().year - x.year)
        self.df_user.loc[:,'user_name'] = self.df['user_firstname'] + ' ' + self.df['user_lastname']
        self.df_user.loc[:,'sexe'] = self.df['sexe'].str.upper()

        mapper = {
            'LB_NAME':'user_name',
            'CD_DEPARTMENT':'department',
            'CD_SEXE':'sexe',
            'DT_BIRTH':'birth_date',
            'NB_AGE':'age'
        }
        self.df_user.rename({v:k for k,v in mapper.items()}, axis=1, inplace=True)
        selection = [k for k in mapper.keys()]
        self.df_user = self.df_user[selection]
        self.df_user.drop_duplicates(['LB_NAME','DT_BIRTH'],inplace=True)

    def alim_item(self):
        # TODO check if table exist on BQ
        item_price_path = self.item_path
        with open(item_price_path,'r') as f:
            item_price_table = json.load(f)

        item_price_table = {
            'NU_ITEM':[x[1] for x in item_price_table.values()],
            'LB_ITEM':list(item_price_table.keys()),
            'MT_ITEM':[x[0] for x in item_price_table.values()],
        }

        item_price_table = pd.DataFrame(item_price_table)

        # TODO verify new item, right new item

    def alim_sales(self):
        self.df_sales = self.df[['uid4','user_lastname','user_firstname','birth_date','item_id',\
            'price_payed','taxes','quantity','date']]
        self.df_sales['user_name'] = self.df['user_firstname'] + ' ' + self.df['user_lastname']
        self.df_sales['mt_taxes'] = self.df['price_payed'] * (self.df['taxes'] / 100)
        self.df_sales['mt_ttc'] = self.df['price_payed'] + self.df_sales['mt_taxes']
        self.df_sales['mt_total_ht'] = self.df['price_payed'] * self.df['quantity']
        self.df_sales['mt_total_ttc'] = self.df_sales['mt_ttc'] * self.df['quantity']

        mapper = {
            'uid4':'NU_SALE',
            'user_name':'LB_NAME',
            'birth_date':'DT_BIRTH',
            'item_id':'NU_ITEM',
            'price_payed':'MT_HT',
            'mt_taxes':'MT_TAXES',
            'quantity':'NB_ITEM',
            'mt_ttc':'MT_TTC',
            'mt_total_ht':'MT_TOTAL_HT',
            'mt_total_ttc':'MT_TOTAL_TTC',
            'date':'DT_PARTITION'
        }

        self.df_sales.rename(mapper, axis=1, inplace=True)
        selection = [v for v in mapper.values()]
        self.df_sales = self.df_sales[selection]

    def bq_alim(self):
        pass

    def compute(self):
        self.alim_user()
        self.alim_item()
        self.alim_sales()
        self.bq_alim()


if '__main__' == __name__:
    f = Feeder()
    f.compute()
