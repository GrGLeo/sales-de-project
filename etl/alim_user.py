import os
import json
from datetime import datetime
import requests
import pandas as pd
from utils import flat_json
from utils.big_data import push_to_s3
pd.options.mode.copy_on_write = True

class Feeder:
    def __init__(self, dt_partition=None, limit=None,):
        self.item_path = os.getenv('JSON_ITEM_PATH')
        self.dt_partition = dt_partition
        self.limit = limit
        self.get_days_sales()
        self.alim_user()
        self.alim_item()
        self.alim_sales()

    def get_days_sales(self):
        '''
        Make an API call to get raw data sales, transform it into a pandas DataFrame.
        Save it to an s3 bucket
        '''
        url = os.getenv('API_URL')
        param = {
            'limit' : self.limit,
            'dt_partition' : self.dt_partition
        }
        response = requests.get(url, params=param,timeout=60)
        # TODO safeguard in case response != 200
        content = response.json()
        df = flat_json(content)
        # create a saved raw file on s3
        push_to_s3(df, 'raw_data.json','')
        print('Pushed to s3')
        self.df = pd.DataFrame(df)

    def alim_user(self):
        '''
        Retrive user info from the raw data, and calculate columns
        '''
        self.df_user = self.df[['user_lastname','user_firstname','department','sexe','birth_date']]
        self.df_user.loc[:,'birth_date'] = pd.to_datetime(self.df_user['birth_date'])
        self.df_user.loc[:,'age'] = self.df_user['birth_date']\
            .apply(lambda x : datetime.today().year - x.year)
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
        selection = list(mapper.keys())
        self.df_user = self.df_user[selection]
        self.df_user.drop_duplicates(['LB_NAME','DT_BIRTH'],inplace=True)

    def alim_item(self):
        '''
        If table is not on redshift, push the json files
        else, check is there is new value in raw data
        and write the new value if needed
        '''
        # TODO check if table exist on AWS
        item_price_path = self.item_path
        with open(item_price_path,'r',encoding='UTF-8') as file:
            item_price_table = json.load(file)

        item_price_table = {
            'NU_ITEM':[x[1] for x in item_price_table.values()],
            'LB_ITEM':list(item_price_table.keys()),
            'MT_ITEM':[x[0] for x in item_price_table.values()],
        }

        item_price_table = pd.DataFrame(item_price_table)

        # TODO verify new item, write new item

    def alim_sales(self):
        '''
        Retrive the sales information, from the raw data
        '''
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
        selection = list(mapper.values())
        self.df_sales = self.df_sales[selection]

    def redshift_alim(self):
        '''
        Feed a table to redshift
        '''
        return

    def compute(self):
        '''
        Feed all three table to the redshit data warehouse
        '''
        self.redshift_alim()


if '__main__' == __name__:
    f = Feeder()
    f.compute()
