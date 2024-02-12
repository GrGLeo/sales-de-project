import os
import json
from datetime import datetime
import requests
import pandas as pd
import botocore.exceptions
from sqlalchemy import create_engine
from utils import flat_json
from utils.big_data import AwsInstance
from utils.logs import Logger
pd.options.mode.copy_on_write = True

class Feeder:
    '''
     Class for fetching, transforming, and feeding data to Redshift.

    This class initializes an instance of AwsInstance to interact with AWS services.
    It provides methods for retrieving raw data, transforming it into suitable formats,
    and feeding it into PostgreSQL.
    '''
    def __init__(self, dt_partition=None, limit=None,):
        self.dt_partition = dt_partition
        self.limit = limit
        self.instance = AwsInstance()
        self.logger = Logger('logger')
        self._get_days_sales()
        self.alim_user()
        self.alim_item()
        self.alim_sales()

    def _get_days_sales(self):
        'Retrieves raw sales data through an API call and transforms it into a pandas DataFrame.'
        url = os.getenv('API_URL')
        param = {
            'limit' : self.limit,
            'dt_partition' : self.dt_partition
        }
        response = requests.get(url, params=param,timeout=60)
        # TODO safeguard in case response != 200
        content = response.json()
        df = flat_json(content)
        self.logger.info(df.keys())
        # create a saved raw file on s3
        try:
            self.instance.create_bucket()
        except botocore.exceptions.ClientError:
            pass
        self.instance.push_to_s3(df, 'raw_data.json')
        self.logger.info('Pushing data to %s', self.instance.bucket_name)
        self.df = pd.DataFrame(df)

    def alim_user(self):
        '''
        Extracts and processes user information from the raw data.
        '''
        self.logger.info('Computing User table')
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
        Processes item data from the raw data.
        If the item table is not present in Redshift, this method pushes JSON files
        containing item information. Otherwise, it checks for new values in the raw data
        and updates the table in Redshift accordingly. This process involves data validation,
        synchronization.
        '''
        # TODO check if table exist on AWS
        item_price_path = os.getenv('JSON_ITEM_PATH')
        with open(item_price_path,'r',encoding='UTF-8') as file:
            item_price_table = json.load(file)

        item_price_table = {
            'NU_ITEM':[x[1] for x in item_price_table.values()],
            'LB_ITEM':list(item_price_table.keys()),
            'MT_ITEM':[x[0] for x in item_price_table.values()],
        }

        self.df_items = pd.DataFrame(item_price_table)

        # TODO verify new item, write new item

    def alim_sales(self):
        '''
        Retrive the sales information, from the raw data
        '''
        self.logger.info('Computing Sales table')
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
            'quantity':'NB_ITEMS',
            'mt_ttc':'MT_TTC',
            'mt_total_ht':'MT_TOTAL_HT',
            'mt_total_ttc':'MT_TOTAL_TTC',
            'date':'DT_PARTITION'
        }

        self.df_sales.rename(mapper, axis=1, inplace=True)
        selection = list(mapper.values())
        self.df_sales = self.df_sales[selection]

    def postgres_alim(self):
        '''
        Feed a table to postgresql
        '''
        connection_url = os.getenv('POSTGRES')
        engine = create_engine(connection_url)
        self.df.to_sql(name='raw', con=engine, if_exists='append', index=False)
        self.df_user.to_sql(name='users', con=engine, if_exists='append', index=False)
        count = self.df_user.shape[0]
        self.logger.write(count, 'user')
        self.logger.info('Inserting %s rows in Users', count)
        self.df_sales.to_sql(name='sales', con=engine, if_exists='append', index=False)
        count = self.df_user.shape[0]
        self.logger.info('Inserting %s rows in Sales', count)
        self.logger.write(count, 'sales')
        self.df_items.to_sql(name='items', con=engine, if_exists='replace', index=False)
        count = self.df_items.shape[0]
        self.logger.write(count,'items')
        self.logger.info('Inserting %s rows in Items', count)
        
    def compute(self):
        '''
        Feed all three table to the redshit data warehouse
        '''
        self.postgres_alim()


if '__main__' == __name__:
    f = Feeder()
    f.compute()
    