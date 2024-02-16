import datetime
import pandas as pd
from sqlalchemy import func, text
from etl.feeder import Feeder
from models.tables import Raw, User
pd.options.mode.copy_on_write = True



class FeedUser(Feeder):
    def __init__(self):
        super().__init__()
        self.dt_partition = datetime.date.today()
        self.dt_partition = datetime.date(2024, 2, 16)
        self.session = self._get_session()
        self.table = User
        
    def readiness(self):
        distinct_dates = self.session.query(func.date(Raw.date)).distinct().all()
        distinct_dates = [date_tuple[0] for date_tuple in distinct_dates]
        if self.dt_partition in distinct_dates:
            return True
        self.logger.warning('%s partition not alimented', self.dt_partition)
        return False
    
    def extract(self):
        query = '''
        SELECT *
        FROM raw
        WHERE date LIKE :dt_partition'''
        result = self.session.execute(text(query), {'dt_partition': f'{self.dt_partition}%'})
        self.df = pd.DataFrame(result.fetchall(), columns=result.keys())
    
    def transform(self):
        df_user = self.df[['user_lastname','user_firstname','department','sexe','birth_date']]
        df_user.loc[:,'birth_date'] = pd.to_datetime(df_user['birth_date'])
        df_user.loc[:,'age'] = df_user['birth_date']\
            .apply(lambda x : datetime.date.today().year - x.year)
        df_user.loc[:,'user_name'] = self.df['user_firstname'] + ' ' + self.df['user_lastname']
        df_user.loc[:,'sexe'] = self.df['sexe'].str.upper()
        mapper = {
            'LB_NAME':'user_name',
            'CD_DEPARTMENT':'department',
            'CD_SEXE':'sexe',
            'DT_BIRTH':'birth_date',
            'NB_AGE':'age'
        }
        df_user.rename({v:k for k,v in mapper.items()}, axis=1, inplace=True)
        selection = list(mapper.keys())
        df_user = df_user[selection]
        df_user.drop_duplicates(['LB_NAME','DT_BIRTH'],inplace=True)
        self.df_user = df_user
    
    def write(self):
        self.df_user.to_sql('user', con=self.session.bind, if_exists='append', index=False)
        self.logger.info('Insert %s rows', self.df_user.shape[0])
        