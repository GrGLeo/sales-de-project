from datetime import datetime
from pathlib import Path
import random
import json
import os
import numpy as np
from faker import Faker
from param import items_prices


class SaleCreator:
    'Class to create the fake sales data, to feed the API'
    def __init__(self, limit=None, dt_partition=None):
        self.limit = limit
        self.json_path = os.path.join(
            os.path.dirname(os.path.realpath(__file__)),
            'items_prices.json'
            )
        self.items_prices = self._get_json()

        if dt_partition:
            self.dt_partition = datetime.strptime(dt_partition,'%Y%m%d')
        else:
            self.dt_partition = datetime.today()

    def _get_json(self):
        'Load items from json if it exist or import from param and write a json'
        if Path(self.json_path).exists():
            with open(self.json_path,'r',encoding='UTF-8') as f:
                items = json.load(f)
        else:
            with open(self.json_path, 'w', encoding='UTF-8') as f:
                json.dump(items_prices,f)
            items = items_prices
        return items
    
    def get_sales(self):
        '''
        Generates fake data for a specified number of records.
        Uses the Faker library to create synthetic data.
        '''
        fake = Faker()
        if self.limit:
            total = self.limit
        else:
            total = np.random.randint(8000,10_000)
        return {
        fake.uuid4():
            {'user':{
                'user_lastname':fake.last_name(),
                'user_firstname':fake.first_name(),
                'department':random.randint(1,96),
                'sexe':random.choice(['m','f']),
                'birth_date':fake.date_of_birth(minimum_age=18)
            },
            'item':{
                'item_name':(i:=random.choice(list(self.items_prices.keys()))),
                'item_id':self.items_prices[i][1],
                'price':self.items_prices[i][0],
                'price_payed':round(self.items_prices[i][0] * np.random.uniform(0.95,1),2)
            },
            'taxes':random.choices([20,0], [a:=random.random(),1-a])[0],
            'quantity':random.randint(1,3),
            'date':self.dt_partition
            }
    for _ in range(total)
    }



if '__main__' == __name__:
    sales = SaleCreator(dt_partition='20141226',limit=1)
    print(sales.get_sales())
