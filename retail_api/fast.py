from fastapi import FastAPI
from sales_creator import SaleCreator

app = FastAPI()

@app.get('/')
def index():
    return {'ok': True}

@app.get('/retail')
def get_sales(dt_partition:str =None, limit:int = None):
    sales = SaleCreator(limit=limit,dt_partition=dt_partition)
    return sales.get_sales()
