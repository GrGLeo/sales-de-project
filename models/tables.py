import os
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, Float, String, DateTime
from sqlalchemy import create_engine

CONNECTION_URL = os.getenv('POSTGRES') #change to glop if not working
engine = create_engine(CONNECTION_URL)

Base = declarative_base()

class Raw(Base):
    '''Raw table documentation'''
    __tablename__ = 'raw'
    uid4 = Column(String, primary_key=True)
    user_lastname = Column(String)
    user_firstname = Column(String)
    department = Column(Integer)
    sexe = Column(String)
    birth_date = Column(String)
    item_name = Column(String)
    item_id = Column(Integer)
    price = Column(Float)
    price_payed = Column(Float)
    taxes = Column(Integer)
    quantity = Column(Integer)
    date = Column(String)

class User(Base):
    'User table documentation'
    __tablename__ = 'users'
    LB_NAME = Column(String, primary_key=True)
    DT_BIRTH = Column(DateTime, primary_key=True)
    CD_DEPARTMENT = Column(String)
    CD_SEXE = Column(String)
    NB_AGE = Column(Integer)

class Item(Base):
    'Items table documentation'
    __tablename__ = 'items'
    NU_ITEM = Column(Integer, primary_key=True)
    LB_ITEM = Column(String)
    MT_ITEM = Column(Float)

class Sales(Base):
    'Sales table documentation'
    __tablename__ = 'sales'
    NU_SALE = Column(String, primary_key=True)
    LB_NAME = Column(String)
    DT_BIRTH = Column(DateTime)
    NU_ITEM = Column(Integer)
    MT_HT = Column(Float)
    MT_TAXES = Column(Float)
    NB_ITEMS = Column(Integer)
    MT_TTC = Column(Float)
    MT_TOTAL_HT = Column(Float)
    MT_TOTAL_TTC = Column(Float)
    DT_PARTITION = Column(DateTime)

class Info(Base):
    'Info table documentation'
    __tablename__ = 'info'
    ID = Column(Integer, primary_key=True)
    STATUS = Column(String)
    TABLE = Column(String)
    ROW = Column(Integer)
    DATE = Column(DateTime)
    
Base.metadata.create_all(engine)
