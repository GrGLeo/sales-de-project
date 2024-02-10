from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, Float, String, DateTime
from sqlalchemy import create_engine

CONNECTION_URL = 'postgresql://lg:glop@localhost:5432/sales_db'
engine = create_engine(CONNECTION_URL)

Base = declarative_base()

class User(Base):
    __tablename__ = 'users'
  
    LB_NAME = Column(String, primary_key=True)
    DT_BIRTH = Column(DateTime, primary_key=True)
    CD_DEPARTMENT = Column(String)
    CD_SEXE = Column(String)
    NB_AGE = Column(Integer)
    
class Item(Base):
    __tablename__ = 'items'
    
    NU_ITEM = Column(Integer, primary_key=True)
    LB_ITEM = Column(String)
    MT_ITEM = Column(Float)
    
class Sales(Base):
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
    
    
Base.metadata.create_all(engine)

