from etl.alim_user import Feeder

f = Feeder()
f.compute()
print(f.df_user.head())
