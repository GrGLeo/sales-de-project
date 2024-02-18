#from etl.alim_user import Feeder
from etl.extract import Extractor
from etl.feed_user import FeedUser
from etl.feed_sales import FeedSales
from etl.feeder import Feeder


ext = Extractor()
ext.extract()
fed = FeedSales()
fed.compute()
print(fed.__name__)
# f_ = FeedSales()
# f_.compute()
# f_ = FeedUser()
# f_.compute()
# f = Feeder()
# f.compute()
