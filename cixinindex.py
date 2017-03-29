import utils
from datetime import datetime,timedelta
from cn_stock_holidays.zipline.default_calendar import shsz_calendar

today = datetime.now().date()
checkday = today-timedelta(days=1)
lianban = utils.get_lianban_new_stock(checkday)
for stock in lianban:
    timeToMarket = datetime.strptime(str(lianban[stock]['timeToMarket']), "%Y%m%d").date()
    days = shsz_calendar.session_distance(timeToMarket, checkday)
    print(stock['name'], days)