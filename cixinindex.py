import utils
from datetime import datetime,timedelta,date
from cn_stock_holidays.zipline.default_calendar import shsz_calendar
import tushare as ts
import click
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib
matplotlib.style.use('ggplot')

#today = datetime.now().date()
#checkday = today-timedelta(days=1)
#lianban = utils.get_lianban_new_stock(checkday)
#for stock in lianban:
#    timeToMarket = datetime.strptime(str(lianban[stock]['timeToMarket']), "%Y%m%d").date()
#    days = shsz_calendar.session_distance(timeToMarket, checkday)
#    print(stock['name'], days)


def cxindex(start, end=None):
    if end == None:
        end = datetime.now().date()

    sessions = shsz_calendar.sessions_in_range(start, end)

    stocks = ts.get_stock_basics()

    start_str = start.strftime('%Y%m%d')
    newstocks = stocks.loc[stocks['timeToMarket'] >= int(start_str)]

    df = pd.DataFrame(0, index=newstocks.index, columns=sessions.date)

    i = 0
    total = len(newstocks)
    for symbol in newstocks.index:
        i += 1
        click.echo("checking symbol %s status (%d/%d)" % (symbol, i, total))
        try:
            time_to_market = datetime.strptime(str(stocks.loc[symbol]['timeToMarket']), "%Y%m%d").date()
        except:
            click.echo("wrong time to market of %s" % symbol)
            continue
        test_day = shsz_calendar.next_session_label(time_to_market)
        data = ts.get_k_data(symbol, end=end.strftime("%Y-%m-%d"), start=test_day.date().strftime("%Y-%m-%d"))
        count = 0
        for row in data.iterrows():
            if utils.is_yiziban(row[1]):
                count += 1
                column = datetime.strptime(row[1]['date'], "%Y-%m-%d").date()
                df.set_value(symbol, column, count)
            else:
                break

    df.to_csv('test.csv')


def calculate_cx_index():
    df = pd.DataFrame.from_csv('test.csv')
    result = df.apply(lambda value:value != 0)
    index = result.sum()
    index.plot()
    index2 = df.sum()/index
    index2.plot()
    plt.show()


cxindex(date(2016, 1, 1))
calculate_cx_index()
