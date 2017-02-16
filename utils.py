import tushare as ts
from datetime import datetime, timedelta, date
from cn_stock_holidays.zipline.default_calendar import shsz_calendar
import click
import pandas as pd

# 获取到目前为止还在连板的新股


def get_lianban_new_stock(before_date = None):
    stocks = ts.get_stock_basics()

    if before_date is None:
        before_date = datetime.now().date()
    last_trade_day = get_last_trade_day(before_date)

    lianban = {}
    i = 0
    total = len(stocks)
    for symbol in stocks.index:
        i += 1
        click.echo("checking symbol %s status (%d/%d)" % (symbol, i, total))
        try:
            timeToMarket = datetime.strptime(str(stocks.loc[symbol]['timeToMarket']), "%Y%m%d").date()
        except:
            # 跳过还没上市的
            continue
        if timeToMarket >= last_trade_day:
            print("跳过当前日期(%s)首日上市和未上市的 %s" % (last_trade_day, symbol))
            continue
        start = shsz_calendar.sessions_window(last_trade_day, -50)[0].date()
        # 取之前50个交易日的数据，应该不可能有超过这么长时间连板的
        data = ts.get_k_data(code=symbol, start=str(start), end=str(last_trade_day))
        data.set_index('date', inplace=True)
        test_day = last_trade_day
        while test_day > timeToMarket:
            try:
                if is_yiziban(data.loc[str(test_day)]) is False:
                    break
            except:
                click.echo("error %s on %s" % (symbol, test_day))
                break
            test_day = shsz_calendar.previous_session_label(test_day).date()
        # 找到一个一直一字板的
        if test_day == timeToMarket:
            lianban[symbol] = stocks.loc[symbol]

    return lianban

# 测试是不是一字板，这个有英语么？老外又没有涨停板，怎么翻译。


def is_yiziban(data):
    if data['open'] == data['close'] == data['high'] == data['low']:
        return True
    return False


def get_last_trade_day(last_date = None):
    if last_date is None:
        last_date = datetime.now().date()
    oneday = timedelta(days=1)
    while shsz_calendar.is_session(last_date) is False:
        last_date -= oneday
    return last_date


def ban_test(preclose, last):
    zhangting = round(preclose * 1.1, 2)
    dieting = round(preclose * 0.9, 2)
    if last == zhangting:
        return 1
    elif last == dieting:
        return -1
    else:
        return 0


def yiziban_break(o, h, l, p):
    if h != l:
        if o > l:
            return 1
        if o < h:
            return -1
    return 0


def symbol_to_wind(symbol):
    isymbol = int(symbol)
    if (isymbol>=600000):
        return symbol + '.SH'
    else:
        return symbol + '.SZ'


def wind_to_dataframe(data):
    d = {}
    i = 0
    for field in data.Fields:
        d[field] = data.Data[i]
        i += 1
    df = pd.DataFrame(d, index=data.Codes)
    return df


if __name__ == "__main__":
    result = get_lianban_new_stock(date(year=2017, month=1, day=26))
    print(result)
