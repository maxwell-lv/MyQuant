from WindPy import w
import asyncio
from utils import get_lianban_new_stock, symbol_to_wind
from datetime import datetime,timedelta
import os


@asyncio.coroutine
def test():
    print('running')


def get_lianban_stock_list():
    today = datetime.now().date()
    file_name = str(today) + ".lianban"
    if os.path.exists(file_name):
        file = open(file_name)
        symbols = [l.strip() for l in file.readlines()]
    else:
        stocks = get_lianban_new_stock(today-timedelta(days=1))
        symbols = stocks.keys()
        file = open(file_name, mode='w')
        file.writelines([s+"\n" for s in symbols])
    file.close()
    return symbols


@asyncio.coroutine
def main():
    stocks = get_lianban_stock_list()
    symbols = [symbol_to_wind(s) for s in stocks]
    fields = "rt_pre_close,rt_low,rt_last"
    windcodes = ','.join(symbols)
    w.start()
    data = w.wsq(windcodes, fields)
    print(data)

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    asyncio.ensure_future(main())
    loop.run_forever()

