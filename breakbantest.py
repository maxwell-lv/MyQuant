from WindPy import w
import asyncio
from utils import get_lianban_new_stock, symbol_to_wind, wind_to_dataframe
from datetime import datetime,timedelta
import os
import click
import winsound



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

base = None
breaklist = []


@asyncio.coroutine
def main():
    global base
    stocks = get_lianban_stock_list()
    symbols = [symbol_to_wind(s) for s in stocks]
    fields = "rt_pre_close"
    windcodes = ','.join(symbols)
    w.start()
    data = w.wsq(windcodes, fields)
    df = wind_to_dataframe(data)
    df['zhangting'] = (df['RT_PRE_CLOSE'] * 1.1).round(2)
    base = df
    w.wsq(windcodes, "rt_last", func=wind_callback)

def wind_callback(indata):
    global base, breaklist
    if indata.ErrorCode != 0:
        click.echo('error code:'+str(indata.ErrorCode))
        return
    df = wind_to_dataframe(indata)
    for i, r in df.iterrows():
        if i in breaklist:
            continue
        if r['RT_LAST'] < base.loc[i]['zhangting']:
            click.echo('%s break' % i)
            winsound.MessageBeep()
            breaklist.append(i)

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    asyncio.ensure_future(main())
    loop.run_forever()

