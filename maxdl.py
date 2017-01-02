import click
import tushare as ts
import pandas as pd
import os
import datetime

from zipline.data.bundles import core as bundles

#@bundles.register('maxdl')
def maxdl_bundle(environ,
                 asset_db_writer,
                 minute_bar_writer,
                 daily_bar_writer,
                 adjustment_writer,
                 calendar,
                 start_session,
                 end_session,
                 cache,
                 show_progress,
                 output_dir):
    """Build a zipline data bundle from any dataset.
    """
    symbol_list = environ.get('test_symbols')
    index_list = environ.get('benchmark_symbols')
    metadata, histories, symbol_map = get_basic_info(show_progress=show_progress, symbol_list=symbol_list, start_session=timestamp_to_date_string(start_session), end_session=timestamp_to_date_string(end_session))
    asset_db_writer.write(metadata)
    daily_bar_writer.write(get_hist_data(symbol_map, histories), show_progress=show_progress)
    adjustment_writer.write()

def timestamp_to_date_string(timestamp):
    return timestamp.date().strftime('%Y-%m-%d')

def get_basic_info(show_progress=True, symbol_list=None, start_session=None, end_session=None):
    if show_progress:
        click.echo("getting stock basic info")
    ts_symbols = ts.get_stock_basics()
    if show_progress:
        click.echo("writing stock list")

    symbols = []

    histories = {}

    if symbol_list is not None:
        f = open(symbol_list)
        c_symbols = f.read().splitlines()
        ts_symbols = ts_symbols.loc[c_symbols]

    i = 0
    total = len(ts_symbols)
    for index, row in ts_symbols.iterrows():
        i = i +1

        srow = {}
        click.echo("getting symbol %s(%s) history (%d/%d)" % (index, row['name'], i, total))
        histories[index] = ts.get_k_data(index, start=start_session, end=end_session)
        srow['start_date'] = histories[index].iloc[0].date
        srow['end_date'] = histories[index].iloc[-1].date
        srow['symbol'] = index
        srow['asset_name'] = row['name']
        symbols.append(srow)

    df_symbols = pd.DataFrame(data=symbols).sort_values('symbol')
    symbol_map = pd.DataFrame.copy(df_symbols.symbol)

    # fix the symbol exchange info
    df = df_symbols.apply(func=convert_symbol_series, axis=1)


    return df, histories, symbol_map

def symbol_to_exchange(symbol):
    isymbol = int(symbol)
    if (isymbol>=600000):
        return symbol + ".SS", "SSE"
    else:
        return symbol + ".SZ", "SZSE"

def convert_symbol_series(s):
    symbol, e = symbol_to_exchange(s['symbol'])
    s['symbol'] = symbol
    s['exchange'] = e
    return s

def get_hist_data(symbol_map, histories):
    for sid, index in symbol_map.iteritems():
        history = histories[index]
        del history['code']
        history.set_index('date', inplace=True)
        yield sid, history.sort_index()

if __name__ == "__main__":
    environ = os.environ
    sl = environ.get('test_symbols')
    end = "2016-12-20"
    start = "2016-01-01"
    d, h, s = get_basic_info(symbol_list=sl, start_session=start, end_session=end)
    print(d)
    for s, h in get_hist_data(s, h):
        print(s)
        print(h)
