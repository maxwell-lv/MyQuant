import click
import tushare as ts
import pandas as pd
import os
from datetime import datetime, timedelta, date
from cn_stock_holidays.zipline.default_calendar import shsz_calendar

from zipline.data.bundles import core as bundles


def hs300_bundle(environ,
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
    if show_progress:
        click.echo('getting hs300 stock info')

    hs300 = ts.get_hs300s()
    get_stock_info(environ,
                   asset_db_writer,
                   minute_bar_writer,
                   daily_bar_writer,
                   adjustment_writer,
                   calendar,
                   start_session,
                   end_session,
                   cache,
                   show_progress,
                   output_dir,
                   hs300.code.tolist())


def get_stock_info(environ,
                   asset_db_writer,
                   minute_bar_writer,
                   daily_bar_writer,
                   adjustment_writer,
                   calendar,
                   start_session,
                   end_session,
                   cache,
                   show_progress,
                   output_dir,
                   symbols):

    ts_symbols = ts.get_stock_basics()
    assets = []
    histories = {}
    i = 0
    total = len(symbols)
    for index, row in ts_symbols.iterrows():
        if index in symbols:
            i += 1
            srow = {}
            click.echo("getting symbol %s(%s) history (%d/%d)" % (index, row['name'], i, total))
            histories[index] = ts.get_k_data(index, start=timestamp_to_date_string(start_session), end=timestamp_to_date_string(end_session))
            if histories[index].size == 0:
                click.echo("no data for %s(%s) between %s and %s" % (index, row['name'], start_session, end_session))
                continue
            srow['start_date'] = histories[index].iloc[0].date
            srow['end_date'] = histories[index].iloc[-1].date
            srow['symbol'] = index
            srow['asset_name'] = row['name']
            assets.append(srow)

    df_symbols = pd.DataFrame(data=assets).sort_values('symbol')
    symbol_map = pd.DataFrame.copy(df_symbols.symbol)

    # fix the symbol exchange info
    df = df_symbols.apply(func=convert_symbol_series, axis=1)

    asset_db_writer.write(df)
    daily_bar_writer.write(get_hist_data(symbol_map, histories, start_session, end_session, calendar),
                           show_progress=show_progress)
    adjustment_writer.write()


def nonnew_bundle(environ,
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
    if show_progress:
        click.echo('getting non-new stock info')

    ts_symbols = ts.get_stock_basics()

    symbols = []

    histories = {}

    i = 0
    total = len(ts_symbols)
    today = datetime.now().date()
    for index, row in ts_symbols.iterrows():
        i += 1
        srow = {}
        click.echo("getting symbol %s(%s) history (%d/%d)" % (index, row['name'], i, total))
        ittm = ts_symbols.loc[index]['timeToMarket']
        if ittm == 0:
            continue
        timeToMarket = timeInttoDate(ittm)
        if today - timeToMarket < timedelta(365):
            print('too new.')
            continue
        histories[index] = ts.get_k_data(index, start=timestamp_to_date_string(start_session), end=timestamp_to_date_string(end_session))
        srow['start_date'] = histories[index].iloc[0].date
        srow['end_date'] = histories[index].iloc[-1].date
        srow['symbol'] = index
        srow['asset_name'] = row['name']
        symbols.append(srow)

    df_symbols = pd.DataFrame(data=symbols).sort_values('symbol')
    symbol_map = pd.DataFrame.copy(df_symbols.symbol)

    # fix the symbol exchange info
    df = df_symbols.apply(func=convert_symbol_series, axis=1)

    asset_db_writer.write(df)
    daily_bar_writer.write(get_hist_data(symbol_map, histories, start_session, end_session, calendar),
                           show_progress=show_progress)
    adjustment_writer.write()


def timeInttoDate(timeInt):
    return datetime.strptime(str(timeInt), "%Y%m%d").date()


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
    daily_bar_writer.write(get_hist_data(symbol_map, histories, start_session, end_session, calendar), show_progress=show_progress)
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
        i += 1

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


def get_hist_data(symbol_map, histories, start_session, end_session, calendar):
    for sid, index in symbol_map.iteritems():
        history = histories[index]
        del history['code']
        history.set_index('date', inplace=True)
        sessions = calendar.sessions_in_range(start_session, end_session)
        # new_index = sessions.tz_localize(None).date
        new_index = sessions.strftime("%Y-%m-%d")
        history = history.reindex(new_index, copy=False, fill_value=0.0)
        yield sid, history.sort_index()

if __name__ == "__main__":
    environ = os.environ
    end = datetime(year=2016, month=6, day=10)  # "2016-06-10"
    start = datetime(year=2016, month=6, day=1)  # "2016-06-01"
    hs300_bundle(environ,
                 None,
                 None,
                 None,
                 None,
                 shsz_calendar,
                 start,
                 end,
                 None,
                 True,
                 None)
