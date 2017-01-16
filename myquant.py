from zipline.data.bundles import register
import pandas as pd
import os
import numpy as np
import matplotlib.pyplot as plt

from zipline.api import (
    schedule_function,
    symbol,
    order_target_percent,
    date_rules,
    record
)

import re
from zipline.algorithm import TradingAlgorithm
from zipline.finance.trading import TradingEnvironment
from zipline.utils.calendars import get_calendar, register_calendar
from zipline.finance import trading
from zipline.utils.factory import create_simulation_parameters
from zipline.data.bundles.core import load
from zipline.data.data_portal import DataPortal
from zipline.api import order_target, record, symbol, order_target_percent, order_percent

from loader import load_market_data

from cn_stock_holidays.zipline.default_calendar import shsz_calendar
from zipline.data.bundles.maxdl import maxdl_bundle

bundle = 'maxdl'

start_session_str = '2010-01-18'
end_session_str = '2016-12-30'

register(
        bundle,
        maxdl_bundle,
        "SHSZ",
        pd.Timestamp(start_session_str, tz='utc'),
        pd.Timestamp(end_session_str, tz='utc')
        )

bundle_data = load(bundle, os.environ, None,)

prefix, connstr = re.split(
    r'sqlite:///',
    str(bundle_data.asset_finder.engine.url),
    maxsplit=1,
)

env = trading.environment = TradingEnvironment(asset_db_path=connstr,
                                               trading_calendar=shsz_calendar,
                                               bm_symbol='000001.SS',
                                               load=load_market_data)

first_trading_day = bundle_data.equity_daily_bar_reader.first_trading_day

data = DataPortal(
    env.asset_finder, shsz_calendar,
    first_trading_day=first_trading_day,
#    equity_minute_reader=bundle_data.equity_minute_bar_reader,
    equity_daily_reader=bundle_data.equity_daily_bar_reader,
    adjustment_reader=bundle_data.adjustment_reader,
)

def initialize(context):
    context.i = 0
    context.full = False
    #schedule_function(handle_daily_data, date_rules.every_day())

def handle_daily_data(context, data):
    sym = symbol('002337.SZ')
#    print(data.current(symbol('002337.SZ'), 'open'))
    # Skip first 300 days to get full windows
    context.i += 1
    if context.i < 300:
        return

    # Compute averages
    # history() has to be called with the same params
    # from above and returns a pandas dataframe.
    short_mavg = data.history(sym, 'price', 100, '1d').mean()
    long_mavg = data.history(sym, 'price', 300, '1d').mean()

    # Trading logic
    if short_mavg > long_mavg:
        # order_target orders as many shares as needed to
        # achieve the desired number of shares.
        if not context.full:
            order_target_percent(sym, 0.9)
            context.full = True
        #order_target_percent(sym, 1)
#        context.order_percent(sym, 1)
    elif short_mavg < long_mavg:
        if context.full:
            order_target_percent(sym, 0)
            context.full = False
        #order_target_percent(sym, 0)
       # context.order_percent(sym, 0)

    # Save values for later inspection
    record(sxkj=data[sym].price,
           short_mavg=short_mavg,
           long_mavg=long_mavg)

def analyse(context, perf):
    fig = plt.figure()
    ax1 = fig.add_subplot(211)
    perf.portfolio_value.plot(ax=ax1)
    ax1.set_ylabel('portfolio value in ￥')

    ax2 = fig.add_subplot(212)
    perf['sxkj'].plot(ax=ax2)
    perf[['short_mavg', 'long_mavg']].plot(ax=ax2)

    perf_trans = perf.ix[[t != [] for t in perf.transactions]]
    buys = perf_trans.ix[[t[0]['amount'] > 0 for t in perf_trans.transactions]]
    sells = perf_trans.ix[
        [t[0]['amount'] < 0 for t in perf_trans.transactions]]
    ax2.plot(buys.index, perf.short_mavg.ix[buys.index],
             '^', markersize=10, color='m')
    ax2.plot(sells.index, perf.short_mavg.ix[sells.index],
             'v', markersize=10, color='k')
    ax2.set_ylabel('price in ￥')
    plt.legend(loc=0)
    plt.show()

if __name__ == "__main__":
    print("hello my quant.")
    sim_params = create_simulation_parameters(
        start=pd.to_datetime("2010-01-15 00:00:00").tz_localize("Asia/Shanghai"),
        end=pd.to_datetime("2016-12-30 00:00:00").tz_localize("Asia/Shanghai"),
        data_frequency="daily", emission_rate="daily", trading_calendar=shsz_calendar)

    perf = TradingAlgorithm(initialize=initialize,
                                 handle_data=handle_daily_data,
                                 sim_params=sim_params,
                                 env=trading.environment,
                                 trading_calendar=shsz_calendar,
                                 analyze=analyse
                                 ).run(data, overwrite_sim_params=False)

    perf.to_pickle('d:\\temp\\output.pickle')
    print(perf)
