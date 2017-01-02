from zipline.data.bundles import register
import pandas as pd
import os

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

from cn_stock_holidays.zipline.default_calendar import shsz_calendar
from zipline.data.bundles.maxdl import maxdl_bundle

bundle = 'maxdl'

start_session_str = '2016-10-01'

register(
        bundle,
        maxdl_bundle,
        "SHSZ",
        pd.Timestamp(start_session_str, tz='utc'),
        pd.Timestamp('2016-12-30', tz='utc')
        )

bundle_data = load(bundle, os.environ, None,)

prefix, connstr = re.split(
    r'sqlite:///',
    str(bundle_data.asset_finder.engine.url),
    maxsplit=1,
)

env = trading.environment = TradingEnvironment(asset_db_path=connstr,
                                               trading_calendar=shsz_calendar)

first_trading_day = bundle_data.equity_daily_bar_reader.first_trading_day

data = DataPortal(
    env.asset_finder, shsz_calendar,
    first_trading_day=first_trading_day,
    equity_minute_reader=bundle_data.equity_minute_bar_reader,
    equity_daily_reader=bundle_data.equity_daily_bar_reader,
    adjustment_reader=bundle_data.adjustment_reader,
)

def initialize(context):
    schedule_function(handle_daily_data, date_rules.every_day())

def handle_daily_data(context, data):
#    sym = symbol('002337.SZ')
    print(data.current(symbol('002337.SZ'), 'price'))

if __name__ == "__main__":
    print("hello my quant.")
    sim_params = create_simulation_parameters(
        start=pd.to_datetime(start_session_str + " 00:00:00").tz_localize("Asia/Shanghai"),
        end=pd.to_datetime("2016-10-09 00:00:00").tz_localize("Asia/Shanghai"),
        data_frequency="daily", emission_rate="daily", trading_calendar=shsz_calendar)

    algor_obj = TradingAlgorithm(initialize=initialize,
                                 handle_data=handle_daily_data,
                                 sim_params=sim_params,
                                 env=trading.environment,
                                 trading_calendar=shsz_calendar
                                 ).run(data, overwrite_sim_params=False)
