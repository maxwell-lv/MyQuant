from zipline.data.bundles import register

from zipline.data.bundles.maxdl import maxdl_bundle, nonnew_bundle
import pandas as pd
from cn_stock_holidays.zipline.default_calendar import shsz_calendar

register(
    'maxdl',
    maxdl_bundle,
    'SHSZ',
    pd.Timestamp('2008-12-19', tz='utc'),
    pd.Timestamp('2017-01-24', tz='utc')
)
register(
    'nonnewdl',
    nonnew_bundle,
    'SHSZ',
    pd.Timestamp('2008-12-19', tz='utc'),
    pd.Timestamp('2017-01-24', tz='utc')
)