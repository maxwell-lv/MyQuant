from zipline.data.bundles import register

from maxdl import maxdl_bundle, nonnew_bundle, hs300_bundle

import pandas as pd
from cn_stock_holidays.zipline.default_calendar import shsz_calendar


start = pd.Timestamp('2008-12-19', tz='utc')
end = pd.Timestamp('2016-12-30', tz='utc')


register(
    'maxdl',
    maxdl_bundle,
    'SHSZ',
    pd.Timestamp('2008-12-19', tz='utc'),
    pd.Timestamp('2016-12-30', tz='utc')
)
register(
    'nonnewdl',
    nonnew_bundle,
    'SHSZ',
    pd.Timestamp('2008-12-19', tz='utc'),
    pd.Timestamp('2016-12-30', tz='utc')
)
register(
    'hs300',
    hs300_bundle,
    'SHSZ',
    start,
    end
)