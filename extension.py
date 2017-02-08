from zipline.data.bundles import register

from maxdl import maxdl_bundle, nonnew_bundle, hs300_bundle

import pandas as pd


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
    'hs300dl',
    hs300_bundle,
    'SHSZ',
    start,
    end
)
