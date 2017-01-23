from zipline.data import bundles as bundles_module
import os
import pandas as pd
from zipline.data.bundles import register

from zipline.data.bundles.maxdl import maxdl_bundle, nonnew_bundle
from cn_stock_holidays.zipline.default_calendar import shsz_calendar

register(
    'maxdl',
    nonnew_bundle,
    calendar='SHSZ',
    start_session=pd.Timestamp('2008-01-04', tz='utc'),
    end_session=pd.Timestamp('2016-12-30', tz='utc')
)

def ingest(bundle, assets_version, show_progress):
    """Ingest the data for the given bundle.
    """
    bundles_module.ingest(
        bundle,
        os.environ,
        pd.Timestamp.utcnow(),
        assets_version,
        show_progress,
    )

if __name__ == "__main__":
    ingest(bundle='maxdl', assets_version=(), show_progress=False)