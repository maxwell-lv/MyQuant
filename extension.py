from zipline.data.bundles import register

from zipline.data.bundles.maxdl import maxdl_bundle

register(
    'maxdl',
    maxdl_bundle,
)