import errno
import os
import re
import sys
from functools import wraps

import click
try:
    from pygments import highlight
    from pygments.lexers import PythonLexer
    from pygments.formatters import TerminalFormatter
    PYGMENTS = True
except:
    PYGMENTS = False
import logbook
import pandas as pd
from six import text_type

from zipline.data.bundles.core import load
from zipline.data import bundles as bundles_module
from zipline.utils.cli import Date, Timestamp
from zipline.utils.run_algo import load_extensions
from zipline.algorithm import TradingAlgorithm
from zipline.data.data_portal import DataPortal
from zipline.finance.trading import TradingEnvironment
from zipline.pipeline.data import USEquityPricing
from zipline.pipeline.loaders import USEquityPricingLoader
from zipline.utils.calendars import get_calendar
from zipline.utils.factory import create_simulation_parameters
from zipline.finance.commission import PerDollar
from zipline.finance.blotter import Blotter
from zipline.finance.slippage import FixedSlippage
import zipline.utils.paths as pth
from cn_stock_holidays.zipline.default_calendar import shsz_calendar
from loader import load_market_data

@click.group()
@click.option(
    '-e',
    '--extension',
    multiple=True,
    help='File or module path to a zipline extension to load.',
)
@click.option(
    '--strict-extensions/--non-strict-extensions',
    is_flag=True,
    help='If --strict-extensions is passed then zipline will not run if it'
    ' cannot load all of the specified extensions. If this is not passed or'
    ' --non-strict-extensions is passed then the failure will be logged but'
    ' execution will continue.',
)
@click.option(
    '--default-extension/--no-default-extension',
    is_flag=True,
    default=True,
    help="Don't load the default zipline extension.py file in $ZIPLINE_HOME.",
)
def main(extension, strict_extensions, default_extension):
    logbook.StderrHandler().push_application()
    load_extensions(
        default_extension,
        extension,
        strict_extensions,
        os.environ,
    )

@main.command()
@click.option(
    '-f',
    '--algofile',
    default=None,
    type=click.File('r'),
    help='The file that contains the algorithm to run.',
)
@click.option(
    '--data-frequency',
    type=click.Choice({'daily', 'minute'}),
    default='daily',
    show_default=True,
    help='The data frequency of the simulation.',
)
@click.option(
    '--capital-base',
    type=float,
    default=10e6,
    show_default=True,
    help='The starting capital for the simulation.',
)
@click.option(
    '-b',
    '--bundle',
    default='quantopian-quandl',
    metavar='BUNDLE-NAME',
    show_default=True,
    help='The data bundle to use for the simulation.',
)
@click.option(
    '--bundle-timestamp',
    type=Timestamp(),
    default=pd.Timestamp.utcnow(),
    show_default=False,
    help='The date to lookup data on or before.\n'
    '[default: <current-time>]'
)
@click.option(
    '-s',
    '--start',
    type=Date(tz='utc', as_timestamp=True),
    help='The start date of the simulation.',
)
@click.option(
    '-e',
    '--end',
    type=Date(tz='utc', as_timestamp=True),
    help='The end date of the simulation.',
)
@click.option(
    '-o',
    '--output',
    default='-',
    metavar='FILENAME',
    show_default=True,
    help="The location to write the perf data. If this is '-' the perf will"
    " be written to stdout.",
)
@click.option(
    '--print-algo/--no-print-algo',
    is_flag=True,
    default=False,
    help='Print the algorithm to stdout.',
)
@click.pass_context
def run(ctx,
        algofile,
        data_frequency,
        capital_base,
        bundle,
        bundle_timestamp,
        start,
        end,
        output,
        print_algo):
    """Run a backtest for the given algorithm.
    """
    # check that the start and end dates are passed correctly
    if start is None and end is None:
        # check both at the same time to avoid the case where a user
        # does not pass either of these and then passes the first only
        # to be told they need to pass the second argument also
        ctx.fail(
            "must specify dates with '-s' / '--start' and '-e' / '--end'",
        )
    if start is None:
        ctx.fail("must specify a start date with '-s' / '--start'")
    if end is None:
        ctx.fail("must specify an end date with '-e' / '--end'")

    if algofile is None:
        ctx.fail(
            "must specify exactly one of '-f' / '--algofile'",
        )

    perf = _run(
        initialize=None,
        handle_data=None,
        before_trading_start=None,
        analyze=None,
        algofile=algofile,
        data_frequency=data_frequency,
        capital_base=capital_base,
        data=None,
        bundle=bundle,
        bundle_timestamp=bundle_timestamp,
        start=start,
        end=end,
        output=output,
        print_algo=print_algo,
        environ=os.environ,
    )

    if output == '-':
        click.echo(str(perf))
    elif output != os.devnull:  # make the zipline magic not write any data
        perf.to_pickle(output)

    return perf

def _run(handle_data,
         initialize,
         before_trading_start,
         analyze,
         algofile,
         data_frequency,
         capital_base,
         data,
         bundle,
         bundle_timestamp,
         start,
         end,
         output,
         print_algo,
         environ):

    namespace = {}
    if algofile is not None:
        algotext = algofile.read()

    if print_algo:
        if PYGMENTS:
            highlight(
                algotext,
                PythonLexer(),
                TerminalFormatter(),
                outfile=sys.stdout,
            )
        else:
            click.echo(algotext)

    if bundle is not None:
        bundle_data = load(
            bundle,
            environ,
            bundle_timestamp,
        )

        prefix, connstr = re.split(
            r'sqlite:///',
            str(bundle_data.asset_finder.engine.url),
            maxsplit=1,
        )
        if prefix:
            raise ValueError(
                "invalid url %r, must begin with 'sqlite:///'" %
                str(bundle_data.asset_finder.engine.url),
            )
        env = TradingEnvironment(asset_db_path=connstr,
                                 trading_calendar=shsz_calendar,
                                 bm_symbol='000001.SS',
                                 load=load_market_data)
        first_trading_day =\
            bundle_data.equity_minute_bar_reader.first_trading_day
        data = DataPortal(
            env.asset_finder, get_calendar("SHSZ"),
            first_trading_day=first_trading_day,
            # equity_minute_reader=bundle_data.equity_minute_bar_reader,
            equity_daily_reader=bundle_data.equity_daily_bar_reader,
            adjustment_reader=bundle_data.adjustment_reader,
        )

        pipeline_loader = USEquityPricingLoader(
            bundle_data.equity_daily_bar_reader,
            bundle_data.adjustment_reader,
        )

        def choose_loader(column):
            if column in USEquityPricing.columns:
                return pipeline_loader
            raise ValueError(
                "No PipelineLoader registered for column %s." % column
            )
    else:
        env = None
        choose_loader = None

    blotter = Blotter(data_frequency = data_frequency,
                      asset_finder=env.asset_finder,
                      slippage_func=FixedSlippage(),
                      commission = PerDollar(cost=0.00025))

    perf = TradingAlgorithm(
        namespace=namespace,
        capital_base=capital_base,
        env=env,
        trading_calendar=shsz_calendar,
        get_pipeline_loader=choose_loader,
        blotter=blotter,
        sim_params=create_simulation_parameters(
            start=start,
            end=end,
            capital_base=capital_base,
            data_frequency=data_frequency,
            trading_calendar=shsz_calendar
        ),
        **{
            'initialize': initialize,
            'handle_data': handle_data,
            'before_trading_start': before_trading_start,
            'analyze': analyze,
        } if algotext is None else {
            'algo_filename': getattr(algofile, 'name', '<algorithm>'),
            'script': algotext,
        }
    ).run(
        data,
        overwrite_sim_params=False,
    )

    if output == '-':
        click.echo(str(perf))
    elif output != os.devnull:  # make the zipline magic not write any data
        perf.to_pickle(output)

    return perf


@main.command()
@click.option(
    '-b',
    '--bundle',
    default='quantopian-quandl',
    metavar='BUNDLE-NAME',
    show_default=True,
    help='The data bundle to ingest.',
)
@click.option(
    '--assets-version',
    type=int,
    multiple=True,
    help='Version of the assets db to which to downgrade.',
)
@click.option(
    '--show-progress/--no-show-progress',
    default=True,
    help='Print progress information to the terminal.'
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

if __name__ == '__main__':
    main()
