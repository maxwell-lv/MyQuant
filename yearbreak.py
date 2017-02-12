import matplotlib.pyplot as plt
import math
from six import viewkeys
from zipline.pipeline.data import USEquityPricing
from zipline.pipeline.factors import SimpleMovingAverage
from zipline.pipeline import Pipeline
from zipline.api import (
attach_pipeline,
pipeline_output,
record,
symbol,
order_target_percent,
order_target_value,
get_datetime
)


def initialize(context):
    context.state = 0
    sma_year = SimpleMovingAverage(inputs=[USEquityPricing.close], window_length=250)
    sma_short = SimpleMovingAverage(inputs=[USEquityPricing.close], window_length=5)
    sma_long = SimpleMovingAverage(inputs=[USEquityPricing.close], window_length=20)
    pipe = Pipeline(
        columns={
            'year': sma_year,
            'short': sma_short,
            'long': sma_long
        }
    )
    attach_pipeline(pipe, 'sma_pipeline')
    context.states = {}
    context.i = 0


def handle_data(context, data):
    if context.i < 250:
        context.i += 1
        return
    pipe = pipeline_output('sma_pipeline')
    assets = data.keys()
    for sym in assets:
        today = get_datetime()
        if data.current(sym, 'close') == 0.0 or math.isnan(data.current(sym, 'close')):
            continue
        if sym not in context.states:
            context.states[sym] = 0
        state = context.states[sym]
        try:
            short = pipe['short'][sym]
            long = pipe['long'][sym]
            year = pipe['year'][sym]
        except KeyError:
            print(today, sym, data.current(sym, 'close'))
            continue
        except IndexError:
            print(today, sym, data.current(sym, 'close'))
            continue
        if state == 0:
            if short < year:
                state = 1
        elif state == 1:
            if short > year:
                state = 2
                order_target_percent(sym, 0.1)
        elif state == 2:
            if short < long:
                state = 0
                order_target_percent(sym, 0)
        context.states[sym] = state


def analyse(context, perf):
    fig = plt.figure()
    plt.show()
