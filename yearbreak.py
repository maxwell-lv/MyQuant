import matplotlib.pyplot as plt

from zipline.pipeline.data import USEquityPricing
from zipline.pipeline.factors import SimpleMovingAverage
from zipline.pipeline import Pipeline
from zipline.api import (
attach_pipeline,
pipeline_output,
record,
symbol,
order_target_percent,
order_target_value
)

def initialize(context):
    context.state = 0
    sma_year = SimpleMovingAverage(inputs=[USEquityPricing.close], window_length=250)
    pipe = Pipeline(
        columns={
            'year':sma_year
        }
    )
    attach_pipeline(pipe, 'sma_pipeline')

def handle_data(context, data):
    pipe = pipeline_output('sma_pipeline')
    assests = data.keys()
    print([e.asset_name for e in assests])

def analyse(context, perf):
    fig = plt.figure()
    plt.show()
