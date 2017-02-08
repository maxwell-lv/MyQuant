from zipline.api import (
    history,
    order_target,
    record,
    symbol,
)

import WindPy as w

def initialize(context):
    context.i = 0

def handle_data(context, data):
    #for d in data:
    #    print(data.current(symbol('600433.SS'), 'price'))
    sym = symbol('600433.SS')
    #sym = symbol('002337.SZ')
    print(sym)

w.start()
data = w.wss("000001.SZ", "sec_name")