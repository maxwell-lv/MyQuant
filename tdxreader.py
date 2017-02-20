import pandas as pd
import os
import numpy as np
from math import floor

import struct
"""
读取通达信数据
"""


class TdxFileNotFoundException(Exception):
    pass


class TdxReader:

    def __init__(self, vipdoc_path):
        self.vipdoc_path = vipdoc_path

    def get_kline_by_code(self, code, exchange):
        fname = os.path.join(self.vipdoc_path, exchange)
        fname = os.path.join(fname, 'lday')
        fname = os.path.join(fname, '%s%s.day' % (exchange, code))
        return self.parse_data_by_file(fname)

    def get_mline_by_code(self, code, exchange):
        fname = os.path.join(self.vipdoc_path, exchange)
        fname = os.path.join(fname, 'minline')
        fname = os.path.join(fname, '%s%s.lc1' % (exchange, code))
        return self.parse_mdata_by_file(fname)

    def parse_data_by_file(self, fname):

        if not os.path.isfile(fname):
            raise TdxFileNotFoundException('no tdx kline data, please check path %s', fname)

        with open(fname, 'rb') as f:
            content = f.read()
            return self.unpack_records('<iiiiifii', content)
        return []

    def parse_mdata_by_file(self, fname):

        if not os.path.isfile(fname):
            raise  TdxFileNotFoundException("no tdx mline data, please check path %s", fname)

        with open(fname, 'rb') as f:
            content = f.read()
            return self.unpack_records('<HHfffffIxxxx', content)
        return []

    def unpack_records(self, format, data):
        record_struct = struct.Struct(format)
        return (record_struct.unpack_from(data, offset)
                for offset in range(0, len(data), record_struct.size))

    def get_df(self, code, exchange):
        data = [self._df_convert(row) for row in self.get_kline_by_code(code, exchange)]
        df =  pd.DataFrame(data=data, columns=('date', 'open', 'high', 'low', 'close', 'amount', 'volume'))
        df.index = pd.to_datetime(df.date)
        return df[['open', 'high', 'low', 'close', 'volume']]

    def get_mindf(self, code, exchange):
        data = [self._mindf_convert(row) for row in self.get_mline_by_code(code, exchange)]
        df = pd.DataFrame(data=data, columns=('datetime', 'open', 'high', 'low', 'close', 'amount', 'volume'))
        print(df.datetime)
        df.index = pd.to_datetime(df.datetime)
        return df[['open', 'high', 'low', 'close', 'volume']]

    def _df_convert(self, row):
        t_date = str(row[0])
        datestr = t_date[:4] + "-" + t_date[4:6] + "-" + t_date[6:]

        new_row = (
            datestr,
            row[1] * 0.01, # * 0.01 * 1000 , zipline need 1000 times to original price
            row[2] * 0.01,
            row[3] * 0.01,
            row[4] * 0.01,
            row[5],
            row[6]
        )

        return new_row

    def _mindf_convert(self, row):
        t_date = row[0]
        year = floor(t_date / 2048) + 2004
        month = floor((t_date % 2048) / 100)
        day = (t_date % 2014) % 100
        datestr = "%d-%2d-%2d" % (year, month, day)
        t_minute = row[1]
        hour = floor(t_minute / 60)
        minute = t_minute % 60
        timestr = "%d:%d:00" % (hour, minute)
        datetimestr = "%s %s" % (datestr, timestr)

        new_row = (
            datetimestr,
            row[2],
            row[3],
            row[4],
            row[5],
            row[6],
            row[7]
        )

        return new_row


if __name__ == '__main__':
    tdx_reader = TdxReader('c:\\new_tdx\\vipdoc\\')
    try:
        #for row in tdx_reader.parse_data_by_file('/Volumes/more/data/vipdoc/sh/lday/sh600000.day'):
        #    print(row)
        for row in tdx_reader.get_mline_by_code('600000', 'sh'):
            print(row)
    except TdxFileNotFoundException as e:
        pass

    print(tdx_reader.get_mindf('600000', 'sh'))
