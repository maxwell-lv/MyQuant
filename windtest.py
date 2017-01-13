from WindPy import w

import time

#pf = open('d:\\work\\pywsqdataif.data', 'w')

def DemoWSQCallback(indata):
    if indata.ErrorCode!=0:
        print('error code:'+str(indata.ErrorCode)+'\n');
        return();

    print(indata.Data[0][0])

if __name__ == "__main__":
    w.start()

    data=w.wsq("002337.SZ", "rt_time,rt_pre_close,rt_open,rt_high,rt_low,rt_last,rt_vol,rt_pct_chg,rt_swing,rt_vwap", func=DemoWSQCallback)
    print(data.Codes)