import WindPy as w
import tushare as ts
from utils import ban_test


snapshot = ts.get_today_all()
dieting = []
for i, row in snapshot.iterrows():
    if ban_test(row['settlement'], row['trade']) == -1:
        dieting.append(row['code'])
print(dieting)