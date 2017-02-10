from xlrd import open_workbook
import pandas as pd


p = 'd:\\projects\\ysdd.xlsm'
w = 'd:\\projects\\total.xls'
wb = open_workbook(p)
table = {}
for s in wb.sheets():
    date = s.name
    for row in range(s.nrows):
        name = s.cell(row, 0).value
        value = s.cell(row, 3).value
        if name not in table:
            table[name] = {}
        table[name][date] = value
df = pd.DataFrame.from_dict(table, orient='index')
df.sort_index(axis=1, inplace=True)
df.to_excel(w)
