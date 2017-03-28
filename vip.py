from xlrd import open_workbook
from xlwt import Workbook
import pandas as pd
import sys

loan_file = 'd:\\projects\\vip\\vip_loan.xls'
profit_file = 'd:\\projects\\vip\\vip_profit.xls'
repay_file = 'd:\\projects\\vip\\vip_repay.xls'

loan_book = open_workbook(loan_file)
profit_book = open_workbook(profit_file)
repay_book = open_workbook(repay_file)

ploan = pd.read_excel(loan_file, '项目信息')
pprofit = pd.read_excel(profit_file, '每日收益信息')
prepay = pd.read_excel(repay_file, '项目结算信息')

loan_sheet = loan_book.sheet_by_index(0)
profit_sheet = profit_book.sheet_by_index(0)
repay_sheet = repay_book.sheet_by_index(0)


def get_project_number(project_name):
    global loan_sheet
    for row in range(loan_sheet.nrows):
        if loan_sheet.cell(row, 3).value == project_name:
            return str(int(loan_sheet.cell(row, 2).value))
    print('can\'t find %s' % project_name)
    return ''

def profit():
    wb = Workbook()
    sprofit = wb.add_sheet('每日收益信息')

    for row in range(1, profit_sheet.nrows):
        project_name = profit_sheet.cell(row, 1).value
        project_number = get_project_number(project_name)
        sprofit.write(row, 0, project_number)
        for i in range(1, 8):
            sprofit.write(row, i, profit_sheet.cell(row, i).value)

    wb.save('test.xls')

def repay():
    global repay_sheet
    wb = Workbook()
    srepay = wb.add_sheet('项目结算信息')

    for row in range(1, repay_sheet.nrows):
        project_name = repay_sheet.cell(row, 1).value
        project_number = get_project_number(project_name)
        srepay.write(row, 0, project_number)
        for i in range(1, 7):
            srepay.write(row, i, repay_sheet.cell(row, i).value)

    wb.save('repay.xls')

repay()