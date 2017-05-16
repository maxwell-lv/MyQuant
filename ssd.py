from sqlalchemy.sql import select
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, Column, Integer, String, Float, Date, MetaData
from sqlalchemy.ext.declarative import declarative_base
Base = declarative_base()
import sqlite3
from xlrd import open_workbook
import re
import click
from datetime import datetime, date
import pandas as pd


db = "sqlite:///ysdd.db"
project_excel = "d:\\projects\\project.xlsx"
perf_excel = "d:\\projects\\ysdd.xlsm"
phase_excel = "d:\\projects\\phase.xlsx"
Session = sessionmaker()


class Team(Base):
    __tablename__ = 'Team'
    name = Column(String, primary_key=True)
    description = Column(String)

    def __repr__(self):
        return "<Team(name='%s', description='%s')>" % (self.name, self.description)


class Project(Base):
    __tablename__ = 'Project'
    name = Column(String, primary_key=True)
    team = Column(String, primary_key=True)
    date = Column(String)
    orderamount = Column(Float)
    riskcapital = Column(Float)
    partneramount = Column(Integer)
    status = Column(String)
    period = Column(Integer)
    numberofperiod = Column(Integer)


class Performance(Base):
    __tablename__ = 'Performance'
    team = Column(String, primary_key=True)
    project = Column(String, primary_key=True)
    number = Column(Integer, primary_key=True)
    reportdate = Column(String)
    positionratio = Column(Float)
    earningrate = Column(Float)


class Phase(Base):
    __tablename__ = 'Phase'
    team = Column(String, primary_key=True)
    project = Column(String, primary_key=True)
    number = Column(Integer, primary_key=True)
    settlementdate = Column(String, primary_key=True)
    traderratio = Column(Float)
    investorratio = Column(Float)
    marketvalue = Column(Float)
    redemption = Column(String)


class MyList(Base):
    __tablename__ = 'list'
    name = Column(String, primary_key=True)
    type = Column(String)
    date = Column(Date, primary_key=True)
    marketvalue = Column(Integer)
    investorratio = Column(Float)
    traderratio = Column(Float)
    ratioperday = Column(Float)


def clear(engine):
    con = engine.connect()
    meta = con.metadata
    trans = con.begin()
    for table in reversed(meta.sorted_tables):
        con.execute(table.delete())
    trans.commit()


def parse_name(value):
    team = value.split('V', 1)[0]
    project = value.split(team, 1)[1]
    t = project.split('期', 1)
    if len(t) > 1:
        project = t[0] + t[1]
    else:
        project = t[0]
    return team, project


def parse_period(value):
    result = re.search("\d+\*\d+", value)
    p, n = result.group(0).split('*')
    return int(p), int(n)


def find_period(value):
    result = re.search("\d+\*\d+", value)
    p, n = result.group(0).split('*')
    return int(p)


def parse_number_of_period(value, n):
    return n - len(value.split(';')) + 1


def float_to_date(value):
    if type(value) is float:
        return str(int(value))
    elif type(value) is str:
        return value


@click.group()
def main():
    click.echo('main')


@main.command()
@click.argument('filename', type=click.Path(exists=True))
def ls(filename):
    click.echo('history convert')
    engine = create_engine(db)
    MyList.metadata.create_all(engine)
    Session.configure(bind=engine)
    session = Session()
    wb = open_workbook(filename)
    for sheet in wb.sheets():
        year = datetime.strptime(sheet.name, "%Y%m").date().year
        for row in range(0, sheet.nrows, 2):
            datestr = str(int(sheet.cell_value(row, 0)))
            if len(datestr) == 3:
                datestr = '0' + datestr
            d = datetime.strptime(datestr, "%m%d").date()
            reportdate = date(year, d.month, d.day)
            name = sheet.cell_value(row, 1)
            type = sheet.cell_value(row + 1, 1)
            marketvalue = sheet.cell_value(row, 2)
            investorratio = sheet.cell_value(row, 3)
            traderratio = sheet.cell_value(row, 4)
            days = find_period(type)
            ratioperday = investorratio / days
            session.add(MyList(name=name, type=type, date=reportdate, marketvalue=marketvalue,
                               investorratio=investorratio, traderratio=traderratio,
                               ratioperday=ratioperday))
    session.commit()


def convert_project_name(name):
    parts = name.split('V')
    return parts[0]

@main.command()
def stats():
    engine = create_engine(db)
    perf = pd.read_sql_table('list', engine)
    perf['name'] = perf['name'].apply(convert_project_name)
    t = perf.groupby(by=['name'])
    mean = t['ratioperday'].mean()
    median = t['ratioperday'].median()
    d = {'mean': mean,
         'medain': median}
    df = pd.DataFrame(d)
    click.echo(df)
    df.to_excel('report.xlsx')


if __name__ == "__main__":
    main()
    # engine = create_engine(db)
    # Session.configure(bind=engine)
    # session = Session()
    # wb = open_workbook(project_excel)
    # sheet = wb.sheet_by_index(0)
    # names = []
    # for row in range(sheet.nrows):
    #     s = sheet.cell(row, 1).value
    #     name, project_name = parse_name(s)
    #     p = Project()
    #     p.name = project_name
    #     p.team = name
    #     p.orderamount = float(sheet.cell(row, 2).value)
    #     p.riskcapital = float(sheet.cell(row, 3).value)
    #     p.partneramount = int(sheet.cell(row, 5).value)
    #     p.status = sheet.cell(row, 6).value
    #     p.date = str(sheet.cell(row, 0).value)
    #     if name not in names:
    #         names.append(name)
    #         session.add(Team(name=name, description="None"))
    #     session.add(p)
    # session.commit()

    # perf_wb = open_workbook(perf_excel)
    # sheet = perf_wb.sheet_by_index(0)
    # date = sheet.name
    # for row in range(sheet.nrows):
    #     period, number = parse_period(sheet.cell(row, 4).value)
    #     team, project = parse_name(sheet.cell(row, 0).value)
    #     position = sheet.cell(row, 2).value
    #     earning = sheet.cell(row, 3).value
    #     tp = sheet.cell_value(row, 1)
    #     tp = tp if type(tp) is str else str(int(tp))
    #     phase = parse_number_of_period(tp, number)
    #     session.add(Performance(team=team, project=project, number=phase, reportdate=date, positionratio=position, earningrate=earning))
    # session.commit()

    # phase_wb = open_workbook(phase_excel)
    # sheet = phase_wb.sheet_by_index(0)
    # for row in range(0, sheet.nrows, 2):
    #     settlementdate = float_to_date(sheet.cell_value(row, 0))
    #     team, project = parse_name(sheet.cell_value(row, 1))
    #     marketvalue = sheet.cell_value(row, 2)
    #     investorratio = sheet.cell_value(row, 3)
    #     traderratio = sheet.cell_value(row, 4)
    #     redemption = sheet.cell_value(row, 5)
    #     session.add(Phase(team=team, project=project, number=1, marketvalue=marketvalue, settlementdate=settlementdate, investorratio=investorratio, traderratio=traderratio, redemption=redemption))
    # session.commit()

