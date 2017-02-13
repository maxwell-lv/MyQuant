from sqlalchemy.sql import select
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, Column, Integer, String, Float, Date, MetaData
from sqlalchemy.ext.declarative import declarative_base
Base = declarative_base()
import sqlite3
from xlrd import open_workbook
import re

db = "sqlite:///d:\\projects\\ysdd.db"
project_excel = "d:\\projects\\project.xlsx"
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


class Phase(Base):
    __tablename__ = 'Phase'
    team = Column(String, primary_key=True)
    project = Column(String, primary_key=True)
    number = Column(Integer, primary_key=True)
    settlementdate = Column(String)
    positionratio = Column(Float)
    earningrate = Column(Float)


class MulitPK(Base):
    __tablename__ = 'test'
    team = Column(String, primary_key=True)
    name = Column(String, primary_key=True)


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
    project = project.split('期', 1)[0]
    return team, project


if __name__ == "__main__":
    engine = create_engine(db)
    Session.configure(bind=engine)
    session = Session()
    wb = open_workbook(project_excel)
    sheet = wb.sheet_by_index(0)
    names = []
    for row in range(sheet.nrows):
        s = sheet.cell(row, 1).value
        name, project_name = parse_name(s)
        p = Project()
        p.name = project_name
        p.team = name
        p.orderamount = float(sheet.cell(row, 2).value)
        p.riskcapital = float(sheet.cell(row, 3).value)
        p.partneramount = int(sheet.cell(row, 5).value)
        p.status = sheet.cell(row, 6).value
        p.date = str(sheet.cell(row, 0).value)
        if name not in names:
            names.append(name)
            session.add(Team(name=name, description="None"))
        session.add(p)
    session.commit()
