from datetime import datetime

from sqlalchemy import Column, Integer, String, Numeric, Text, PrimaryKeyConstraint
from sqlalchemy.dialects.mysql import DATETIME
from sqlalchemy.ext.declarative import declarative_base

# Создание основного класса
Base = declarative_base()

class Data_Base(Base):
    __tablename__ = 'tab_sut_post_rep_d314'
    __table_args__ = {'schema': 'public'}

    # Поля описываются точно как в QUERY:
    id = Column(Integer, primary_key=True)
    tab_str_rep_sut_post_d314_ids = Column(Integer)  # Ключ к Строке
    date = Column(Text)  # Дата
    plan = Column(Numeric)  # Сумма Плана
    factreal = Column(Numeric)  # Сумма Факта
    fact_last_year = Column(Numeric)  # Сумма Факта прошлого года
    factrealni = Column(Numeric)  # Сумма Факта нарастающим итогом
    plan_ni = Column(Numeric)  # Сумма Плана с нарастающим итогом
    fact_last_year_ni = Column(Numeric)  # Сумма Факта прошлого года с нарастающим итогом
    datetime = Column(DATETIME)  # Дата

class Data_Region(Base):
    __tablename__ = 'tab_sh_perebor_region_d314'
    __table_args__ = {'schema': 'public'}

    # Поля описываются точно как в QUERY:
    id = Column(Integer, primary_key=True)
    date = Column(Text)  # Дата
    col = Column(Integer)  # Количество
    tab_region_d314_ids = Column(Integer)  # Ключ к Регион
    pn = Column(Integer)  # Перебор/Невыборка
    plan = Column(Numeric)  # Сумма Плана
    fact = Column(Numeric)  # Сумма Факта
    tab_fo_d314_ids = Column(Integer)  # Ключ к Федеральный округ
    datetime = Column(DATETIME) # Дата


class Data_Otrasl(Base):
    __tablename__ = 'tab_sh_perebor_otrasl_d314'
    __table_args__ = {'schema': 'public'}
    # Поля описываются точно как в QUERY:
    id = Column(Integer, primary_key=True)
    date = Column(Text)  # Дата
    col = Column(Integer)  # Количество
    tab_otrasl_economy_d314_ids = Column(Integer)  # Ключ к Отрасль
    pn = Column(Integer)  # Перебор/Невыборка
    plan = Column(Numeric)  # Сумма Плана
    fact = Column(Numeric)  # Сумма Факта
    datetime = Column(DATETIME)  # Дата

class FedState(Base):
    __tablename__ = 'tab_fo_d314'
    __table_args__ = {'schema': 'public'}
    id = Column(Integer, primary_key=True)
    name = Column(Text, unique=True)
    ord = Column(Integer)
    short_name = Column(Text)

class Regions(Base):
    __tablename__ = 'tab_region_d314'
    __table_args__ = {'schema': 'public'}
    id = Column(Integer, primary_key=True)
    name = Column(Text, unique=True)
    ord = Column(Integer)
    short_name = Column(Text)
    tab_fo_d314_ids = Column(Integer)
    mid_name = Column(Text)
    real_name = Column(Text)

class Otrasl(Base):
    __tablename__ = 'tab_otrasl_economy_d314'
    __table_args__ = {'schema': 'public'}
    id = Column(Integer, primary_key=True)
    name = Column(Text, unique=True)
    ord = Column(Integer)
    full_name = Column(Text)

class StrRep(Base):
    __tablename__ = 'tab_str_rep_sut_post_d314'
    __table_args__ = {'schema': 'public'}
    id = Column(Integer, primary_key=True)
    name = Column(Text, unique=True)

# Определить эталонные модели и их атрибуты. Поля можно задать без _ids
reference_models = {
    'tab_fo_d314': FedState,
    'tab_region_d314': Regions,
    'tab_otrasl_economy_d314': Otrasl,
    'tab_str_rep_sut_post_d314': StrRep
}

