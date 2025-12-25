from datetime import datetime

from sqlalchemy import Column, Integer, String, Numeric, Text, PrimaryKeyConstraint
from sqlalchemy.dialects.mysql import DATETIME
from sqlalchemy.ext.declarative import declarative_base

# Создание основного класса
Base = declarative_base()

class PSDATA(Base):
    __tablename__ = 'tab_progn_spr_gaz_d314'
    __table_args__ = {'schema': 'public'}

    # Поля описываются точно как в QUERY:
    id = Column(Integer, primary_key=True)
    tab_fo_d314_ids = Column(Integer)  # Ключ к Федеральный округ
    tab_region_d314_ids = Column(Integer)  # Ключ к Регион
    tab_otrasl_economy_d314_ids = Column(Integer)  # Ключ к Отрасль
    tab_contragent_d314_ids = Column(Integer)  # Ключ к Потребитель
    tab_status_potreb_d314_ids = Column(Integer)  # Ключ к Статус потребителя
    tab_group_post_d314_ids= Column(Integer)  # Ключ к Группа поставщиков
    tab_dogovor_visual_d314_ids = Column(Integer)  # Ключ к Договор
    otl_usl = Column(Integer)  # Ключ к Отл.усл
    takeorpay = Column(Integer)  # Ключ к take-or-pay
    tab_tu_visual_d314_ids = Column(Integer)  # Ключ к ТУ
    tu308 = Column(Integer)  # Ключ к ТУ308
    tab_infr_d314_ids = Column(Integer)  # Ключ к Наличие инфраструктуры
    gen_schema= Column(Integer)  # Ключ к Ген.схема
    poruch = Column(String)  # Ключ к Поручения
    tab_pg_visual_d314_ids = Column(Integer)  # Ключ к Готовность объекта
    tab_ver_real_pr_d314_ids = Column(Integer)  # Ключ к Вероятность реализации проекта
    year = Column(Integer)  # Ключ к Год
    summ = Column(Numeric)  # Ключ к Сумма
    post = Column(Integer)  # Ключ к Поставщик
    tab_proizvoditel_d314_ids = Column(Integer)  # Ключ к Производитель
    tab_start_gaz_d314_ids = Column(Integer)  # Ключ к Начало отбора
    date = Column(DATETIME)

class Prirost(Base):
    __tablename__ = 'tab_prirost_d314'
    __table_args__ = {'schema': 'public'}

    # Поля описываются точно как в QUERY:
    id = Column(Integer, primary_key=True)
    tab_fo_d314_ids = Column(Integer)  # Ключ к Федеральный округ
    tab_region_d314_ids = Column(Integer)  # Ключ к Регион
    tab_otrasl_economy_d314_ids = Column(Integer)  # Ключ к Отрасль
    tab_contragent_d314_ids = Column(Integer)  # Ключ к Потребитель
    tab_status_potreb_d314_ids = Column(Integer)  # Ключ к Статус потребителя
    tab_group_post_d314_ids= Column(Integer)  # Ключ к Группа поставщиков
    tab_dogovor_visual_d314_ids = Column(Integer)  # Ключ к Договор
    tab_tu_visual_d314_ids = Column(Integer)  # Ключ к ТУ
    tab_infr_d314_ids = Column(Integer)  # Ключ к Наличие инфраструктуры
    tab_ver_real_pr_d314_ids = Column(Integer)  # Ключ к Вероятность реализации проекта
    summ = Column(Numeric)  # Ключ к Сумма
    tab_start_gaz_d314_ids = Column(Integer)  # Ключ к Начало отбора
    yearfrom = Column(Integer)  # Ключ к Год
    yearto = Column(Integer)  # Ключ к Год
    date = Column(DATETIME)

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

class Contragent(Base):
    __tablename__ = 'tab_contragent_d314'
    __table_args__ = {'schema': 'public'}
    id = Column(Integer, primary_key=True)
    name = Column(Text, unique=True)
    inn = Column(Text)

class GroupPost(Base):
    __tablename__ = 'tab_group_post_d314'
    __table_args__ = {'schema': 'public'}
    id = Column(Integer, primary_key=True)
    name = Column(Text, unique=True)
class VersProgn(Base):
    __tablename__ = 'tab_ver_real_pr_d314'
    __table_args__ = {'schema': 'public'}
    id = Column(Integer, primary_key=True)
    name = Column(Text, unique=True)
    full_name = Column(Text)
    short_name = Column(Text)

class Proizv(Base):
    __tablename__ = 'tab_proizvoditel_d314'
    __table_args__ = {'schema': 'public'}
    id = Column(Integer, primary_key=True)
    name = Column(Text, unique=True)
    short_name = Column(Text)

class StPotr(Base):
    __tablename__ = 'tab_status_potreb_d314'
    __table_args__ = {'schema': 'public'}
    id = Column(Integer, primary_key=True)
    name = Column(Text, unique=True)

class StGaz(Base):
    __tablename__ = 'tab_start_gaz_d314'
    __table_args__ = {'schema': 'public'}
    id = Column(Integer, primary_key=True)
    name = Column(Text, unique=True)

class Dogovor(Base):
    __tablename__ = 'tab_dogovor_visual_d314'
    __table_args__ = {'schema': 'public'}
    id = Column(Integer, primary_key=True)
    name = Column(Text, unique=True)

class PG(Base):
    __tablename__ = 'tab_pg_visual_d314'
    __table_args__ = {'schema': 'public'}
    id = Column(Integer, primary_key=True)
    name = Column(Text, unique=True)

class TU(Base):
    __tablename__ = 'tab_tu_visual_d314'
    __table_args__ = {'schema': 'public'}
    id = Column(Integer, primary_key=True)
    name = Column(Text, unique=True)

class Infr(Base):
    __tablename__ = 'tab_infr_d314'
    __table_args__ = {'schema': 'public'}
    id = Column(Integer, primary_key=True)
    name = Column(Text, unique=True)

# Определить эталонные модели и их атрибуты. Поля можно задать без _ids
reference_models = {
    'TAB_FO_D314': FedState,
    'tab_region_d314': Regions,
    'tab_otrasl_economy_d314': Otrasl,
    'tab_contragent_d314': Contragent,
    'tab_group_post_d314': GroupPost,
    'tab_ver_real_pr_d314': VersProgn,
    'tab_proizvoditel_d314': Proizv,
    'tab_status_potreb_d314': StPotr,
    'tab_start_gaz_d314': StGaz,
    'tab_dogovor_visual_d314': Dogovor,
    'tab_pg_visual_d314': PG,
    'tab_tu_visual_d314': TU,
    'tab_infr_d314': Infr
}

