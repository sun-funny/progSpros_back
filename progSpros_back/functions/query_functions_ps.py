from copy import copy
from datetime import date, datetime
from enum import Enum
import itertools
from typing import Any, Dict, Iterable, List, Optional, Tuple
from sqlalchemy import Column, desc, func, and_, case, CTE, literal, or_, select, Case, text, Text
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.sql.expression import cast
from sqlalchemy.orm import aliased, scoped_session, Query
from progSpros_back.functions.file_upload_functions_ps import CaseDescriptor, ColumnDescriptor
from progSpros_back.model.mappings_ps import version_leveled_mappings
from progSpros_back.model.db_models_ps import PG, PSDATA, TU, Contragent, Dogovor, FedState, Otrasl, Regions, StGaz, VersProgn
# from progSpros_back.model.db_models_ps import PG, PSDATA, FedState, Regions, Contragent, Otrasl, GroupPost, Proizv, Dogovor, TU, Infr, VersProgn, StPotr, StGaz

'''# Округа и регионы
def fo_region_query(base_query, tab_region_d314, tab_fo_d314):
    """
    Генерирует запрос для "Прогнозный спрос по отраслям" на основе указанного столбца.

    Аргументы:
        base_query (Запрос): Базовый объект запроса SQLAlchemy.
        progn_spros_data (База): Объект таблицы SQLAlchemy, содержащий данные ресурса.

    Возвращается:
        QUERY: объект запроса SQLAlchemy, который группирует и суммирует поле "СУММА" по полям:

                отфильтрованными по `ГОДАМ`.
    """
    return (base_query.with_entities(
        tab_region_d314.name.label('region')
    ).join(
    tab_fo_d314, tab_fo_d314.id == tab_region_d314.tab_fo_d314_ids
    ).group_by(
        tab_region_d314.name,
    ).order_by(
        tab_region_d314.name
    )
    )'''
'''# Группы округов и регионов
def fo_group_query(base_query, tab_group_region_d314): #, tab_fo_d314):
    """
    Генерирует запрос для "Прогнозный спрос по отраслям" на основе указанного столбца.

    Аргументы:
        base_query (Запрос): Базовый объект запроса SQLAlchemy.
        progn_spros_data (База): Объект таблицы SQLAlchemy, содержащий данные ресурса.

    """
    return (base_query.with_entities(
        tab_group_region_d314.name.label('group')
    ).join(
    tab_fo_d314, tab_fo_d314.id == tab_group_region_d314.tab_fo_d314_ids
    ).group_by(
        tab_group_region_d314.name,
    ).order_by(
        tab_group_region_d314.name
    )
    )'''

def region_fo_query(base_query, tab_region_d314, tab_fo_d314):
    """
    Генерирует запрос для "Прогнозный спрос по отраслям" на основе указанного столбца.

    Аргументы:
        base_query (Запрос): Базовый объект запроса SQLAlchemy.
        progn_spros_data (База): Объект таблицы SQLAlchemy, содержащий данные ресурса.

    Возвращается:
        QUERY: объект запроса SQLAlchemy, который группирует и суммирует поле "СУММА" по полям:

                отфильтрованными по `ГОДАМ`.
    """
    return (base_query.with_entities(
        tab_fo_d314.name.label('fo')
    ).join(
    tab_region_d314, tab_fo_d314.id == tab_region_d314.tab_fo_d314_ids
    ).group_by(
        tab_fo_d314.name,
    ).order_by(
        tab_fo_d314.name
    )
    )

# Общий запрос для всех данных
def all_data_query(base_query, tab_progn_spr_gaz_d314, tab_contragent_d314, tab_otrasl_economy_d314, tab_fo_d314,
                     tab_region_d314, tab_group_post_d314, tab_status_potreb_d314, tab_start_gaz_d314, tab_pg_visual_d314,
                     tab_dogovor_visual_d314, tab_tu_visual_d314, yearfrom, yearto):
    """
    Генерирует запрос для "Крупные инвестиционные проекты" на основе указанного столбца.

    Аргументы:
        base_query (Запрос): Базовый объект запроса SQLAlchemy.
        progn_spros_data (База): Объект таблицы SQLAlchemy, содержащий данные ресурса.

    Возвращается:
        QUERY: объект запроса SQLAlchemy, который группирует и суммирует поле "СУММА" по полям:

                отфильтрованными по `ГОДАМ`.
    """
    return (base_query.with_entities(
        tab_progn_spr_gaz_d314.year,
        tab_contragent_d314.name.label('contragent'),
        tab_fo_d314.name.label('fo'),
        tab_region_d314.name.label('region'),
        tab_group_post_d314.name.label('grpost'),
        tab_otrasl_economy_d314.name.label('otrasl'),
        tab_status_potreb_d314.name.label('stpotr'),
        tab_start_gaz_d314.name.label('stgaz'),
        tab_pg_visual_d314.name.label('pg'),
        tab_dogovor_visual_d314.name.label('dogovor'),
        tab_tu_visual_d314.name.label('tu'),
        func.sum(tab_progn_spr_gaz_d314.summ).label('total_indicator')
    ).join(
    tab_contragent_d314, tab_contragent_d314.id == tab_progn_spr_gaz_d314.tab_contragent_d314_ids
    ).join(
    tab_otrasl_economy_d314, tab_otrasl_economy_d314.id == tab_progn_spr_gaz_d314.tab_otrasl_economy_d314_ids
    ).join(
    tab_fo_d314, tab_fo_d314.id == tab_progn_spr_gaz_d314.tab_fo_d314_ids
    ).join(
    tab_region_d314, tab_region_d314.id == tab_progn_spr_gaz_d314.tab_region_d314_ids
    ).join(
    tab_group_post_d314, tab_group_post_d314.id == tab_progn_spr_gaz_d314.tab_group_post_d314_ids
    ).join(
    tab_status_potreb_d314, tab_status_potreb_d314.id == tab_progn_spr_gaz_d314.tab_status_potreb_d314_ids
    ).join(
    tab_start_gaz_d314, tab_start_gaz_d314.id == tab_progn_spr_gaz_d314.tab_start_gaz_d314_ids
    ).join(
    tab_pg_visual_d314, tab_pg_visual_d314.id == tab_progn_spr_gaz_d314.tab_pg_visual_d314_ids
    ).join(
    tab_dogovor_visual_d314, tab_dogovor_visual_d314.id == tab_progn_spr_gaz_d314.tab_dogovor_visual_d314_ids
    ).join(
    tab_tu_visual_d314, tab_tu_visual_d314.id == tab_progn_spr_gaz_d314.tab_tu_visual_d314_ids
    ).filter(tab_progn_spr_gaz_d314.year.between(yearfrom, yearto)
    ).group_by(
        tab_otrasl_economy_d314.name,
        tab_progn_spr_gaz_d314.year,
        tab_fo_d314.name,
        tab_region_d314.name,
        tab_group_post_d314.name,
        tab_status_potreb_d314.name,
        tab_start_gaz_d314.name,
        tab_pg_visual_d314.name,
        tab_dogovor_visual_d314.name,
        tab_tu_visual_d314.name
    ).order_by(
        tab_otrasl_economy_d314.name,
        tab_progn_spr_gaz_d314.year,
        tab_fo_d314.name,
        tab_region_d314.name,
        tab_group_post_d314.name,
        tab_status_potreb_d314.name,
        tab_start_gaz_d314.name,
        tab_pg_visual_d314.name,
        tab_dogovor_visual_d314.name,
        tab_tu_visual_d314.name
    )
    )

# Прогнозный спрос по отраслям
def otrasl_query(base_query, tab_progn_spr_gaz_d314, tab_otrasl_economy_d314, yearfrom, yearto, date):
    """
    Генерирует запрос для "Прогнозный спрос по отраслям" на основе указанного столбца.
    Аргументы:
        base_query (Запрос): Базовый объект запроса SQLAlchemy.
        progn_spros_data (База): Объект таблицы SQLAlchemy, содержащий данные ресурса.
    Возвращается:
        QUERY: объект запроса SQLAlchemy, который группирует и суммирует поле "СУММА" по полям:
                отфильтрованными по `ГОДАМ`.
    """
    return (base_query.with_entities(
        tab_progn_spr_gaz_d314.year.label('year'),
        tab_otrasl_economy_d314.name.label('otrasl'),
        func.sum(tab_progn_spr_gaz_d314.summ).label('total_indicator')
    ).join(
    tab_otrasl_economy_d314, tab_otrasl_economy_d314.id == tab_progn_spr_gaz_d314.tab_otrasl_economy_d314_ids
    ).filter(tab_progn_spr_gaz_d314.year.in_([yearfrom, yearto])
    ).filter(tab_progn_spr_gaz_d314.date == date
    ).group_by(
        tab_otrasl_economy_d314.name,
        tab_progn_spr_gaz_d314.year
    ).order_by(
        tab_otrasl_economy_d314.name,
        tab_progn_spr_gaz_d314.year,
        (func.sum(tab_progn_spr_gaz_d314.summ).desc())
    )
    )
def query_prirost(base_query, tab_progn_spr_gaz_d314, tab_otrasl_economy_d314, otrasl_name, year_par, date):
    return (base_query.with_entities(
                func.sum(tab_progn_spr_gaz_d314.summ).label('sum_par')
    ).join(
    tab_otrasl_economy_d314, tab_otrasl_economy_d314.id == tab_progn_spr_gaz_d314.tab_otrasl_economy_d314_ids
    ).filter(tab_progn_spr_gaz_d314.year == year_par
    ).filter(tab_progn_spr_gaz_d314.date == date
    ).filter(tab_otrasl_economy_d314.name == otrasl_name
    )
    )
# Прогнозный спрос РФ топ-5 потребителей
def top_potr_query(base_query, tab_progn_spr_gaz_d314, tab_contragent_d314, tab_ver_real_pr_d314, yearfrom, yearto, date):
    """
    Генерирует запрос для "Крупные инвестиционные проекты" на основе указанного столбца.
    Аргументы:
        base_query (Запрос): Базовый объект запроса SQLAlchemy.
        progn_spros_data (База): Объект таблицы SQLAlchemy, содержащий данные ресурса.
    Возвращается:
        QUERY: объект запроса SQLAlchemy, который группирует и суммирует поле "СУММА" по полям:
                отфильтрованными по `ГОДАМ`.
    """
    return (base_query.with_entities(
        tab_progn_spr_gaz_d314.year.label('year'),
        tab_ver_real_pr_d314.name.label('vers'),
        tab_contragent_d314.name.label('contragent'),
        func.sum(tab_progn_spr_gaz_d314.summ).label('total_indicator')
    ).join(
    tab_contragent_d314, tab_contragent_d314.id == tab_progn_spr_gaz_d314.tab_contragent_d314_ids
    ).join(
    tab_ver_real_pr_d314, tab_ver_real_pr_d314.id == tab_progn_spr_gaz_d314.tab_ver_real_pr_d314_ids
    ).filter(tab_progn_spr_gaz_d314.year.between(yearfrom, yearto)
    ).filter(tab_progn_spr_gaz_d314.date == date
    ).group_by(
        tab_progn_spr_gaz_d314.year,
        tab_ver_real_pr_d314.name,
        tab_contragent_d314.name
    ).order_by(
        tab_progn_spr_gaz_d314.year,
        tab_ver_real_pr_d314.name,
        (func.sum(tab_progn_spr_gaz_d314.summ).desc()),
        tab_contragent_d314.name
    )
    )

# =======================  ЗАПРОСЫ ДЛЯ SANKEY ====================
def sankey_query(base_query, tab_progn_spr_gaz_d314, tab_group_post_d314, tab_proizvoditel_d314, yearfrom, date):
    """
    Генерирует запрос для "Sankey" на основе указанного столбца.
    Аргументы:
        base_query (Запрос): Базовый объект запроса SQLAlchemy.
        progn_spros_data (База): Объект таблицы SQLAlchemy, содержащий данные ресурса.
    Возвращается:
        QUERY: объект запроса SQLAlchemy, который группирует и суммирует поле "СУММА" по полям:
                отфильтрованными по `году и дате загрузки`.
    """
    return (base_query.with_entities(
        tab_proizvoditel_d314.name.label('proizv'),
        tab_group_post_d314.name.label('grpost'),
        func.sum(tab_progn_spr_gaz_d314.summ).label('summ')
    ).join(
    tab_proizvoditel_d314, tab_proizvoditel_d314.id == tab_progn_spr_gaz_d314.tab_proizvoditel_d314_ids
    ).join(
    tab_group_post_d314, tab_group_post_d314.id == tab_progn_spr_gaz_d314.tab_group_post_d314_ids
    ).filter(tab_progn_spr_gaz_d314.year == yearfrom
    ).filter(tab_progn_spr_gaz_d314.date == date
    ).group_by(
        tab_proizvoditel_d314.name,
        tab_group_post_d314.name,
    ).order_by(
        tab_proizvoditel_d314.name,
        tab_group_post_d314.name,
    )
    )
def sankey_query2(base_query, tab_progn_spr_gaz_d314, tab_otrasl_economy_d314, tab_group_post_d314, yearfrom, date):
    """
    Генерирует запрос для "Sankey" на основе указанного столбца.

    Аргументы:
        base_query (Запрос): Базовый объект запроса SQLAlchemy.
        progn_spros_data (База): Объект таблицы SQLAlchemy, содержащий данные ресурса.

    Возвращается:
        QUERY: объект запроса SQLAlchemy, который группирует и суммирует поле "СУММА" по полям:

                отфильтрованными по `году и дате загрузки`.
    """
    return (base_query.with_entities(
        tab_group_post_d314.name.label('grpost'),
        tab_otrasl_economy_d314.name.label('otrasl'),
        func.sum(tab_progn_spr_gaz_d314.summ).label('summ')
    ).join(
    tab_otrasl_economy_d314, tab_otrasl_economy_d314.id == tab_progn_spr_gaz_d314.tab_otrasl_economy_d314_ids
    ).join(
    tab_group_post_d314, tab_group_post_d314.id == tab_progn_spr_gaz_d314.tab_group_post_d314_ids
    ).filter(tab_progn_spr_gaz_d314.year == yearfrom
    ).filter(tab_progn_spr_gaz_d314.date == date
    ).group_by(
        tab_otrasl_economy_d314.name,
        tab_group_post_d314.name,
    ).order_by(
        tab_otrasl_economy_d314.name,
        tab_group_post_d314.name,
    )
    )

def sankey_query3(base_query, tab_progn_spr_gaz_d314, tab_proizvoditel_d314, yearfrom, date):
    """
    Генерирует запрос для "Крупные инвестиционные проекты" на основе указанного столбца.
    Аргументы:
        base_query (Запрос): Базовый объект запроса SQLAlchemy.
        progn_spros_data (База): Объект таблицы SQLAlchemy, содержащий данные ресурса.
    Возвращается:
        QUERY: объект запроса SQLAlchemy, который группирует и суммирует поле "СУММА" по полям:
                отфильтрованными по `году и дате загрузки`.
    """
    return (base_query.with_entities(
        tab_proizvoditel_d314.name.label('proizv'),
        func.sum(tab_progn_spr_gaz_d314.summ).label('summ')
    ).join(
    tab_proizvoditel_d314, tab_proizvoditel_d314.id == tab_progn_spr_gaz_d314.tab_proizvoditel_d314_ids
    ).filter(tab_progn_spr_gaz_d314.year == yearfrom
    ).filter(tab_progn_spr_gaz_d314.date == date
    ).group_by(
        tab_proizvoditel_d314.name
    ).order_by(
        tab_proizvoditel_d314.name,
    )
    )
def sankey_query4(base_query, tab_progn_spr_gaz_d314, tab_group_post_d314, yearfrom, date):
    """
    Генерирует запрос для "Крупные инвестиционные проекты" на основе указанного столбца.
    Аргументы:
        base_query (Запрос): Базовый объект запроса SQLAlchemy.
        progn_spros_data (База): Объект таблицы SQLAlchemy, содержащий данные ресурса.
    Возвращается:
        QUERY: объект запроса SQLAlchemy, который группирует и суммирует поле "СУММА" по полям:
                отфильтрованными по `году и дате загрузки`.
    """
    return (base_query.with_entities(
        tab_group_post_d314.name.label('grpost'),
        func.sum(tab_progn_spr_gaz_d314.summ).label('summ')
    ).join(
    tab_group_post_d314, tab_group_post_d314.id == tab_progn_spr_gaz_d314.tab_group_post_d314_ids
    ).filter(tab_progn_spr_gaz_d314.year == yearfrom
    ).filter(tab_progn_spr_gaz_d314.date == date
    ).group_by(
        tab_group_post_d314.name
    ).order_by(
        tab_group_post_d314.name
    )
    )
def sankey_query5(base_query, tab_progn_spr_gaz_d314, tab_otrasl_economy_d314, yearfrom, date):
    """
    Генерирует запрос для "Крупные инвестиционные проекты" на основе указанного столбца.
    Аргументы:
        base_query (Запрос): Базовый объект запроса SQLAlchemy.
        progn_spros_data (База): Объект таблицы SQLAlchemy, содержащий данные ресурса.
    Возвращается:
        QUERY: объект запроса SQLAlchemy, который группирует и суммирует поле "СУММА" по полям:
                отфильтрованными по `году и дате загрузки`.
    """
    return (base_query.with_entities(
        tab_otrasl_economy_d314.name.label('otrasl'),
        func.sum(tab_progn_spr_gaz_d314.summ).label('summ')
    ).join(
    tab_otrasl_economy_d314, tab_otrasl_economy_d314.id == tab_progn_spr_gaz_d314.tab_otrasl_economy_d314_ids
    ).filter(tab_progn_spr_gaz_d314.year == yearfrom
    ).filter(tab_progn_spr_gaz_d314.date == date
    ).group_by(
        tab_otrasl_economy_d314.name
    ).order_by(
        tab_otrasl_economy_d314.name
    )
    )

# Карта по отраслям
def fo_otrasl_query(base_query, tab_progn_spr_gaz_d314, tab_fo_d314, tab_otrasl_economy_d314, yearfrom, yearto, date):
    """
    Генерирует запрос для "Карта по отраслям" на основе указанного столбца.
    Аргументы:
        base_query (Запрос): Базовый объект запроса SQLAlchemy.
        progn_spros_data (База): Объект таблицы SQLAlchemy, содержащий данные ресурса.
    """
    return (base_query.with_entities(
        tab_fo_d314.name.label('fo'),
        tab_otrasl_economy_d314.name.label('otrasl'),
        func.sum(tab_progn_spr_gaz_d314.summ).label('total_indicator')
    ).join(
    tab_otrasl_economy_d314, tab_otrasl_economy_d314.id == tab_progn_spr_gaz_d314.tab_otrasl_economy_d314_ids
    ).join(
    tab_fo_d314, tab_fo_d314.id == tab_progn_spr_gaz_d314.tab_fo_d314_ids
    ).filter(tab_progn_spr_gaz_d314.year == yearto
    ).filter(tab_progn_spr_gaz_d314.date == date
    ).group_by(
        tab_fo_d314.name,
        tab_otrasl_economy_d314.name
    ).order_by(
        tab_fo_d314.name,
        (func.sum(tab_progn_spr_gaz_d314.summ).desc()),
        tab_otrasl_economy_d314.name
    )
    )
# Карта по потребителям
def fo_potr_query(base_query, tab_progn_spr_gaz_d314, tab_fo_d314, tab_contragent_d314, yearfrom, yearto, date):
    """
    Генерирует запрос для "Карта" на основе указанного столбца.
    Аргументы:
        base_query (Запрос): Базовый объект запроса SQLAlchemy.
        progn_spros_data (База): Объект таблицы SQLAlchemy, содержащий данные ресурса.

    """
    return (base_query.with_entities(
        tab_fo_d314.name.label('fo'),
        tab_contragent_d314.name.label('potr'),
        func.sum(tab_progn_spr_gaz_d314.summ).label('total_indicator')
    ).join(
    tab_contragent_d314, tab_contragent_d314.id == tab_progn_spr_gaz_d314.tab_contragent_d314_ids
    ).join(
    tab_fo_d314, tab_fo_d314.id == tab_progn_spr_gaz_d314.tab_fo_d314_ids
    ).filter(tab_progn_spr_gaz_d314.year == yearto
    ).filter(tab_progn_spr_gaz_d314.date == date
    ).filter(and_(
        tab_progn_spr_gaz_d314.tab_contragent_d314_ids != 44502,
        tab_progn_spr_gaz_d314.tab_contragent_d314_ids != 44484)
    ).group_by(
        tab_fo_d314.name,
        tab_contragent_d314.name
    ).order_by(
        tab_fo_d314.name,
        (func.sum(tab_progn_spr_gaz_d314.summ).desc()),
        tab_contragent_d314.name
    )
    )

def big_invest_query(base_query, tab_progn_spr_gaz_d314, tab_otrasl_economy_d314, tab_fo_d314,
                     tab_region_d314, tab_group_post_d314, tab_status_potreb_d314, tab_start_gaz_d314, tab_infr_d314,
                     tab_dogovor_visual_d314, tab_tu_visual_d314, yearfrom, yearto):
    """
    Генерирует запрос для "Крупные инвестиционные проекты" на основе указанного столбца.

    Аргументы:
        base_query (Запрос): Базовый объект запроса SQLAlchemy.
        progn_spros_data (База): Объект таблицы SQLAlchemy, содержащий данные ресурса.

    Возвращается:
        QUERY: объект запроса SQLAlchemy, который группирует и суммирует поле "СУММА" по полям:

                отфильтрованными по `ГОДАМ`.
    """
    return (base_query.with_entities(
        tab_otrasl_economy_d314.name.label('otrasl'),
        tab_fo_d314.name.label('fo'),
        tab_region_d314.name.label('region'),
        tab_group_post_d314.name.label('grpost'),
        tab_status_potreb_d314.name.label('stpotr'),
        tab_start_gaz_d314.name.label('stgaz'),
        tab_infr_d314.name.label('infr'),
        tab_dogovor_visual_d314.name.label('dogovor'),
        tab_tu_visual_d314.name.label('tu'),
        func.sum(tab_progn_spr_gaz_d314.summ).label('summ')
    ).join(
    tab_otrasl_economy_d314, tab_otrasl_economy_d314.id == tab_progn_spr_gaz_d314.tab_otrasl_economy_d314_ids
    ).join(
    tab_fo_d314, tab_fo_d314.id == tab_progn_spr_gaz_d314.tab_fo_d314_ids
    ).join(
    tab_region_d314, tab_region_d314.id == tab_progn_spr_gaz_d314.tab_region_d314_ids
    ).join(
    tab_group_post_d314, tab_group_post_d314.id == tab_progn_spr_gaz_d314.tab_group_post_d314_ids
    ).join(
    tab_status_potreb_d314, tab_status_potreb_d314.id == tab_progn_spr_gaz_d314.tab_status_potreb_d314_ids
    ).join(
    tab_start_gaz_d314, tab_start_gaz_d314.id == tab_progn_spr_gaz_d314.tab_start_gaz_d314_ids
    ).join(
    tab_infr_d314, tab_infr_d314.id == tab_progn_spr_gaz_d314.tab_infr_d314_ids
    ).join(
    tab_dogovor_visual_d314, tab_dogovor_visual_d314.id == tab_progn_spr_gaz_d314.tab_dogovor_visual_d314_ids
    ).join(
    tab_tu_visual_d314, tab_tu_visual_d314.id == tab_progn_spr_gaz_d314.tab_tu_visual_d314_ids
    ).filter(tab_progn_spr_gaz_d314.year.in_([yearfrom, yearto])
    ).group_by(
        tab_otrasl_economy_d314.name,
        tab_fo_d314.name,
        tab_region_d314.name,
        tab_group_post_d314.name,
        tab_status_potreb_d314.name,
        tab_start_gaz_d314.name,
        tab_infr_d314.name,
        tab_dogovor_visual_d314.name,
        tab_tu_visual_d314.name
    ).order_by(
        tab_otrasl_economy_d314.name,
        tab_fo_d314.name,
        tab_region_d314.name,
        tab_group_post_d314.name,
        tab_status_potreb_d314.name,
        tab_start_gaz_d314.name,
        tab_infr_d314.name,
        tab_dogovor_visual_d314.name,
        tab_tu_visual_d314.name
    )
    )

def query_prirost_table(base_query, tab_progn_spr_gaz_d314, tab_otrasl_economy_d314, tab_fo_d314,
                     tab_region_d314, tab_group_post_d314, tab_status_potreb_d314, tab_start_gaz_d314, tab_infr_d314,
                     tab_dogovor_visual_d314, tab_tu_visual_d314, otr_name, fo_name, reg_name, grp_name, stp_name, stg_name,
                     infr_name, dog_name, tu_name, year_par):
    return (base_query.with_entities(
             func.sum(tab_progn_spr_gaz_d314.summ).label('sum_par')
    ).join(
    tab_otrasl_economy_d314, tab_otrasl_economy_d314.id == tab_progn_spr_gaz_d314.tab_otrasl_economy_d314_ids
    ).join(
    tab_fo_d314, tab_fo_d314.id == tab_progn_spr_gaz_d314.tab_fo_d314_ids
    ).join(
    tab_region_d314, tab_region_d314.id == tab_progn_spr_gaz_d314.tab_region_d314_ids
    ).join(
    tab_group_post_d314, tab_group_post_d314.id == tab_progn_spr_gaz_d314.tab_group_post_d314_ids
    ).join(
    tab_status_potreb_d314, tab_status_potreb_d314.id == tab_progn_spr_gaz_d314.tab_status_potreb_d314_ids
    ).join(
    tab_start_gaz_d314, tab_start_gaz_d314.id == tab_progn_spr_gaz_d314.tab_start_gaz_d314_ids
    ).join(
    tab_infr_d314, tab_infr_d314.id == tab_progn_spr_gaz_d314.tab_infr_d314_ids
    ).join(
    tab_dogovor_visual_d314, tab_dogovor_visual_d314.id == tab_progn_spr_gaz_d314.tab_dogovor_visual_d314_ids
    ).join(
    tab_tu_visual_d314, tab_tu_visual_d314.id == tab_progn_spr_gaz_d314.tab_tu_visual_d314_ids
    ).filter(tab_progn_spr_gaz_d314.year == year_par
    ).filter(tab_otrasl_economy_d314.name == otr_name
    ).filter(tab_fo_d314.name == fo_name
    ).filter(tab_region_d314.name == reg_name
    ).filter(tab_group_post_d314.name == grp_name
    ).filter(tab_status_potreb_d314.name == stp_name
    ).filter(tab_start_gaz_d314.name == stg_name
    ).filter(tab_infr_d314.name == infr_name
    ).filter(tab_dogovor_visual_d314.name == dog_name
    ).filter(tab_tu_visual_d314.name == tu_name
    )
    )

def query_prirost_potr_table(base_query, tab_progn_spr_gaz_d314, tab_otrasl_economy_d314, tab_fo_d314,
                     tab_region_d314, tab_group_post_d314, tab_contragent_d314, tab_status_potreb_d314, tab_start_gaz_d314, tab_infr_d314,
                     tab_dogovor_visual_d314, tab_tu_visual_d314, otr_name, fo_name, reg_name, grp_name, contragent_name, stp_name, stg_name,
                     infr_name, dog_name, tu_name, year_par):

    return (base_query.with_entities(
             func.sum(tab_progn_spr_gaz_d314.summ).label('sum_par')
    ).join(
    tab_otrasl_economy_d314, tab_otrasl_economy_d314.id == tab_progn_spr_gaz_d314.tab_otrasl_economy_d314_ids
    ).join(
    tab_fo_d314, tab_fo_d314.id == tab_progn_spr_gaz_d314.tab_fo_d314_ids
    ).join(
    tab_region_d314, tab_region_d314.id == tab_progn_spr_gaz_d314.tab_region_d314_ids
    ).join(
    tab_group_post_d314, tab_group_post_d314.id == tab_progn_spr_gaz_d314.tab_group_post_d314_ids
    ).join(
    tab_contragent_d314, tab_contragent_d314.id == tab_progn_spr_gaz_d314.tab_contragent_d314_ids
    ).join(
    tab_status_potreb_d314, tab_status_potreb_d314.id == tab_progn_spr_gaz_d314.tab_status_potreb_d314_ids
    ).join(
    tab_start_gaz_d314, tab_start_gaz_d314.id == tab_progn_spr_gaz_d314.tab_start_gaz_d314_ids
    ).join(
    tab_infr_d314, tab_infr_d314.id == tab_progn_spr_gaz_d314.tab_infr_d314_ids
    ).join(
    tab_dogovor_visual_d314, tab_dogovor_visual_d314.id == tab_progn_spr_gaz_d314.tab_dogovor_visual_d314_ids
    ).join(
    tab_tu_visual_d314, tab_tu_visual_d314.id == tab_progn_spr_gaz_d314.tab_tu_visual_d314_ids
    ).filter(tab_progn_spr_gaz_d314.year == year_par
    ).filter(tab_otrasl_economy_d314.name == otr_name
    ).filter(tab_fo_d314.name == fo_name
    ).filter(tab_region_d314.name == reg_name
    ).filter(tab_group_post_d314.name == grp_name
    ).filter(tab_contragent_d314.name == contragent_name
    ).filter(tab_status_potreb_d314.name == stp_name
    ).filter(tab_start_gaz_d314.name == stg_name
    ).filter(tab_infr_d314.name == infr_name
    ).filter(tab_dogovor_visual_d314.name == dog_name
    ).filter(tab_tu_visual_d314.name == tu_name
    )
    )

def big_invest_query_potr(base_query, tab_prirost_d314, tab_otrasl_economy_d314, tab_fo_d314,
                     tab_region_d314, tab_group_post_d314, tab_status_potreb_d314, tab_start_gaz_d314, tab_infr_d314,
                     tab_dogovor_visual_d314, tab_tu_visual_d314, yearfrom, yearto, tab_contragent_d314, date):
    """
    Генерирует запрос для "Крупные инвестиционные проекты" на основе указанного столбца.
    Аргументы:
        base_query (Запрос): Базовый объект запроса SQLAlchemy.
        progn_spros_data (База): Объект таблицы SQLAlchemy, содержащий данные ресурса.
    Возвращается:
        QUERY: объект запроса SQLAlchemy, который группирует и суммирует поле "СУММА" по полям:
                отфильтрованными по `ГОДАМ`.
    """
    return (base_query.with_entities(
        tab_otrasl_economy_d314.name.label('otrasl'),
        tab_fo_d314.name.label('fo'),
        tab_region_d314.name.label('region'),
        tab_group_post_d314.name.label('grpost'),
        tab_status_potreb_d314.name.label('stpotr'),
        tab_start_gaz_d314.name.label('stgaz'),
        tab_infr_d314.name.label('infr'),
        tab_dogovor_visual_d314.name.label('dogovor'),
        tab_tu_visual_d314.name.label('tu'),
        tab_contragent_d314.name.label('contragent'),
        func.sum(tab_prirost_d314.summ).label('prirost')
    ).join(
    tab_otrasl_economy_d314, tab_otrasl_economy_d314.id == tab_prirost_d314.tab_otrasl_economy_d314_ids
    ).join(
    tab_fo_d314, tab_fo_d314.id == tab_prirost_d314.tab_fo_d314_ids
    ).join(
    tab_region_d314, tab_region_d314.id == tab_prirost_d314.tab_region_d314_ids
    ).join(
    tab_group_post_d314, tab_group_post_d314.id == tab_prirost_d314.tab_group_post_d314_ids
    ).join(
    tab_status_potreb_d314, tab_status_potreb_d314.id == tab_prirost_d314.tab_status_potreb_d314_ids
    ).join(
    tab_start_gaz_d314, tab_start_gaz_d314.id == tab_prirost_d314.tab_start_gaz_d314_ids
    ).join(
    tab_infr_d314, tab_infr_d314.id == tab_prirost_d314.tab_infr_d314_ids
    ).join(
    tab_dogovor_visual_d314, tab_dogovor_visual_d314.id == tab_prirost_d314.tab_dogovor_visual_d314_ids
    ).join(
    tab_tu_visual_d314, tab_tu_visual_d314.id == tab_prirost_d314.tab_tu_visual_d314_ids
    ).join(
        tab_contragent_d314, tab_contragent_d314.id == tab_prirost_d314.tab_contragent_d314_ids
    ).filter(tab_prirost_d314.yearfrom == yearfrom
    ).filter(tab_prirost_d314.yearto == yearto
    ).filter(tab_prirost_d314.date == date
    ).filter(
        and_(
            tab_prirost_d314.tab_contragent_d314_ids != 44502,
            tab_prirost_d314.tab_contragent_d314_ids != 44484
        )
    ).group_by(
        tab_otrasl_economy_d314.name,
        tab_fo_d314.name,
        tab_region_d314.name,
        tab_group_post_d314.name,
        tab_status_potreb_d314.name,
        tab_start_gaz_d314.name,
        tab_infr_d314.name,
        tab_dogovor_visual_d314.name,
        tab_tu_visual_d314.name,
        tab_contragent_d314.name
    ).order_by(
        (func.sum(tab_prirost_d314.summ).desc()),
        tab_otrasl_economy_d314.name,
        tab_fo_d314.name,
        tab_region_d314.name,
        tab_group_post_d314.name,
        tab_contragent_d314.name,
        tab_status_potreb_d314.name,
        tab_start_gaz_d314.name,
        tab_infr_d314.name,
        tab_dogovor_visual_d314.name,
        tab_tu_visual_d314.name,
        tab_contragent_d314.name
    )
    )

def mapping_otrasl_query(base_query, tab_otrasl_economy_d314):
    """
        запрос мэппинга по отраслям из таблицы tab_otrasl_economy_d314
    """
    return (base_query.with_entities(
        tab_otrasl_economy_d314.name.label('name'),
        tab_otrasl_economy_d314.id.label('id')
    ).group_by(
        tab_otrasl_economy_d314.name,
        tab_otrasl_economy_d314.id
    ).order_by(
        tab_otrasl_economy_d314.name
    ).filter(tab_otrasl_economy_d314.id.not_in([19])
    )
    )
def year_query(base_query, tab_progn_spr_gaz_d314):
    return (base_query.with_entities(
        tab_progn_spr_gaz_d314.year.label('year')
    ).group_by(
        tab_progn_spr_gaz_d314.year
    ).order_by(
        tab_progn_spr_gaz_d314.year
    )
    )

def date_query(base_query, tab_progn_spr_gaz_d314):
    return (base_query.with_entities(
        tab_progn_spr_gaz_d314.date.label('date')
    ).group_by(
        tab_progn_spr_gaz_d314.date
    ).order_by(
        tab_progn_spr_gaz_d314.date
    )
    )

def yearto_query(base_query, tab_progn_spr_gaz_d314):
    """
        запрос мэппинга по отраслям из таблицы tab_otrasl_economy_d314
    """
    return (base_query.with_entities(
        func.max(tab_progn_spr_gaz_d314.year).label('year')
    ).group_by(
        tab_progn_spr_gaz_d314.year
    ).filter(tab_progn_spr_gaz_d314.summ != 0
    )
    )

# Запрос для мэппингов
def mapping_query(base_query, tab):
    return (base_query.with_entities(
        tab.name.label('name'),
        tab.id.label('id')
    ).group_by(
        tab.name,
        tab.id
    ).order_by(
        tab.name
    ).filter(tab.id.not_in([19])
    )
    )

def mapping_vers(base_query, tab):
    return (base_query.with_entities(
        tab.name.label('short_name'),
        tab.id.label('id')
    ).group_by(
        tab.name,
        tab.id
    ).order_by(
        tab.name
    ).filter(tab.id.not_in([19])
    )
    )


# Запрос для выгрузки плоской даты
def create_simple_query(db: scoped_session, base_table, columns: Dict[str, ColumnDescriptor], join_cols_dict: Dict,
                        distinct: Optional[bool] = False, isouter: Optional[bool] = False, select_from = None) -> Query:
    entities_list = []
    joined_tables_list = []
    joined_expressions_list: List[Tuple] = []
    for label, column_d in columns.items():
        if column_d.case_desc is not None:
            column = column_d.case_desc.sql_case
            if column_d.case_desc.join is not None:
                joined_expressions_list.append(column_d.case_desc.join)
        else:
            column = column_d.db_column
            if column.table != base_table.__table__ and column.table.name in join_cols_dict:
                joined_tables_list.append(column.table)
        if column_d.aggr_func is not None:
                column = column_d.aggr_func(column)
        
        if column_d.mapping is not None:
            when_clauses = []
            for key, value in column_d.mapping.items():
                when_clauses.append((cast(column, Text) == key, literal(value)))
            column = case(*when_clauses, else_=cast(column, Text))
        entities_list.append(column.label(label))
        
    query = db.query(base_table)
    # print('\n'*10, str(query.with_labels().statement))
    # print(select_from)
    if select_from is not None:
        query = query.select_from(select_from)
    else:
        query = query.select_from(base_table)
    # print('\n*'*10, str(query.with_labels().statement))
    query = query.with_entities(*entities_list)
    if distinct:
        query = query.distinct()
    # print('\n'*10, str(query.with_labels().statement))
    # print('\n*'*10, str(query.with_labels().statement))
    for joined_table in joined_tables_list:
        query = query.join(joined_table, onclause=join_cols_dict[joined_table.name]==joined_table.c.id, isouter=isouter)
    for table, expr in joined_expressions_list:
        query = query.join(table, onclause=expr, isouter=isouter)
    return query


###### Сравнительные таблицы

### Запросы для выгрузки данных Таблицы 1 (сравнительной)
def modify_optional_column(cte: CTE, id: str, col_desc: ColumnDescriptor):
    col_desc.case_desc=CaseDescriptor(sql_case = case(
                                (func.min(getattr(cte.c, f'{id}_count', 0)) > 1,
                                func.concat(
                                    'изменение значения с ', #! склонения
                                    func.min(getattr(cte.c, f'{id}_date1')),
                                    ' на ',
                                    func.min(getattr(cte.c, f'{id}_date2')))
                                )
                                ,
                                else_=cast(func.min(func.coalesce(getattr(cte.c, f'{id}_date1'), getattr(cte.c, f'{id}_date2'))), Text)))
    col_desc.db_column=None

    return col_desc
                   

def get_version_info_cte(db: scoped_session, date1_cte: CTE, date2_cte: CTE, optional_cols: Dict[str, ColumnDescriptor], identity_cols: Optional[List[str]] = None, cte_name: str = 'version_info') -> CTE:
    identity_cols = identity_cols or []
    optional_cols_versions = []

    for id, desc in optional_cols.items():
        if desc.mapping is not None:
            when_clauses_1 = []
            when_clauses_2 = []
            for key, value in desc.mapping.items():
                when_clauses_1.append((cast(getattr(date1_cte.c, id), Text) == key, literal(value)))
                when_clauses_2.append((cast(getattr(date2_cte.c, id), Text) == key, literal(value)))
            column_1 = case(*when_clauses_1, else_=cast(getattr(date1_cte.c, id), Text))
            column_2 = case(*when_clauses_2, else_=cast(getattr(date2_cte.c, id), Text))
        else:
            column_1 = getattr(date1_cte.c, id)
            column_2 = getattr(date2_cte.c, id)
        optional_cols_versions.append(column_1.label(f'{id}_date1'))
        optional_cols_versions.append(column_2.label(f'{id}_date2'))
        optional_cols_versions.append(case(
                                        (
                                            and_(
                                                getattr(date1_cte.c, id).isnot(None),
                                                getattr(date2_cte.c, id).isnot(None),
                                                getattr(date1_cte.c, id) != getattr(date2_cte.c, id)
                                            ),
                                            2
                                        )
                                    ,
                                    else_=1
                                ).label(f'{id}_count'))

    join_conditions = [
        date1_cte.c.fo == date2_cte.c.fo,
        date1_cte.c.region == date2_cte.c.region,
        date1_cte.c.potr == date2_cte.c.potr,
    ]
    identity_selects = []
    for id in identity_cols:
        join_conditions.append(getattr(date1_cte.c, id).is_not_distinct_from(getattr(date2_cte.c, id)))
        identity_selects.append(func.coalesce(getattr(date1_cte.c, id), getattr(date2_cte.c, id)).label(id))

    version_info = db.query().with_entities(
        func.coalesce(date1_cte.c.fo, date2_cte.c.fo).label('fo'),
        func.coalesce(date1_cte.c.region, date2_cte.c.region).label('region'),
        func.coalesce(date1_cte.c.potr, date2_cte.c.potr).label('potr'),
        and_(date1_cte.c.potr.is_(None), date2_cte.c.potr.isnot(None)).label('potr_is_new'),
        *identity_selects,
        *optional_cols_versions,
    ).select_from(
        date1_cte.outerjoin(
            date2_cte,
            and_(*join_conditions),
            full=True
        )
    )
    version_info = version_info.cte(cte_name)

    return version_info

def check_optional_cols(db: scoped_session, base_query: Query, optional_cols: Dict[str, ColumnDescriptor], pattern: Optional[str] = None) -> List[str]:
    if hasattr(base_query, 'statement'):
        compiled = base_query.statement.compile(
            dialect=db.bind.dialect,
            compile_kwargs={"literal_binds": True}
        )
    elif hasattr(base_query, 'compile'):
        compiled = base_query.compile(
            dialect=db.bind.dialect,
            compile_kwargs={"literal_binds": True}
        )
    else:
        compiled = base_query

    sql_string = str(compiled)

    pattern_matcher = lambda col : f'subquery.{col}::text'
    if pattern is not None:
        pattern_matcher = lambda col : f'CASE WHEN subquery.{col}::text ~ \'{pattern}\' THEN 1 END'

    checks = [f"COUNT({pattern_matcher(col)}) > 0 AS {col}_has_data" for col in optional_cols.keys()]
    
    query = f"""
        WITH subquery AS (
            {sql_string}
        )
        SELECT 
            {', '.join(checks)}
        FROM subquery
    """
    result = db.execute(text(query)).fetchone()
    # print('result', result)
    return [col for idx, col in enumerate(optional_cols.keys()) if result[idx]]


######### Пояснительная записка
def reverse_mapper_list(mapper: Dict) -> Dict:
    reversed_dict = {}
    for key, value in mapper.items():
        if value not in reversed_dict:
            reversed_dict[value] = []
        reversed_dict[value].append(key)
    return reversed_dict

def _get_map_table_subq(column: Column, mapper: Dict, mapped_name: str = 'mapped_name'):
    unions : List[Tuple[bool, List[Column]]] = [] # фильтр/юнион
    for key, value in mapper.items():
        unions.append((column.in_(ensure_list(value)), key))
    return unions

def ensure_list(items) -> List[Any]:
    if isinstance(items, str):
        items_list = [items]
    elif isinstance(items, Iterable) and not isinstance(items, str):
        items_list = list(items)
    else:
        items_list = [items]
    return items_list
def get_note_query(db: scoped_session, start_year: int, end_year: int, fos: List[str], regions: List[str], industry: str, date) -> Tuple[Query, Query]:
    VERSION_MAPPER = reverse_mapper_list(copy(version_leveled_mappings['ver_real_level1']))
    
    filters_unions = _get_map_table_subq(VersProgn.full_name, VERSION_MAPPER)
    
    # first_filter, first_label = filters_unions[0]
    version_mapping_subq = db.query(
        VersProgn.id.label('id'),
        VersProgn.full_name.label('full_name'), 
        VersProgn.full_name.label('mapped_name')
    ).filter(VersProgn.full_name.not_in(list(itertools.chain.from_iterable(VERSION_MAPPER.values()))))
    
    for filter_cond, label_value in filters_unions:
        union_part = db.query(
                VersProgn.id.label('id'),
                VersProgn.full_name.label('full_name'),
                literal(label_value).label('mapped_name')
            ).filter(filter_cond)#.cte('union_part')
        version_mapping_subq = version_mapping_subq.union(union_part)
        
    # print('\n*'*10, db.execute(version_mapping_subq).mappings().all())
    # print('\n'*10, str(version_mapping_subq.with_labels().statement))
    version_mapping_subq = version_mapping_subq.subquery('version_mapping')
    cols = list(version_mapping_subq.c)
    id_col = cols[0]
    mapped_name_col = cols[2] 
    base_filters = [Otrasl.name == industry,
                    PSDATA.year.in_([start_year, end_year]),
                    PSDATA.date == date]
    if fos and len(fos) > 0:
        fos_list = ensure_list(fos)
        base_filters.append(FedState.name.in_(fos_list))
    if regions and len(regions) > 0:
        regions_list = ensure_list(regions)
        base_filters.append(Regions.name.in_(regions_list))
    # def get_year_filters() -> Tuple[Query]:
    #     start_subq = db.query(PSDATA).filter(*base_filters, PSDATA.year == start_year).subquery()
    #     end_subq = db.query(PSDATA).filter(*base_filters, PSDATA.year == end_year).subquery()
    main_query = db.query(
            Regions.name.label('region_name'),
            FedState.short_name.label('fo_name'),
            mapped_name_col.label('version_name'),
            func.sum(
                case(
                    (PSDATA.year == start_year, PSDATA.summ),
                    else_=None
                )
            ).label('summ_start'),

            func.sum(
                case(
                    (PSDATA.year == end_year, PSDATA.summ),
                    else_=None
                )
            ).label('summ_end'),
            # func.sum(PSDATA.summ).filter(PSDATA.year == start_year).label('summ_start'),
            # func.sum(PSDATA.summ).filter(PSDATA.year == end_year).label('summ_end')
        ).select_from(
            PSDATA
        ).join(
            Regions, Regions.id == PSDATA.tab_region_d314_ids
        ).join(
            FedState, FedState.id == Regions.tab_fo_d314_ids
        ).join(
            version_mapping_subq, id_col == PSDATA.tab_ver_real_pr_d314_ids
        ).join(
            Otrasl, Otrasl.id == PSDATA.tab_otrasl_economy_d314_ids
        ).filter(
            *base_filters
        ).group_by(
            Regions.id,
            Regions.name,
            FedState.ord,
            FedState.short_name,
            mapped_name_col
        ).order_by(
            FedState.ord, 
            Regions.name
        )
    

    ranking_q = db.query(
        PSDATA.tab_region_d314_ids.label('region_id'),
        mapped_name_col.label('version_name'),
        id_col.label('version_id'),
        PSDATA.tab_contragent_d314_ids.label('contragent_id'),
        func.sum(PSDATA.summ).label('total_summ')
    ).select_from(
        PSDATA
    ).join(
        version_mapping_subq, id_col == PSDATA.tab_ver_real_pr_d314_ids
    ).join(
        Otrasl, Otrasl.id == PSDATA.tab_otrasl_economy_d314_ids
    ).join(
        Regions, Regions.id == PSDATA.tab_region_d314_ids
    ).join(
        FedState, FedState.id == Regions.tab_fo_d314_ids
    ).filter(
        PSDATA.year == end_year,  # ранжирование по end году 
        PSDATA.tab_contragent_d314_ids.not_in([44915, 44502, 46048, 44484]), # 44484 - 'действующие потребители'
        *base_filters
    ).group_by(
        PSDATA.tab_region_d314_ids,
        mapped_name_col, id_col,
        PSDATA.tab_contragent_d314_ids
    ).order_by(
        PSDATA.tab_region_d314_ids,
        mapped_name_col,
        desc(func.sum(PSDATA.summ))
    ).limit(3).subquery('rank')
    
    
    top3_query = db.query(
        FedState.short_name.label('fo_name'),
        Regions.name.label('region_name'),
        ranking_q.c.version_name,
        Contragent.name.label('contragent_name'),
        StGaz.name.label('start_gaz_year'),
        func.sum(PSDATA.summ).filter(PSDATA.year == start_year).label('summ_start'),
        func.sum(PSDATA.summ).filter(PSDATA.year == end_year).label('summ_end'),
        func.string_agg(func.distinct(TU.name), ', ').label('tu_list'),
        func.string_agg(func.distinct(PG.name), ', ').label('pg_list'),
        func.string_agg(func.distinct(Dogovor.name), ', ').label('dogovor_list')
    ).select_from(
        ranking_q
    ).join(
        Contragent, Contragent.id == ranking_q.c.contragent_id
    ).join(
        Regions, Regions.id == ranking_q.c.region_id
    ).join(
        FedState, FedState.id == Regions.tab_fo_d314_ids
    ).outerjoin(
        PSDATA, and_(
            PSDATA.tab_region_d314_ids == ranking_q.c.region_id,
            PSDATA.tab_ver_real_pr_d314_ids == ranking_q.c.version_id,
            PSDATA.tab_contragent_d314_ids == ranking_q.c.contragent_id,
            PSDATA.date == date,
            PSDATA.year.in_([start_year, end_year])
        )
    ).outerjoin(
        StGaz, StGaz.id == PSDATA.tab_start_gaz_d314_ids
    ).outerjoin(
        TU, TU.id == PSDATA.tab_tu_visual_d314_ids
    ).outerjoin(
        PG, PG.id == PSDATA.tab_pg_visual_d314_ids
    ).outerjoin(
        Dogovor, Dogovor.id == PSDATA.tab_dogovor_visual_d314_ids
    ).group_by(
        FedState.short_name,
        Regions.name,
        ranking_q.c.version_name,
        ranking_q.c.version_id,
        ranking_q.c.region_id,
        ranking_q.c.contragent_id,
        ranking_q.c.total_summ,
        Contragent.name,
        StGaz.name
    ).order_by(desc(ranking_q.c.total_summ))
    

    return (main_query, top3_query)


# # Запрос для справочника регионов по ФО
# def fo_regions_map_query(db, regions: List[str]):
#     query = db.query()
