from datetime import datetime
from enum import Enum
from typing import Any, Dict, Iterable, List, Optional, Tuple
from sqlalchemy import desc, func, and_, case, CTE, literal, or_, select, Case, text, Text
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.sql.expression import cast
from sqlalchemy.orm import aliased, scoped_session, Query
from progSpros_back.functions.file_upload_functions_ps import CaseDescriptor, ColumnDescriptor
from progSpros_back.model.mappings_ps import version_leveled_mappings
from progSpros_back.model.db_models_ps import PG, PSDATA, TU, Contragent, Dogovor, FedState, Otrasl, Regions, VersProgn
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
                   

def get_version_info_cte(db: scoped_session, date1_cte: CTE, date2_cte: CTE, optional_cols: Dict[str, ColumnDescriptor]) -> CTE:
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

    version_info = db.query().with_entities(
        func.coalesce(date1_cte.c.fo, date2_cte.c.fo).label('fo'),
        func.coalesce(date1_cte.c.region, date2_cte.c.region).label('region'),
        func.coalesce(date1_cte.c.potr, date2_cte.c.potr).label('potr'),
        *optional_cols_versions,
    ).select_from(
        date1_cte.outerjoin(
            date2_cte,
            and_(
                date1_cte.c.fo == date2_cte.c.fo,
                date1_cte.c.region == date2_cte.c.region,
                date1_cte.c.potr == date2_cte.c.potr
            ),
            full=True
        )
    )
    version_info = version_info.cte('version_info')

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
def map_column(column, mapper: Dict):
    when_clauses = []
    for key, value in mapper.items():
        when_clauses.append((cast(column, Text) == str(key), literal(value)))
    return case(*when_clauses, else_=cast(column, Text))
def ensure_list(items) -> List[Any]:
    if isinstance(items, str):
        items_list = [items]
    elif isinstance(items, Iterable) and not isinstance(items, str):
        items_list = list(items)
    else:
        items_list = [items]
    return items_list
def get_note_query(db: scoped_session, start_year: int, end_year: int, regions: List[str], industry: str) -> Tuple[Query, Query]:
    VERSION_MAPPER = version_leveled_mappings['ver_real_level1']

    def get_base_queries() -> Tuple[Tuple[DeclarativeBase, List[bool]]]:
        PSDataStart = aliased(PSDATA)
        PSDataEnd = aliased(PSDATA)
        
        base_filters_start = [
            or_(PSDataStart.year == start_year, PSDataEnd.year == end_year),
            # or_(regions is None, Regions.name.in_(ensure_list(regions))),
            Otrasl.name == industry,
        ]
        
        base_filters_end = [
            PSDataEnd.year == end_year,
            # or_(regions is None, Regions.name.in_(ensure_list(regions))),
            Otrasl.name == industry
        ]

        if regions and len(regions) > 0:
            regions_list = ensure_list(regions)
            base_filters_start.append(Regions.name.in_(regions_list))
            base_filters_end.append(Regions.name.in_(regions_list))

        return ((PSDataStart, base_filters_start), (PSDataEnd, base_filters_end))

    (PSDataStart, base_filters_start), (PSDataEnd, base_filters_end) = get_base_queries()
    mapped_version_expr = map_column(VersProgn.full_name, VERSION_MAPPER)
    mapped_version_col = mapped_version_expr.label('version_name')
    # mapped_version_col = map_column(VersProgn.full_name, VERSION_MAPPER).label('version_name')

    # base_filters_start = [
    #         PSDataStart.year == start_year,
    #         # or_(regions is None, Regions.name.in_(ensure_list(regions))),
    #         Otrasl.name == industry
    #     ]

    main_query = db.query(
        Regions.name.label('region_name'),
        FedState.short_name.label('fo_name'),
        # VersProgn.id.label('version_id'),
        mapped_version_col,#(map_column(VersProgn.full_name, VERSION_MAPPER)).label('version_name'),
        # VersProgn.short_name.label('version_short_name'),
        func.coalesce(func.sum(PSDataStart.summ), 0).label('summ_start'),
        func.coalesce(func.sum(PSDataEnd.summ), 0).label('summ_end'),
    ).join(
        Regions, Regions.id == PSDataStart.tab_region_d314_ids
    ).join(
        FedState, FedState.id == Regions.tab_fo_d314_ids
    ).join(
        VersProgn, VersProgn.id == PSDataStart.tab_ver_real_pr_d314_ids
    ).join(
        Otrasl, Otrasl.id == PSDataStart.tab_otrasl_economy_d314_ids
    ).join(
        PSDataEnd, and_(
            PSDataEnd.tab_region_d314_ids == PSDataStart.tab_region_d314_ids,
            PSDataStart.tab_contragent_d314_ids == PSDataEnd.tab_contragent_d314_ids,
            # PSDataEnd.tab_ver_real_pr_d314_ids == PSDataStart.tab_ver_real_pr_d314_ids,
            PSDataEnd.tab_otrasl_economy_d314_ids == PSDataStart.tab_otrasl_economy_d314_ids,
            # PSDataEnd.tab_fo_d314_ids == PSDataStart.tab_fo_d314_ids,
            # PSDataEnd.year == end_year
        ),
        isouter=True
    # ).join(
    #     Regions, Regions.id == PSDataEnd.tab_region_d314_ids
    ).filter(
        *base_filters_start
    ).group_by(
        VersProgn.id,
        Regions.id, Regions.name, #Regions.short_name,
        FedState.ord, FedState.short_name,# FedState.short_name,
        mapped_version_expr,#(map_column(VersProgn.full_name, VERSION_MAPPER)).label('version_name'),
    ).order_by(
        FedState.ord, Regions.name, VersProgn.id
    )

    # ТОП ПОТРЕБИТЕЛЕЙ
    # (PSDataStartContr, filters_start), (PSDataEndContr, filters_end) = get_base_queries()
    base_filters_start = [
        or_(PSDataStart.year == start_year, PSDataEnd.year == end_year),
        Otrasl.name == industry
    ]
    
    if regions and len(regions) > 0:
        regions_list = ensure_list(regions)
        base_filters_start.append(Regions.name.in_(regions_list))
    
    contragent_total = db.query(
        PSDataStart.tab_region_d314_ids.label('region_id'),
        PSDataStart.tab_ver_real_pr_d314_ids.label('version_id'),
        PSDataStart.tab_otrasl_economy_d314_ids.label('industry_id'),
        mapped_version_col,#map_column(VersProgn.full_name, VERSION_MAPPER).label('version_name'),
        PSDataStart.tab_contragent_d314_ids.label('contragent_id'),
        func.sum(PSDataStart.summ).label('total_summ_start'),
        func.sum(PSDataEnd.summ).label('total_summ_end')
    ).filter(
        *base_filters_start
    ).group_by(
        PSDataStart.tab_otrasl_economy_d314_ids,
        PSDataStart.tab_region_d314_ids,
        mapped_version_expr,#map_column(VersProgn.full_name, VERSION_MAPPER).label('version_name'),
        PSDataStart.tab_ver_real_pr_d314_ids,
        PSDataStart.tab_contragent_d314_ids,
        map_column(VersProgn.full_name, VERSION_MAPPER)
    ).join(
        Regions, Regions.id == PSDataStart.tab_region_d314_ids
    ).join(
        Otrasl, Otrasl.id == PSDataStart.tab_otrasl_economy_d314_ids
    ).join(
        PSDataEnd, and_(
            PSDataEnd.tab_region_d314_ids == PSDataStart.tab_region_d314_ids,
            PSDataEnd.tab_ver_real_pr_d314_ids == PSDataStart.tab_ver_real_pr_d314_ids,
            PSDataEnd.tab_contragent_d314_ids == PSDataStart.tab_contragent_d314_ids,
            # PSDataEnd.year == end_year
        ),
        isouter=True
    ).join(
        VersProgn, VersProgn.id == PSDataStart.tab_ver_real_pr_d314_ids
    )
    # print(contragent_total.all())
    contragent_total = contragent_total.subquery()
    ranked_contragents = db.query(
        contragent_total.c.region_id,
        contragent_total.c.version_id,
        contragent_total.c.version_name,
        contragent_total.c.contragent_id,
        contragent_total.c.industry_id,
        # (func.coalesce(contragent_total.c.total_summ_start, 0) + 
        #  func.coalesce(contragent_total.c.total_summ_end, 0)).label('total_summ'),
        func.row_number().over(
            partition_by=[contragent_total.c.region_id, contragent_total.c.version_name, contragent_total.c.industry_id],
            order_by=desc(
                # func.coalesce(contragent_total.c.total_summ_start, 0) + 
                func.coalesce(contragent_total.c.total_summ_end, 0)
            )
        ).label('rank')
    )
    print(db.execute(ranked_contragents).mappings().all())
    ranked_contragents = ranked_contragents.subquery('ranked_contragents')

    top3_query_filters = [
        ranked_contragents.c.rank <= 3,
        Otrasl.name == industry
    ]
    
    if regions and len(regions) > 0:
        regions_list = ensure_list(regions)
        top3_query_filters.append(Regions.name.in_(regions_list))
    top3_query = db.query(
        # ranked_contragents.c.region_id,
        FedState.short_name.label('fo_name'),
        Regions.name.label('region_name'),
        ranked_contragents.c.version_id,
        ranked_contragents.c.version_name,
        # ranked_contragents.c.contragent_id,
        # ranked_contragents.c.rank,
        Contragent.name.label('contragent_name'),
        func.sum(func.coalesce(PSDataStart.summ, 0)).label('summ_start'),
        func.sum(func.coalesce(PSDataEnd.summ, 0)).label('summ_end'),

        func.string_agg(func.distinct(TU.name), ', ').label('tu_list'),
        # func.count(func.distinct(PSDataStart.tab_tu_visual_d314_ids)).label('tu_count'),
        func.string_agg(func.distinct(PG.name), ', ').label('pg_list'),
        # func.count(func.distinct(PSDataStart.tab_pg_visual_d314_ids)).label('pg_count'),
        
        # func.string_agg(func.distinct(StGaz.name), ', ').label('start_gaz_names'),
        func.string_agg(func.distinct(Dogovor.name), ', ').label('dogovor_list'),
        # func.string_agg(func.distinct(Infr.name), ', ').label('infr_names'),
        # func.string_agg(func.distinct(StPotr.name), ', ').label('status_potreb_names')
    ).select_from(
        ranked_contragents
    ).join(
        Contragent, Contragent.id == ranked_contragents.c.contragent_id
    ).outerjoin(
        PSDataStart, and_(
            PSDataStart.tab_region_d314_ids == ranked_contragents.c.region_id,
            PSDataStart.tab_ver_real_pr_d314_ids == ranked_contragents.c.version_id,
            PSDataStart.tab_contragent_d314_ids == ranked_contragents.c.contragent_id,
            # PSDataStart.year == start_year
        )
    ).outerjoin(
        PSDataEnd, and_(
            PSDataEnd.tab_region_d314_ids == ranked_contragents.c.region_id,
            PSDataEnd.tab_ver_real_pr_d314_ids == ranked_contragents.c.version_id,
            PSDataEnd.tab_contragent_d314_ids == ranked_contragents.c.contragent_id,
            # PSDataEnd.year == end_year
        )
    ).join(
        Otrasl, ranked_contragents.c.industry_id == Otrasl.id
    ).join(
        Regions, ranked_contragents.c.region_id == Regions.id
    # ).outerjoin(
    #     VersProgn, VersProgn.id == PSDataStart.tab_ver_real_pr_d314_ids
    ).join(
        FedState, FedState.id == Regions.tab_fo_d314_ids,
    ).outerjoin(
        TU, TU.id == PSDataStart.tab_tu_visual_d314_ids
    ).outerjoin(
        PG, PG.id == PSDataStart.tab_pg_visual_d314_ids
    ).outerjoin(
        Dogovor, Dogovor.id == PSDataStart.tab_dogovor_visual_d314_ids
    ).filter(
        *top3_query_filters
    ).group_by(
        FedState.short_name,
        Regions.name,
        ranked_contragents.c.region_id,
        ranked_contragents.c.version_name,
        ranked_contragents.c.version_id,
        ranked_contragents.c.industry_id,
        # ranked_contragents.c.contragent_id,
        ranked_contragents.c.rank,
        Contragent.name,
        # Contragent.inn
    ).order_by(
        ranked_contragents.c.region_id,
        ranked_contragents.c.version_name,
        ranked_contragents.c.rank
    )

    return (main_query, top3_query)


# # Запрос для справочника регионов по ФО
# def fo_regions_map_query(db, regions: List[str]):
#     query = db.query()
