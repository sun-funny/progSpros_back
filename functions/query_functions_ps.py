from sqlalchemy import func, and_, case

# Округа и регионы
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
    )

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
def otrasl_query(base_query, tab_progn_spr_gaz_d314, tab_otrasl_economy_d314, yearfrom, yearto):
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
    ).group_by(
        tab_otrasl_economy_d314.name,
        tab_progn_spr_gaz_d314.year
    ).order_by(
        tab_otrasl_economy_d314.name,
        tab_progn_spr_gaz_d314.year,
        (func.sum(tab_progn_spr_gaz_d314.summ).desc())
    )
    )
def query_prirost(base_query, tab_progn_spr_gaz_d314, tab_otrasl_economy_d314, otrasl_name, year_par):
    return (base_query.with_entities(
                func.sum(tab_progn_spr_gaz_d314.summ).label('sum_par')
    ).join(
    tab_otrasl_economy_d314, tab_otrasl_economy_d314.id == tab_progn_spr_gaz_d314.tab_otrasl_economy_d314_ids
    ).filter(tab_progn_spr_gaz_d314.year == year_par
    ).filter(tab_otrasl_economy_d314.name == otrasl_name
    )
    )
# Прогнозный спрос РФ топ-5 потребителей
def top_potr_query(base_query, tab_progn_spr_gaz_d314, tab_contragent_d314, tab_ver_real_pr_d314, yearfrom, yearto):
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
# Sankey
def sankey_query(base_query, tab_progn_spr_gaz_d314, tab_group_post_d314, tab_proizvoditel_d314, yearfrom, yearto):
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
        tab_proizvoditel_d314.name.label('proizv'),
        tab_group_post_d314.name.label('grpost'),
        func.sum(tab_progn_spr_gaz_d314.summ).label('summ')
    ).join(
    tab_proizvoditel_d314, tab_proizvoditel_d314.id == tab_progn_spr_gaz_d314.tab_proizvoditel_d314_ids
    ).join(
    tab_group_post_d314, tab_group_post_d314.id == tab_progn_spr_gaz_d314.tab_group_post_d314_ids
    ).filter(tab_progn_spr_gaz_d314.year == yearfrom
    ).group_by(
        tab_proizvoditel_d314.name,
        tab_group_post_d314.name,
    ).order_by(
        tab_proizvoditel_d314.name,
        tab_group_post_d314.name,
    )
    )
def sankey_query2(base_query, tab_progn_spr_gaz_d314, tab_otrasl_economy_d314, tab_group_post_d314, yearfrom, yearto):
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
        tab_group_post_d314.name.label('grpost'),
        tab_otrasl_economy_d314.name.label('otrasl'),
        func.sum(tab_progn_spr_gaz_d314.summ).label('summ')
    ).join(
    tab_otrasl_economy_d314, tab_otrasl_economy_d314.id == tab_progn_spr_gaz_d314.tab_otrasl_economy_d314_ids
    ).join(
    tab_group_post_d314, tab_group_post_d314.id == tab_progn_spr_gaz_d314.tab_group_post_d314_ids
    ).filter(tab_progn_spr_gaz_d314.year == yearfrom
    ).group_by(
        tab_otrasl_economy_d314.name,
        tab_group_post_d314.name,
    ).order_by(
        tab_otrasl_economy_d314.name,
        tab_group_post_d314.name,
    )
    )

def sankey_query3(base_query, tab_progn_spr_gaz_d314, tab_proizvoditel_d314, yearfrom, yearto):
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
        tab_proizvoditel_d314.name.label('proizv'),
        func.sum(tab_progn_spr_gaz_d314.summ).label('summ')
    ).join(
    tab_proizvoditel_d314, tab_proizvoditel_d314.id == tab_progn_spr_gaz_d314.tab_proizvoditel_d314_ids
    ).filter(tab_progn_spr_gaz_d314.year == yearfrom
    ).group_by(
        tab_proizvoditel_d314.name
    ).order_by(
        tab_proizvoditel_d314.name,
    )
    )
def sankey_query4(base_query, tab_progn_spr_gaz_d314, tab_group_post_d314, yearfrom, yearto):
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
        tab_group_post_d314.name.label('grpost'),
        func.sum(tab_progn_spr_gaz_d314.summ).label('summ')
    ).join(
    tab_group_post_d314, tab_group_post_d314.id == tab_progn_spr_gaz_d314.tab_group_post_d314_ids
    ).filter(tab_progn_spr_gaz_d314.year == yearfrom
    ).group_by(
        tab_group_post_d314.name
    ).order_by(
        tab_group_post_d314.name
    )
    )
def sankey_query5(base_query, tab_progn_spr_gaz_d314, tab_otrasl_economy_d314, yearfrom, yearto):
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
        func.sum(tab_progn_spr_gaz_d314.summ).label('summ')
    ).join(
    tab_otrasl_economy_d314, tab_otrasl_economy_d314.id == tab_progn_spr_gaz_d314.tab_otrasl_economy_d314_ids
    ).filter(tab_progn_spr_gaz_d314.year == yearfrom
    ).group_by(
        tab_otrasl_economy_d314.name
    ).order_by(
        tab_otrasl_economy_d314.name
    )
    )
# Карта по отраслям
def fo_otrasl_query(base_query, tab_progn_spr_gaz_d314, tab_fo_d314, tab_otrasl_economy_d314, yearfrom, yearto):
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
        tab_fo_d314.name.label('fo'),
        tab_otrasl_economy_d314.name.label('otrasl'),
        func.sum(tab_progn_spr_gaz_d314.summ).label('total_indicator')
    ).join(
    tab_otrasl_economy_d314, tab_otrasl_economy_d314.id == tab_progn_spr_gaz_d314.tab_otrasl_economy_d314_ids
    ).join(
    tab_fo_d314, tab_fo_d314.id == tab_progn_spr_gaz_d314.tab_fo_d314_ids
    ).filter(tab_progn_spr_gaz_d314.year == yearto
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
def fo_potr_query(base_query, tab_progn_spr_gaz_d314, tab_fo_d314, tab_contragent_d314, yearfrom, yearto):
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
        tab_fo_d314.name.label('fo'),
        tab_contragent_d314.name.label('potr'),
        func.sum(tab_progn_spr_gaz_d314.summ).label('total_indicator')
    ).join(
    tab_contragent_d314, tab_contragent_d314.id == tab_progn_spr_gaz_d314.tab_contragent_d314_ids
    ).join(
    tab_fo_d314, tab_fo_d314.id == tab_progn_spr_gaz_d314.tab_fo_d314_ids
    ).filter(tab_progn_spr_gaz_d314.year == yearto
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
                     tab_dogovor_visual_d314, tab_tu_visual_d314, yearfrom, yearto, tab_contragent_d314):
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
    """
        запрос мэппинга по отраслям из таблицы tab_otrasl_economy_d314
    """
    return (base_query.with_entities(
        tab_progn_spr_gaz_d314.year.label('year')
    ).group_by(
        tab_progn_spr_gaz_d314.year
    ).order_by(
        tab_progn_spr_gaz_d314.year
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