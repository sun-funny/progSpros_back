from flask import session
from sqlalchemy import and_, func, case
from progSpros_back.functions.chart_data_functions_ps import apply_dynamic_filters
from progSpros_back.functions.utility_functions_ps import create_filter_params, set_db_connection
from progSpros_back.model.db_models_ps import Prirost, PSDATA, reference_models, Otrasl, FedState, Regions, GroupPost, Contragent, StPotr, StGaz, PG, Dogovor, TU, Infr, VersProgn, Proizv
from progSpros_back.model.mappings_ps import otr_mapping, vers_mapping, grpost_mapping, fo_mapping, region_mapping, yn_mapping
from progSpros_back.database_ps import db

def get_query(request, yearfrom, yearto):
    try:
        # Получить фильтр-параметры из запроса
        filter_params = create_filter_params(request)
        # Если не заданы глобальные параметры, взять их из session
        if not filter_params:
            filter_params = session.get('filter_params')

        base_query = (
            db.query(
                PSDATA.tab_fo_d314_ids,
                PSDATA.tab_region_d314_ids,
                PSDATA.tab_otrasl_economy_d314_ids,
                PSDATA.tab_contragent_d314_ids,
                PSDATA.tab_status_potreb_d314_ids,
                PSDATA.tab_group_post_d314_ids,
                PSDATA.tab_dogovor_visual_d314_ids,
                PSDATA.tab_tu_visual_d314_ids,
                PSDATA.tab_infr_d314_ids,
                PSDATA.tab_ver_real_pr_d314_ids,
                PSDATA.tab_start_gaz_d314_ids,
                PSDATA.year,
                func.sum(PSDATA.summ).label("summ")
            )
            .filter(
                PSDATA.year >= yearfrom,
                PSDATA.year <= yearto
            )
            .group_by(
                PSDATA.tab_fo_d314_ids,
                PSDATA.tab_region_d314_ids,
                PSDATA.tab_otrasl_economy_d314_ids,
                PSDATA.tab_contragent_d314_ids,
                PSDATA.tab_status_potreb_d314_ids,
                PSDATA.tab_group_post_d314_ids,
                PSDATA.tab_dogovor_visual_d314_ids,
                PSDATA.tab_tu_visual_d314_ids,
                PSDATA.tab_infr_d314_ids,
                PSDATA.tab_ver_real_pr_d314_ids,
                PSDATA.tab_start_gaz_d314_ids,
                PSDATA.year
            )
        )

        base_query = apply_dynamic_filters(base_query, PSDATA, filter_params, db, reference_models)

        sum_pr = request.args.get('sum_pr', 0, type=int)

        # Параметры Отрасль
        reverse_otr_mapping = {value: key for key, value in otr_mapping.items()}
        otr = [company.strip() for item in request.args.getlist('otrasl', None) for company in item.split(',')]
        mapped_otr = [reverse_otr_mapping.get(company, company) for company in otr]
        if otr:
            base_query = base_query.filter((PSDATA.tab_otrasl_economy_d314_ids.in_(mapped_otr)))

        # Параметры Версия прогноза
        reverse_vers_mapping = {value: key for key, value in vers_mapping.items()}
        vers = [company.strip() for item in request.args.getlist('vers', None) for company in item.split(',')]
        mapped_vers = [reverse_vers_mapping.get(company, company) for company in vers]
        if vers:
            base_query = base_query.filter((PSDATA.tab_ver_real_pr_d314_ids.in_(mapped_vers)))

        # Параметры Группа поставщиков
        reverse_grpost_mapping = {value: key for key, value in grpost_mapping.items()}
        grpost = [company.strip() for item in request.args.getlist('grpost', None) for company in item.split(',')]
        mapped_grpost = [reverse_grpost_mapping.get(company, company) for company in grpost]
        if grpost:
            base_query = base_query.filter((PSDATA.tab_group_post_d314_ids.in_(mapped_grpost)))

        # Параметры Федеральный округ
        reverse_fo_mapping = {value: key for key, value in fo_mapping.items()}
        fo = [company.strip() for item in request.args.getlist('fo', None) for company in item.split(',')]
        mapped_fo = [reverse_fo_mapping.get(company, company) for company in fo]
        if fo:
            base_query = base_query.filter((PSDATA.tab_fo_d314_ids.in_(mapped_fo)))

        # Параметры Регион
        reverse_region_mapping = {value: key for key, value in region_mapping.items()}
        region = [company.strip() for item in request.args.getlist('region', None) for company in item.split(',')]
        mapped_region = [reverse_region_mapping.get(company, company) for company in region]
        if region:
            base_query = base_query.filter((PSDATA.tab_region_d314_ids.in_(mapped_region)))

        # Параметры Договор
        reverse_dog_mapping = {value: key for key, value in yn_mapping.items()}
        dogovor = [company.strip() for item in request.args.getlist('dogovor', None) for company in item.split(',')]
        mapped_dog = [reverse_dog_mapping.get(company, company) for company in dogovor]
        if dogovor:
            base_query = base_query.filter((PSDATA.tab_dogovor_visual_d314_ids.in_(mapped_dog)))

        # Параметры ТУ
        reverse_tu_mapping = {value: key for key, value in yn_mapping.items()}
        tu = [company.strip() for item in request.args.getlist('tu', None) for company in item.split(',')]
        mapped_tu = [reverse_tu_mapping.get(company, company) for company in tu]
        if tu:
            base_query = base_query.filter((PSDATA.tab_tu_visual_d314_ids.in_(mapped_tu)))

        # Параметры Инфраструктура
        reverse_infr_mapping = {value: key for key, value in yn_mapping.items()}
        infr = [company.strip() for item in request.args.getlist('infr', None) for company in item.split(',')]
        mapped_infr = [reverse_infr_mapping.get(company, company) for company in infr]
        if infr:
            base_query = base_query.filter((PSDATA.tab_infr_d314_ids.in_(mapped_infr)))
        years = [str(year) for year in range(yearfrom, yearto + 1)]

        # Продолжить создавать основной запрос
        query = ots_pr_spr_pot_query(years, base_query, Prirost, PSDATA, Otrasl, FedState,
                                    Regions, GroupPost, StPotr, StGaz, Infr, Dogovor, TU,
                                    yearfrom, yearto, Contragent, VersProgn, db)

        #from sqlalchemy.dialects import postgresql
        # This will print the query with parameters inline
        #print(query.statement.compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}))

        return query
    finally:
        db.close()

def ots_pr_spr_pot_query(
        years, base_query,
        tab_prirost_d314, tab_progn_spr_gaz_d314, tab_otrasl_economy_d314, tab_fo_d314,
        tab_region_d314, tab_group_post_d314, tab_status_potreb_d314, tab_start_gaz_d314, tab_infr_d314,
        tab_dogovor_visual_d314, tab_tu_visual_d314, yearfrom, yearto, tab_contragent_d314, tab_ver_real_pr_d314, db
):
    base_subq = base_query.subquery()

    # колонки по годам
    columns = [
        func.sum(
            case((base_subq.c.year == int(year), base_subq.c.summ), else_=0)
        ).label(f'y{year}')
        for year in years
    ]

    prirost_col = (
        func.sum(case((base_subq.c.year == yearto, base_subq.c.summ), else_=0))
        - func.sum(case((base_subq.c.year == yearfrom, base_subq.c.summ), else_=0))
    ).label("prirost")

    query = (
        db.query(
            tab_fo_d314.name.label("fo"),
            tab_region_d314.name.label("region"),
            case(
                (tab_contragent_d314.name.in_(["Действующие потребители", "Прочие потребители"]), None),
                else_=tab_otrasl_economy_d314.name,
            ).label("otrasl"),
            case(
                (tab_contragent_d314.name.in_(["Действующие потребители", "Прочие потребители"]), None),
                else_=tab_otrasl_economy_d314.ord,
            ).label("otrasl_ord"),
            tab_contragent_d314.name.label("contragent"),
            tab_group_post_d314.name.label("grpost"),
            tab_status_potreb_d314.name.label("stpotr"),
            tab_start_gaz_d314.name.label("stgaz"),
            tab_infr_d314.name.label("infr"),
            tab_dogovor_visual_d314.name.label("dogovor"),
            tab_tu_visual_d314.name.label("tu"),
            prirost_col,
            tab_ver_real_pr_d314.full_name.label("ver_real"),
            *columns,
        )
        .select_from(base_subq)
        .join(tab_otrasl_economy_d314, tab_otrasl_economy_d314.id == base_subq.c.tab_otrasl_economy_d314_ids)
        .join(tab_fo_d314, tab_fo_d314.id == base_subq.c.tab_fo_d314_ids)
        .join(tab_region_d314, tab_region_d314.id == base_subq.c.tab_region_d314_ids)
        .join(tab_group_post_d314, tab_group_post_d314.id == base_subq.c.tab_group_post_d314_ids)
        .join(tab_status_potreb_d314, tab_status_potreb_d314.id == base_subq.c.tab_status_potreb_d314_ids)
        .join(tab_start_gaz_d314, tab_start_gaz_d314.id == base_subq.c.tab_start_gaz_d314_ids)
        .join(tab_infr_d314, tab_infr_d314.id == base_subq.c.tab_infr_d314_ids)
        .join(tab_dogovor_visual_d314, tab_dogovor_visual_d314.id == base_subq.c.tab_dogovor_visual_d314_ids)
        .join(tab_tu_visual_d314, tab_tu_visual_d314.id == base_subq.c.tab_tu_visual_d314_ids)
        .join(tab_contragent_d314, tab_contragent_d314.id == base_subq.c.tab_contragent_d314_ids)
        .join(tab_ver_real_pr_d314, tab_ver_real_pr_d314.id == base_subq.c.tab_ver_real_pr_d314_ids)
        .group_by(
            tab_otrasl_economy_d314.name,
            tab_otrasl_economy_d314.ord,
            tab_fo_d314.name,
            tab_region_d314.name,
            tab_group_post_d314.name,
            tab_status_potreb_d314.name,
            tab_start_gaz_d314.name,
            tab_infr_d314.name,
            tab_dogovor_visual_d314.name,
            tab_tu_visual_d314.name,
            tab_contragent_d314.name,
            tab_ver_real_pr_d314.full_name,
        )
        .order_by(tab_otrasl_economy_d314.ord)
    )

    return query