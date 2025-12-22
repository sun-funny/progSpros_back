from decimal import Decimal
from datetime import datetime
from sqlalchemy import func, select, and_, distinct, or_
from flask import jsonify, session, request
from flask_restx import Namespace, Resource
# Import the database session
from progSpros_back.database_ps import db, cache, errorhandler
from progSpros_back.functions.chart_data_functions_ps import apply_dynamic_filters
from progSpros_back.functions.query_functions_ps import big_invest_query_potr, query_prirost_potr_table
from progSpros_back.functions.utility_functions_ps import create_filter_params, substitute_in_json, sum_prirost, set_db_connection, mapping
from progSpros_back.model.db_models_ps import Prirost, reference_models, Otrasl, FedState, Regions, GroupPost, \
    Contragent, StPotr, StGaz, PG, Dogovor, TU, Infr, VersProgn
from progSpros_back.model.mappings_ps import yn_mapping

# Define the namespace
ns_big_invest_ps = Namespace('BigInvest', description='Крупные инвестиционные проекты')

# Рут для очистки данных сессии
@ns_big_invest_ps.route('/clear_session_flask')
@ns_big_invest_ps.response(200, 'True: session cleared')  # Ответ при успешной очистке
class ClearSession(Resource):
    def delete(self):
        """ Очищает сессию Flask """
        session.clear()  # Очищаем хранилище сессий Flask
        return "Session cleared", 200  # Сообщение об успешной очистке

@ns_big_invest_ps.route('/big_invest_ps')
@ns_big_invest_ps.response(200, 'Success')
@ns_big_invest_ps.doc(params={
    'yearfrom': {'description': 'Год с', 'in': 'query', 'type': 'integer'},
    'yearto': {'description': 'Год по', 'in': 'query', 'type': 'integer'},
    'sum_pr': {'description': 'Прирост с', 'in': 'query', 'type': 'integer'},
    'otrasl': {'description': 'Отрасль', 'in': 'query', 'type': 'string'},
    'vers': {'description': 'Версия прогноза', 'in': 'query', 'type': 'string'},
    'grpost': {'description': 'Группа поставщиков', 'in': 'query', 'type': 'string'},
    'fo': {'description': 'Федеральный округ', 'in': 'query', 'type': 'string'},
    'region': {'description': 'Регион', 'in': 'query', 'type': 'string'},
    'dogovor': {'description': 'Договор', 'in': 'query', 'type': 'string'},
    'tu': {'description': 'ТУ', 'in': 'query', 'type': 'string'},
    'infr': {'description': 'Инфраструктура', 'in': 'query', 'type': 'string'}
    })

class BigInvest(Resource):
    def get(self):
        """
        Возвращает данные для карты
        Аргументы:
            - принимает аргументы yearfrom, yearto
        """
        try:
            #db = set_db_connection()
            # Получить фильтр-параметры из запроса
            filter_params = create_filter_params(request)
            # Если не заданы глобальные параметры, взять их из session
            if not filter_params:
                filter_params = session.get('filter_params')

            # Определите базовый запрос с помощью динамических фильтров
            base_query = db.query(Prirost)
            base_query = apply_dynamic_filters(base_query, Prirost, filter_params, db, reference_models)

            # Мэппинги из справочников
            otr_mapping = mapping(Otrasl)
            vers_mapping = mapping(VersProgn)
            grpost_mapping = mapping(GroupPost)
            fo_mapping = mapping(FedState)
            region_mapping = mapping(Regions)

            yearfrom = request.args.get('yearfrom', 2023, type=int)
            yearto = request.args.get('yearto', 2034, type=int)
            sum_pr = request.args.get('sum_pr', 0, type=int)

            # Параметры Отрасль
            reverse_otr_mapping = {value: key for key, value in otr_mapping.items()}
            otr = [company.strip() for item in request.args.getlist('otrasl', None) for company in item.split(',')]
            mapped_otr = [reverse_otr_mapping.get(company, company) for company in otr]
            if otr:
                base_query = base_query.filter((Prirost.tab_otrasl_economy_d314_ids.in_(mapped_otr)))

            # Параметры Версия прогноза
            reverse_vers_mapping = {value: key for key, value in vers_mapping.items()}
            vers = [company.strip() for item in request.args.getlist('vers', None) for company in item.split(',')]
            mapped_vers = [reverse_vers_mapping.get(company, company) for company in vers]
            if vers:
                base_query = base_query.filter((Prirost.tab_ver_real_pr_d314_ids.in_(mapped_vers)))

            # Параметры Группа поставщиков
            reverse_grpost_mapping = {value: key for key, value in grpost_mapping.items()}
            grpost = [company.strip() for item in request.args.getlist('grpost', None) for company in item.split(',')]
            mapped_grpost = [reverse_grpost_mapping.get(company, company) for company in grpost]
            if grpost:
                base_query = base_query.filter((Prirost.tab_group_post_d314_ids.in_(mapped_grpost)))

            # Параметры Федеральный округ
            reverse_fo_mapping = {value: key for key, value in fo_mapping.items()}
            fo = [company.strip() for item in request.args.getlist('fo', None) for company in item.split(',')]
            mapped_fo = [reverse_fo_mapping.get(company, company) for company in fo]
            if fo:
                base_query = base_query.filter((Prirost.tab_fo_d314_ids.in_(mapped_fo)))

            # Параметры Регион
            reverse_region_mapping = {value: key for key, value in region_mapping.items()}
            region = [company.strip() for item in request.args.getlist('region', None) for company in item.split(',')]
            mapped_region = [reverse_region_mapping.get(company, company) for company in region]
            if region:
                base_query = base_query.filter((Prirost.tab_region_d314_ids.in_(mapped_region)))

            # Параметры Договор
            reverse_dog_mapping = {value: key for key, value in yn_mapping.items()}
            dogovor = [company.strip() for item in request.args.getlist('dogovor', None) for company in item.split(',')]
            mapped_dog = [reverse_dog_mapping.get(company, company) for company in dogovor]
            if dogovor:
                base_query = base_query.filter((Prirost.tab_dogovor_visual_d314_ids.in_(mapped_dog)))

            # Параметры ТУ
            reverse_tu_mapping = {value: key for key, value in yn_mapping.items()}
            tu = [company.strip() for item in request.args.getlist('tu', None) for company in item.split(',')]
            mapped_tu = [reverse_tu_mapping.get(company, company) for company in tu]
            if tu:
                base_query = base_query.filter((Prirost.tab_tu_visual_d314_ids.in_(mapped_tu)))

            # Параметры Инфраструктура
            reverse_infr_mapping = {value: key for key, value in yn_mapping.items()}
            infr = [company.strip() for item in request.args.getlist('infr', None) for company in item.split(',')]
            mapped_infr = [reverse_infr_mapping.get(company, company) for company in infr]
            if infr:
                base_query = base_query.filter((Prirost.tab_infr_d314_ids.in_(mapped_infr)))
                from sqlalchemy.dialects import postgresql

            # Продолжить создавать основной запрос
            query = big_invest_query_potr(base_query, Prirost, Otrasl, FedState, Regions, GroupPost, StPotr, StGaz, Infr,
                                     Dogovor, TU, yearfrom, yearto, Contragent)
            
            from sqlalchemy.dialects import postgresql

            # This will print the query with parameters inline
            print(query.statement.compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}))

            title = f"Крупные инвестиционные проекты"

            # Создать структуру вывода для Json
            result = []
            i = 0
            for row in query:

                if row.dogovor == 'V':
                    dog = True
                else:
                    dog = False

                if row.infr == 'V':
                    infr = True
                else:
                    infr = False

                if row.tu == 'V':
                    tu = True
                else:
                    tu = False

                if float(row.prirost) >= sum_pr and float(row.prirost) != 0:
                    result_dict ={'fo': row.fo,
                                  'reg': row.region,
                                  'grp': row.grpost,
                                  'otrasl': row.otrasl,
                                  'stp': row.stpotr,
                                  'st': row.stgaz,
                                  'pg': infr,
                                  'dog': dog,
                                  'tu': tu,
                                  'prirost': float(row.prirost),
                                  'contragent': row.contragent
                             }

                    result.append(result_dict)
                    i += 1
                if i > 100:
                    break

            def get_prirost(element):
                return element['prirost']
            
            result.sort(key=get_prirost, reverse=True)
            
            graph_data = {
                "title": title,
                "data": result
                }

                #            return jsonify(graph_data)
            response = jsonify(graph_data)
            response.headers.add('Access-Control-Allow-Origin', '*')

            return response

        except Exception as e:
                ns_big_invest_ps.abort(*errorhandler(e))
