from decimal import Decimal
from datetime import datetime
from sqlalchemy import func, select, and_, distinct, or_
from flask import jsonify, session, request
from flask_restx import Namespace, Resource
# Import the database session
from progSpros_back.database_ps import cache, errorhandler
from progSpros_back.functions.chart_data_functions_ps import apply_dynamic_filters
from progSpros_back.functions.query_functions_ps import sankey_query, sankey_query2, sankey_query3, sankey_query4, sankey_query5
from progSpros_back.functions.utility_functions_ps import create_filter_params, create_structure, set_db_connection
from progSpros_back.model.db_models_ps import PSDATA, reference_models, Otrasl, Contragent, FedState, Regions, GroupPost, StPotr, StGaz, PG, Dogovor, TU, Proizv
from progSpros_back.model.mappings_ps import otr_mapping, vers_mapping, grpost_mapping, fo_mapping, region_mapping, yn_mapping

# Define the namespace
ns_sankey_ps = Namespace('Sankey', description='Sankey')


@ns_sankey_ps.route('/sankey')
@ns_sankey_ps.response(200, 'Success')
@ns_sankey_ps.doc(params={
    'yearfrom': {'description': 'till what year number', 'in': 'query', 'type': 'integer'},
    'yearto': {'description': 'till what year number', 'in': 'query', 'type': 'integer'},
    'otrasl': {'description': 'Отрасль', 'in': 'query', 'type': 'string'},
    'vers': {'description': 'Версия прогноза', 'in': 'query', 'type': 'string'},
    'grpost': {'description': 'Группа поставщиков', 'in': 'query', 'type': 'string'},
    'fo': {'description': 'Федеральный округ', 'in': 'query', 'type': 'string'},
    'region': {'description': 'Регион', 'in': 'query', 'type': 'string'},
    'dogovor': {'description': 'Договор', 'in': 'query', 'type': 'string'},
    'tu': {'description': 'ТУ', 'in': 'query', 'type': 'string'},
    'infr': {'description': 'Инфраструктура', 'in': 'query', 'type': 'string'}
})

class Sankey(Resource):
    def get(self):
        """
        Возвращает обратно данные для Прогнозный спрос РФ

        Аргументы:
            - принимает аргумент global_filters из /get_basic_filters
            - принимает аргументы yearfrom, yearto
        """
        try:
            db = set_db_connection()
            # Получить фильтр-параметры из запроса
            filter_params = create_filter_params(request)
            # Если не заданы глобальные параметры, взять их из session
            if not filter_params:
                filter_params = session.get('filter_params')

            # Определите базовый запрос с помощью динамических фильтров
            base_query = db.query(PSDATA)
            base_query = apply_dynamic_filters(base_query, PSDATA, filter_params, db, reference_models)

            yearfrom = request.args.get('yearfrom', 2023, type=int)
            yearto = request.args.get('yearto', 2034, type=int)

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

            query = sankey_query(base_query, PSDATA, GroupPost, Proizv, yearfrom, yearto)
            
            query2 = sankey_query2(base_query, PSDATA, Otrasl, GroupPost, yearfrom, yearto)

            query3 = sankey_query3(base_query, PSDATA, Proizv, yearfrom, yearto)
                       
            query4 = sankey_query4(base_query, PSDATA, GroupPost, yearfrom, yearto)
            
            query5 = sankey_query5(base_query, PSDATA, Otrasl, yearfrom, yearto)
            
            summ_all = 0
            result = {'nodes':[],'data':[], 'sum_all': round(summ_all/1000, 2)}
            title = f"Sankey"

            #Проценты Производители
            for row in query3:
                if row.proizv not in result:
                    summ_all += row.summ

            for row in query3:
                summ = round(row.summ/1000, 2)
                if summ < 0.01:
                    summ = 0.01

                result_dict = {'proizv': row.proizv, 'sum': summ, 'percent': round(row.summ / summ_all * 100,1)}

                if result_dict not in result['nodes']:
                    result['nodes'].append(result_dict)
            # Проценты Группа поставщиков
            for row in query4:
                summ = round(row.summ/1000, 2)
                if summ < 0.01:
                    summ = 0.01

                result_dict = {'grpost': row.grpost, 'sum': summ, 'percent': round(row.summ / summ_all * 100,1)}

                if result_dict not in result['nodes']:
                    result['nodes'].append(result_dict)

            # Проценты Группа поставщиков
            for row in query5:
                summ = round(row.summ/1000, 2)
                if summ < 0.01:
                    summ = 0.01

                result_dict = {'otrasl': row.otrasl, 'sum': summ, 'percent': round(row.summ / summ_all * 100,1)}

                if result_dict not in result['nodes']:
                    result['nodes'].append(result_dict)

            for row in query:
                summ = row.summ
                #summ = round(row.summ/1000, 2)
                #if summ < 0.01:
                #    summ = 0.01
                result_dict = {'proizv': row.proizv, 'grpost': row.grpost, 'sum': summ}
                if result_dict not in result['data']:
                    result['data'].append(result_dict)


            for row in query2:
                summ = row.summ
                #summ = round(row.summ/1000, 2)
                #if summ < 0.01:
                #    summ = 0.01
                result_dict2 = {'grpost': row.grpost,'otrasl': row.otrasl, 'sum': summ}
                if result_dict2 not in result['data']:
                    result['data'].append(result_dict2)

            summ_itog = summ_all/1000
            #summ_itog = round(summ_all/1000, 2)
            #if summ_itog < 0.01:
            #    summ_itog = 0.01

            result['sum_all'] = {'sum_all': summ_itog}

            graph_data = {
                "title": title,
                "data": result
            }

            response = jsonify(graph_data)
            response.headers.add('Access-Control-Allow-Origin', '*');
            return response
        except Exception as e:
            ns_sankey_ps.abort(*errorhandler(e))