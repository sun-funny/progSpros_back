from decimal import Decimal
from datetime import datetime
from sqlalchemy import func, select, and_, distinct, or_
from flask import jsonify, session, request
from flask_restx import Namespace, Resource
# Import the database session
from progSpros_back.database_ps import db, cache, errorhandler
from progSpros_back.functions.chart_data_functions_ps import apply_dynamic_filters
from progSpros_back.functions.query_functions_ps import fo_otrasl_query
from progSpros_back.functions.utility_functions_ps import create_filter_params, create_structure_fo, set_db_connection, \
    mapping, to_date
from progSpros_back.model.db_models_ps import PSDATA, reference_models, FedState, Otrasl, VersProgn, GroupPost, Regions
from progSpros_back.model.mappings_ps import yn_mapping

# Define the namespace
ns_map_otr_ps = Namespace('MapOtrasl', description='Карта по отраслям')


@ns_map_otr_ps.route('/map_ps')
@ns_map_otr_ps.response(200, 'Success')
@ns_map_otr_ps.doc(params={
    'yearfrom': {'description': 'till what year number', 'in': 'query', 'type': 'integer'},
    'yearto': {'description': 'till what year number', 'in': 'query', 'type': 'integer'},
    'otrasl': {'description': 'Отрасль', 'in': 'query', 'type': 'string'},
    'vers': {'description': 'Версия прогноза', 'in': 'query', 'type': 'string'},
    'grpost': {'description': 'Группа поставщиков', 'in': 'query', 'type': 'string'},
    'fo': {'description': 'Федеральный округ', 'in': 'query', 'type': 'string'},
    'region': {'description': 'Регион', 'in': 'query', 'type': 'string'},
    'dogovor': {'description': 'Договор', 'in': 'query', 'type': 'string'},
    'tu': {'description': 'ТУ', 'in': 'query', 'type': 'string'},
    'infr': {'description': 'Ифнраструктура', 'in': 'query', 'type': 'string'},
    'date': {'description': 'Дата загрузки', 'in': 'query', 'type': 'to_date'}
})

class MapRF(Resource):
    def get(self):
        """
        Возвращает обратно данные для карты

        Аргументы:
            - принимает аргумент global_filters из /get_basic_filters
            - принимает аргументы yearfrom, yearto
        """
        try:
            #db = set_db_connection()
            # Мэппинги из справочников
            otr_mapping = mapping(Otrasl)
            vers_mapping = mapping(VersProgn)
            grpost_mapping = mapping(GroupPost)
            fo_mapping = mapping(FedState)
            region_mapping = mapping(Regions)

            # Получить фильтр-параметры из запроса
            filter_params = create_filter_params(request)
            if not filter_params:
                filter_params = session.get('filter_params')

            # Определите базовый запрос с помощью динамических фильтров
            base_query = db.query(PSDATA)
            base_query = apply_dynamic_filters(base_query, PSDATA, filter_params, db, reference_models)

            yearfrom = request.args.get('yearfrom', 2023, type=int)
            yearto = request.args.get('yearto', 2034, type=int)
            date = request.args.get('date', type=to_date)

            # Параметры отраслей
            reverse_otr_mapping = {value: key for key, value in otr_mapping.items()}
            otr = [company.strip() for item in request.args.getlist('otrasl', None) for company in item.split(',')]
            mapped_otr = [reverse_otr_mapping.get(company, company) for company in otr]
            if otr:
                base_query = base_query.filter((PSDATA.tab_otrasl_economy_d314_ids.in_(mapped_otr)))

            # Параметры версий
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

            # Продолжить создавать основной запрос
            query = fo_otrasl_query(base_query, PSDATA, FedState, Otrasl, yearfrom, yearto, date)
            title = f"Карта по отраслям"
            version_mapping = {
                'Дальневосточный федеральный округ': 'DFO',
                'Приволжский федеральный округ': 'PFO',
                'Северо-Западный федеральный округ': 'SZFO',
                'Северо-Кавказский федеральный округ': 'SKFO',
                'Сибирский федеральный округ': 'SFO',
                'Уральский федеральный округ': 'UFO',
                'Центральный федеральный округ': 'CFO',
                'Южный федеральный округ': 'YUFO'
            }

            # Создать структуру вывода для Json
            structure = {}
            results = query.all()
            structure = create_structure_fo('Карта по отраслям', 'otrasl_list', results, version_mapping, structure)
            structure = {version_mapping.get(year, year): versions for year, versions in structure.items()}

            graph_data = {
                "title": title,
                "data": structure
            }

            #            return jsonify(graph_data)
            response = jsonify(graph_data)
            response.headers.add('Access-Control-Allow-Origin', '*');
            return response

        except Exception as e:
            ns_map_otr_ps.abort(*errorhandler(e))