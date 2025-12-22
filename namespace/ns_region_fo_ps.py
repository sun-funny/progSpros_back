from decimal import Decimal
from datetime import datetime
from sqlalchemy import func, select, and_, distinct, or_
from flask import jsonify, session, request
from flask_restx import Namespace, Resource
# Import the database session
from progSpros_back.database_ps import db, cache, errorhandler
from progSpros_back.functions.chart_data_functions_ps import apply_dynamic_filters
from progSpros_back.functions.query_functions_ps import region_fo_query
from progSpros_back.functions.utility_functions_ps import create_filter_params, set_db_connection, mapping
from progSpros_back.model.db_models_ps import PSDATA, reference_models, FedState, Regions

# Define the namespace
ns_region_fo_ps = Namespace('RegionFO', description='Федеральные округа и Регионы')


@ns_region_fo_ps.route('/region-fo')
@ns_region_fo_ps.response(200, 'Success')
@ns_region_fo_ps.doc(params={
    'region': {'description': 'Регион', 'in': 'query', 'type': 'string'}
})
class FORegionDATA(Resource):
    def get(self):
        """
        Возвращает регионы в зависимости от выбранного округа
        """
        try:
            #db = set_db_connection()

            # Мэппинги из справочников
            fo_mapping = mapping(FedState)
            region_mapping = mapping(Regions)

            # Получить фильтр-параметры из запроса
            filter_params = create_filter_params(request)

            # Если не заданы глобальные параметры, взять их из session
            if not filter_params:
                filter_params = session.get('filter_params')

            # Определите базовый запрос с помощью динамических фильтров
            base_query = db.query(Regions)
            base_query = apply_dynamic_filters(base_query, Regions, filter_params, db, reference_models)

            # Параметры Регион
            reverse_region_mapping = {value: key for key, value in region_mapping.items()}
            region = [company.strip() for item in request.args.getlist('region', None) for company in item.split(',')]
            mapped_region = [reverse_region_mapping.get(company, company) for company in region]
            if region:
                base_query = base_query.filter((Regions.id.in_(mapped_region)))

            # Продолжить создавать основной запрос
            query = region_fo_query(base_query, Regions, FedState)
            title = f"Федеральные округа"

            result = []
            for row in query:
                if row.fo not in result:
                    result.append(row.fo)

            graph_data = {
                "title": title,
                "data": result
            }

            #            return jsonify(graph_data)

            response = jsonify(graph_data)
            response.headers.add('Access-Control-Allow-Origin', '*');
            return response

        except Exception as e:
            ns_region_fo_ps.abort(*errorhandler(e))