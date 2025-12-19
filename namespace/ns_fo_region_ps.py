from decimal import Decimal
from datetime import datetime
from sqlalchemy import func, select, and_, distinct, or_
from flask import jsonify, session, request
from flask_restx import Namespace, Resource
# Import the database session
from progSpros_back.database_ps import cache, errorhandler
from progSpros_back.functions.chart_data_functions_ps import apply_dynamic_filters
from progSpros_back.functions.query_functions_ps import fo_region_query
from progSpros_back.functions.utility_functions_ps import create_filter_params, set_db_connection
from progSpros_back.model.db_models_ps import PSDATA, reference_models, FedState, Regions
from progSpros_back.model.mappings_ps import fo_mapping

# Define the namespace
ns_fo_region_ps = Namespace('FORegion', description='Регионы и Федеральные округа')


@ns_fo_region_ps.route('/fo-region')
@ns_fo_region_ps.response(200, 'Success')
@ns_fo_region_ps.doc(params={
    'fo': {'description': 'Федеральный округ', 'in': 'query', 'type': 'string'}
})
class FORegionDATA(Resource):
    def get(self):
        """
        Возвращает регионы в зависимости от выбранного округа
        """
        try:

            db = set_db_connection()
            # Получить фильтр-параметры из запроса
            filter_params = create_filter_params(request)

            # Если не заданы глобальные параметры, взять их из session
            if not filter_params:
                filter_params = session.get('filter_params')

            # Определите базовый запрос с помощью динамических фильтров
            base_query = db.query(Regions)
            base_query = apply_dynamic_filters(base_query, Regions, filter_params, db, reference_models)

            # Параметры Федеральный округ
            reverse_fo_mapping = {value: key for key, value in fo_mapping.items()}
            fo = [company.strip() for item in request.args.getlist('fo', None) for company in item.split(',')]
            mapped_fo = [reverse_fo_mapping.get(company, company) for company in fo]
            if fo:
                base_query = base_query.filter((Regions.tab_fo_d314_ids.in_(mapped_fo)))

            # Продолжить создавать основной запрос
            query = fo_region_query(base_query, Regions, FedState)
            title = f"Регионы"

            result = []
            for row in query:
                if row.region not in result:
                    result.append(row.region)

            graph_data = {
                "title": title,
                "data": result
            }

            #            return jsonify(graph_data)

            response = jsonify(graph_data)
            response.headers.add('Access-Control-Allow-Origin', '*');
            return response

        except Exception as e:
            ns_fo_region_ps.abort(*errorhandler(e))