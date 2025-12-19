from decimal import Decimal
from datetime import datetime
from sqlalchemy import func, select, and_, distinct, or_
from flask import jsonify, session, request
from flask_restx import Namespace, Resource
# Import the database session
from progSpros_back.database_ps import cache, errorhandler
from progSpros_back.functions.query_functions_ps import mapping_otrasl_query
from progSpros_back.functions.utility_functions_ps import set_db_connection
from progSpros_back.model.db_models_ps import reference_models, Otrasl

# Define the namespace
ns_mapping_otr_ps = Namespace('MappingOtrasl', description='Отрасли')


@ns_mapping_otr_ps.route('/Mapping-otrasl')
@ns_mapping_otr_ps.response(200, 'Success')

class FORegionDATA(Resource):
    def get(self):
        """
        Возвращает отсортированные отрасли
        """
        try:
            db = set_db_connection()
            # Определите базовый запрос с помощью динамических фильтров
            base_query = db.query(Otrasl)

            # Продолжить создавать основной запрос
            query = mapping_otrasl_query(base_query, Otrasl)
            title = f"Отрасли"

            result = []
            for row in query:
                if row.name not in result:
                    result_dict ={'id': row.id,
                                  'name': row.name
                             }
                    result.append(result_dict)

            graph_data = {
                "title": title,
                "data": result
            }

            #            return jsonify(graph_data)

            response = jsonify(graph_data)
            response.headers.add('Access-Control-Allow-Origin', '*');
            return response

        except Exception as e:
            ns_mapping_otr_ps.abort(*errorhandler(e))