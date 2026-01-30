from decimal import Decimal
from datetime import datetime
from sqlalchemy import func, select, and_, distinct, or_
from flask import jsonify, session, request
from flask_restx import Namespace, Resource


# Import the database session
from progSpros_back.database_ps import set_db_connection, cache, errorhandler
from progSpros_back.functions.query_functions_ps import mapping_otrasl_query, mapping_query
# from progSpros_back.functions.utility_functions_ps import set_db_connection, mapping
from progSpros_back.model.db_models_ps import reference_models, GroupPost



# Define the namespace
ns_group_post_ps = Namespace('MappingGroupPost', description='Группа поставщиков')


@ns_group_post_ps.route('/Mapping-group-post')
@ns_group_post_ps.response(200, 'Success')

class GrPostDATA(Resource):
    def get(self):
        """
        Возвращает группы поставщиков
        """
        try:
            db = set_db_connection()
            
            # Определите базовый запрос с помощью динамических фильтров
            base_query = db.query(GroupPost)

            # Продолжить создавать основной запрос
            query = mapping_query(base_query, GroupPost)
            title = f"Группа поставщиков"

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
            ns_group_post_ps.abort(*errorhandler(e))