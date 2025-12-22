import datetime
from flask import jsonify, session, request
from flask_restx import Namespace, Resource
# Import the database session
from progSpros_back.database_ps import db, cache, errorhandler
from progSpros_back.functions.query_functions_ps import year_query, yearto_query
from progSpros_back.functions.utility_functions_ps import set_db_connection
from progSpros_back.model.db_models_ps import PSDATA

# Define the namespace
ns_years_ps = Namespace('Years', description='Годы')


@ns_years_ps.route('/years')
@ns_years_ps.response(200, 'Success')

class YearDATA(Resource):
    def get(self):
        """
        Возвращает регионы в зависимости от выбранного округа
        """
        try:
            #db = set_db_connection()
            # Определите базовый запрос с помощью динамических фильтров
            base_query = db.query(PSDATA)

            # Продолжить создавать основной запрос
            query_years = year_query(base_query, PSDATA)

            title = f"Годы"

            yearto = 2033
            today = datetime.date.today()
            yearfrom = today.year - 1
            q_yearto = yearto_query(base_query, PSDATA).order_by(PSDATA.year.desc()).first()
            for row in q_yearto:
                yearto = int(row)

            result = {'yearfrom': yearfrom,'yearto': yearto, 'years': []}
            for row in query_years:
                if row.year not in result['years']:
                    result['years'].append(row.year)

            graph_data = {
                "title": title,
                "data": result
            }

            #            return jsonify(graph_data)

            response = jsonify(graph_data)
            response.headers.add('Access-Control-Allow-Origin', '*');
            return response

        except Exception as e:
            ns_years_ps.abort(*errorhandler(e))
