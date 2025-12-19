from flask import jsonify, session, request, send_from_directory, send_file
from flask_restx import Namespace, Resource
# Import the database session
from progSpros_back.database_ps import cache, errorhandler
from progSpros_back.model.mappings_ps import yn_mapping
#
from progSpros_back.namespace.ots_pr_spr.constants import shown_columns_map
from progSpros_back.namespace.ots_pr_spr.query_builder import get_query
from progSpros_back.namespace.ots_pr_spr.data_processor import get_data
from progSpros_back.namespace.ots_pr_spr.excel_generator import get_data_exl, get_excel
from progSpros_back.namespace.ots_pr_spr.maping import reverse_replace

# Define the namespace
ns_ots_pr_spr_ps = Namespace('OtsPrSpr', description='Оценка прогнозного спроса на газ потребителей')

@ns_ots_pr_spr_ps.route('/columns')
@ns_ots_pr_spr_ps.response(200, 'Success')

class Columns(Resource):
    def get(self):
        """
        Возвращает колонки
        """
        try:

            result = list(shown_columns_map.values())

            title = "Колонки"

            graph_data = {
                "title": title,
                "data": result
            }

            response = jsonify(graph_data)
            response.headers.add('Access-Control-Allow-Origin', '*');
            return response

        except Exception as e:
            ns_ots_pr_spr_ps.abort(*errorhandler(e))


@ns_ots_pr_spr_ps.route('/ots_pr_spr_pot_ps')
@ns_ots_pr_spr_ps.response(200, 'Success')
@ns_ots_pr_spr_ps.doc(params={
    'yearfrom': {'description': 'Год с', 'in': 'query', 'type': 'integer'},
    'yearto': {'description': 'Год по', 'in': 'query', 'type': 'integer'},
    'sum_pr': {'description': 'Прирост с', 'in': 'query', 'type': 'integer'},
    'fo': {'description': 'Федеральный округ', 'in': 'query', 'type': 'string'},
    'region': {'description': 'Регион', 'in': 'query', 'type': 'string'},
    'otrasl': {'description': 'Отрасль', 'in': 'query', 'type': 'string'},
    'vers': {'description': 'Версия прогноза', 'in': 'query', 'type': 'string'},
    'grpost': {'description': 'Группа поставщиков', 'in': 'query', 'type': 'string'},
    'dogovor': {'description': 'Договор', 'in': 'query', 'type': 'string'},
    'tu': {'description': 'ТУ', 'in': 'query', 'type': 'string'},
    'shown_columns': {'description': 'Колонки', 'in': 'query', 'type': 'string'},
    'otrasl_total': {'description': 'Отрасли в итогах', 'in': 'query', 'type': 'string'},
    'infr': {'description': 'Инфраструктура', 'in': 'query', 'type': 'string'}
    })
class OtsPrSpr(Resource):
    def get(self):
        """
        Возвращает данные для отчета
        """
        try:

            title = f"Оценка прогнозного спроса на газ потребителей"

            # default
            # year
            yearfrom = request.args.get('yearfrom', 2024, type=int)
            yearto = request.args.get('yearto', 2036, type=int)
            sum_pr = request.args.get('sum_pr', None, type=float)

            # Колонки
            shown_columns = [company.strip() for item in request.args.getlist('shown_columns', None) for company in item.split(',')]
            if not len(shown_columns):
                shown_columns = ['otrasl', 'dogovor', 'tu']
            shown_columns = reverse_replace(shown_columns, shown_columns_map)

            # Отрасли в итогах
            otrasl_total = [company.strip() for item in request.args.getlist('otrasl_total', None) for company in
                             item.split(',')]

            query = get_query(request, yearfrom, yearto)

            result = get_data(query, shown_columns, otrasl_total, yearfrom, yearto, sum_pr)

            graph_data = {
                "title": title,
                "data": result
            }

            response = jsonify(graph_data)
            response.headers.add('Access-Control-Allow-Origin', '*')

            return response

        except Exception as e:
             ns_ots_pr_spr_ps.abort(*errorhandler(e))


@ns_ots_pr_spr_ps.route('/ots_pr_spr_pot_ps_xls')
@ns_ots_pr_spr_ps.response(200, 'Success')
@ns_ots_pr_spr_ps.doc(params={
    'yearfrom': {'description': 'Год с', 'in': 'query', 'type': 'integer'},
    'yearto': {'description': 'Год по', 'in': 'query', 'type': 'integer'},
    'sum_pr': {'description': 'Прирост с', 'in': 'query', 'type': 'integer'},
    'fo': {'description': 'Федеральный округ', 'in': 'query', 'type': 'string'},
    'region': {'description': 'Регион', 'in': 'query', 'type': 'string'},
    'otrasl': {'description': 'Отрасль', 'in': 'query', 'type': 'string'},
    'vers': {'description': 'Версия прогноза', 'in': 'query', 'type': 'string'},
    'grpost': {'description': 'Группа поставщиков', 'in': 'query', 'type': 'string'},
    'dogovor': {'description': 'Договор', 'in': 'query', 'type': 'string'},
    'tu': {'description': 'ТУ', 'in': 'query', 'type': 'string'},
    'shown_columns': {'description': 'Колонки', 'in': 'query', 'type': 'string'},
    'otrasl_total': {'description': 'Отрасли в итогах', 'in': 'query', 'type': 'string'},
    'infr': {'description': 'Инфраструктура', 'in': 'query', 'type': 'string'},
    })

class OtsPrSprXls(Resource):
    def get(self):
        """
        Возвращает Excel
        """
        try:

            yearfrom = request.args.get('yearfrom', 2024, type=int)
            yearto = request.args.get('yearto', 2036, type=int)
            fo_params = [fo.strip() for item in request.args.getlist('fo') for fo in item.split(',') if fo.strip()]
            otrasl_total = request.args.get('otrasl_total', 'Население', type=str)
            sum_pr = request.args.get('sum_pr', None, type=float)
            # Колонки
            shown_columns = [company.strip() for item in request.args.getlist('shown_columns', None) for company in item.split(',')]

            if not len(shown_columns):
                shown_columns = ['otrasl', 'dogovor', 'tu']
            shown_columns = reverse_replace(shown_columns, shown_columns_map)

            query = get_query(request, yearfrom, yearto)

            result = get_data_exl(query, shown_columns, yearfrom, yearto, otrasl_total, sum_pr)

            name, basic_path = get_excel(result, yearfrom, yearto, fo_params)

            # Передать файл Excel выгруженный на сервер
            response = send_from_directory(directory=f"{basic_path}", path=name, as_attachment=True)
            response.headers.add('Access-Control-Allow-Origin', '*')

            return response

        except Exception as e:
             ns_ots_pr_spr_ps.abort(*errorhandler(e))