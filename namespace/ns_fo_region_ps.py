from collections import defaultdict
from collections import OrderedDict
from decimal import Decimal
from datetime import datetime
from sqlalchemy import func, select, and_, distinct, or_
from flask import jsonify, session, request
from flask_restx import Namespace, Resource

# relimport

from database_ps import db, cache, errorhandler
from functions.chart_data_functions_ps import apply_dynamic_filters
# from functions.query_functions_ps import fo_region_query, fo_group_query
from functions.utility_functions_ps import create_filter_params, set_db_connection, mapping
from model.db_models_ps import PSDATA, reference_models, FedState, Regions, GroupRegions, group_regions_relation
# relimport


# absimport
# Import the database session
''' # absimport
from progSpros_back.database_ps import db, cache, errorhandler
from progSpros_back.functions.chart_data_functions_ps import apply_dynamic_filters
# from progSpros_back.functions.query_functions_ps import fo_region_query
from progSpros_back.functions.utility_functions_ps import create_filter_params, set_db_connection, mapping
from progSpros_back.model.db_models_ps import PSDATA, reference_models, FedState, Regions
''' # absimport

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
            # Получить фильтр-параметры из запроса
            filter_params = create_filter_params(request)

            # Если не заданы глобальные параметры, взять их из session
            if not filter_params:
                filter_params = session.get('filter_params')

            # Определите базовый запрос с помощью динамических фильтров
            base_query = db.query(GroupRegions).select_from(GroupRegions)#.join(Regions, ).join(FedState)
            base_query = base_query.join(
                        group_regions_relation, 
                        GroupRegions.id == group_regions_relation.c.id_group_region
                    ).join(
                        Regions, 
                        group_regions_relation.c.id_region == Regions.id
                    )
            fo = [company.strip() for item in request.args.getlist('fo', None) for company in item.split(',')]
            if fo:
                base_query = base_query.join(
                        FedState,
                        Regions.tab_fo_d314_ids == FedState.id
                    ).filter((FedState.name.in_(fo)))
                
                groups_with_outside_regions = db.query(
                        GroupRegions.id.label('group_id')
                    ).select_from(GroupRegions)\
                    .join(
                        group_regions_relation, 
                        GroupRegions.id == group_regions_relation.c.id_group_region
                    ).join(
                        Regions, 
                        group_regions_relation.c.id_region == Regions.id
                    ).join(
                        FedState,
                        Regions.tab_fo_d314_ids == FedState.id
                    )\
                    .filter(~FedState.name.in_(fo))\
                    .subquery()
                base_query = base_query.filter(GroupRegions.id.notin_(db.query(groups_with_outside_regions.c.group_id)))

            base_query = apply_dynamic_filters(base_query, GroupRegions, filter_params, db, reference_models)
            
            result = base_query.with_entities(GroupRegions.id.label('group_id'), GroupRegions.name.label('group'), Regions.name.label('region')).order_by(
                                                    Regions.name
                                                ).all()
            
            
            regions = OrderedDict()
            groups = defaultdict(list)
            for row in result:
                regions[row.region] = None
                groups[(row.group_id, row.group)]
                groups[(row.group_id, row.group)].append(row.region)
            
            # Параметры Федеральный округ
            '''reverse_fo_mapping = {value: key for key, value in fo_mapping.items()}
            fo = [company.strip() for item in request.args.getlist('fo', None) for company in item.split(',')]
            mapped_fo = [reverse_fo_mapping.get(company, company) for company in fo]
            if fo:
                base_query = base_query.filter((Regions.tab_fo_d314_ids.in_(mapped_fo)))

            # Продолжить создавать основной запрос
            query = fo_regions_query(base_query, Regions, FedState)
            #title = f"Регионы"
            print(query)
            regions = set()
            # groups = {}
            for row in query:
                regions.add(row.region)
                # groups[]
            '''
            graph_data = {
                #"title": title,
                "regions": list(regions.keys()),
                "fo_group": [{'group_id': id, "group_name": name, "regions": value} for (id, name), value in groups.items()]
            }

            #            return jsonify(graph_data)

            response = jsonify(graph_data)
            response.headers.add('Access-Control-Allow-Origin', '*');
            return response

        except Exception as e:
            ns_fo_region_ps.abort(*errorhandler(e))