from collections import defaultdict
from collections import OrderedDict
from decimal import Decimal
from datetime import datetime
from sqlalchemy import func, select, and_, distinct, or_, case, delete, update
from flask import jsonify, session, request
from flask_restx import fields, Namespace, Resource, reqparse


# Import the database session
from progSpros_back.database_ps import cache, errorhandler, set_db_connection#, db
from progSpros_back.functions.chart_data_functions_ps import apply_dynamic_filters
# from progSpros_back.functions.query_functions_ps import fo_region_query
from progSpros_back.functions.utility_functions_ps import create_filter_params#, set_db_connection, mapping
from progSpros_back.model.db_models_ps import PSDATA, reference_models, FedState, Regions, group_regions_relation, GroupRegions


# Define the namespace
ns_fo_region_ps = Namespace('FORegion', description='Регионы и Федеральные округа')

region_model = ns_fo_region_ps.model('Region', {
    'name': fields.String(required=True, description='Region name'),
})

group_region_item_model = ns_fo_region_ps.model('GroupRegionItem', {
    'group_id': fields.Integer(required=False, description='Existing group ID (for updates)', default=None),
    'group_name': fields.String(required=True, description='Group name'),
    'regions': fields.List(fields.String, required=True, description='List of region names in this group'),
})

group_regions_request_model = ns_fo_region_ps.model('GroupRegionsRequest', {
    'fo_group': fields.List(fields.Nested(group_region_item_model), 
                           required=True, 
                           description='List of groups with their regions')
})

region_ids_model = ns_fo_region_ps.model('GroupRegionsDeleteRequest', {
    'group_ids': fields.List(fields.Integer, required=True, description='List of ids')
})
# parser = reqparse.RequestParser()
# parser.add_argument('groups_ids', required=True, type=int, help='comma separated list of ids', action='split')


@ns_fo_region_ps.route('/fo-region', methods=['GET', 'POST', 'PUT', 'OPTIONS', 'DELETE'])
@ns_fo_region_ps.response(200, 'Success')
class FORegionDATA(Resource):
    @ns_fo_region_ps.doc(params={
        'fo': {'description': 'Федеральный округ', 'in': 'query', 'type': 'string'}
    })
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
            base_query = db.query(Regions).select_from(Regions)#.join(Regions, ).join(FedState)
            base_query = base_query.join(
                        group_regions_relation, 
                        Regions.id == group_regions_relation.c.id_region,
                        isouter=True
                    ).join(
                        GroupRegions, 
                        group_regions_relation.c.id_group_region == GroupRegions.id,
                        isouter=True
                    )
            fo = [company.strip() for item in request.args.getlist('fo', None) for company in item.split(',')]
            if fo:
                base_query = base_query.join(
                        FedState,
                        Regions.tab_fo_d314_ids == FedState.id,
                        isouter=True
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
                groups_all_in_fo = db.query(
                                    GroupRegions.id.label('group_id')
                                ).select_from(GroupRegions)\
                                .filter(GroupRegions.id.notin_(db.query(groups_with_outside_regions.c.group_id)))\
                                .subquery()
                base_query = base_query.add_columns(
                                case(
                                    (GroupRegions.id.in_(db.query(groups_all_in_fo.c.group_id)), True),
                                    else_=False
                                ).label('group_in_query')
                            )
                # base_query = base_query.filter(GroupRegions.id.notin_(db.query(groups_with_outside_regions.c.group_id)))

            base_query = apply_dynamic_filters(base_query, GroupRegions, filter_params, db, reference_models)
            
            result = base_query.with_entities(GroupRegions.id.label('group_id'), GroupRegions.name.label('group'), Regions.name.label('region')).order_by(
                                                    Regions.name
                                                ).all()
            
            
            regions = OrderedDict()
            groups = defaultdict(list)
            for row in result:
                regions[row.region] = None
                if getattr(row, 'group_in_query', True) and row.group_id:
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

    @ns_fo_region_ps.expect(group_regions_request_model)
    def post(self):
        try:
            
            db = set_db_connection()
            data = request.get_json()
            
            if not data or 'fo_group' not in data:
                return {'error': 'Missing fo_group data'}, 400
            
            groups_data = data['fo_group']

            for group_data in groups_data:
                try:
                    group_id = group_data.get('group_id')
                    group_name = group_data.get('group_name')
                    regions = group_data.get('regions', [])
                    
                    if not group_name or not regions:
                        raise ValueError('Missing group data')
                    
                    regions_ids = [item.id for item in db.query(Regions).filter(Regions.name.in_(regions)
                                                            ).with_entities(Regions.id.label('id')).all()]
                    if len(regions_ids) == 0:
                        raise ValueError('Invalid regions')
                    
                    # Создаём группу (если нет)
                    if not group_id:
                        group_id = db.query(func.max(GroupRegions.id)).scalar()
                        group_id = (group_id or 0) + 1
                        group_region = GroupRegions(id=group_id, name=group_name)
                        db.add(group_region)
                        db.flush()
                    else:
                        delete_stmt = delete(group_regions_relation).where(
                            (group_regions_relation.c.id_group_region == group_id)
                        )
                        db.execute(delete_stmt)
                    
                    for id in regions_ids:
                        association_data = {
                            'id_region': id,
                            'id_group_region': group_id,
                            'date_added': datetime.now()
                        }
                        db.execute(group_regions_relation.insert().values(**association_data))
                    stmt = (
                            update(GroupRegions)
                            .where(GroupRegions.id == group_id)
                            .values(name=group_name)
                        )
                    db.execute(stmt)
                    
                    db.commit()
                except Exception as e:
                    db.rollback()
                    raise e
            
                group_id = None
                regions_ids = []

        except Exception as e:
            ns_fo_region_ps.abort(*errorhandler(e))
        return 200
    
    @ns_fo_region_ps.expect(region_ids_model)
    def delete(self):
        try:
            db = set_db_connection()
            # data = request.get_json()
            
            # if not data or 'fo_group' not in data:
            #     return {'error': 'Missing fo_group data'}, 400
            args = request.get_json()
            groups_ids = args['group_ids']
            # groups_ids = data['fo_group']

            try:
                delete_stmt = delete(group_regions_relation).where(
                            (group_regions_relation.c.id_group_region.in_(groups_ids))
                        )
                db.execute(delete_stmt)
                delete_stmt = delete(GroupRegions).where(
                            (GroupRegions.id.in_(groups_ids))
                        )
                db.execute(delete_stmt)
                db.commit()

            except Exception as e:
                db.rollback()
                raise e
        except Exception as e:
            ns_fo_region_ps.abort(*errorhandler(e))
        return 200

    def options(self):
        origin = request.headers.get('Origin')
        return {'Allow': 'OPTIONS, POST, GET, DELETE'}, 200, {
            'Access-Control-Allow-Origin': origin,
            'Access-Control-Allow-Methods': 'POST, OPTIONS, GET, DELETE',
            'Access-Control-Allow-Headers': 'Content-Type, Authorization'
        }