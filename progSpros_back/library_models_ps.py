from flask_restx import fields, Namespace


def initiate_base_models(ns):
    amount_unit_model = ns.model('AmountUnitModel', {
        'amount': fields.String(required=True, description='Amount value'),
        'unit': fields.String(required=True, description='Unit of the amount')
    })

    category_level_2_model = ns.model('CategoryLevel2Model', {
        'category_level_2': fields.Nested(amount_unit_model, required=True, description='Data category_level_2'),
    })

    data_model_tree = ns.model('DataModelTree', {
        'category_level_1': fields.Nested(category_level_2_model, required=True, description='Data for the category_level_1')
    })

    data_model_plan_fact = ns.model('DataModelPlanFact', {
        'category': fields.String(required=True, description='Category'),
        'fact': fields.String(required=True, description='Fact value for the category'),
        'plan': fields.String(required=True, description='Plan value for the category')
    })

    data_model_gas = ns.model('DataModelGas', {
        'category': fields.String(required=True, description='Category'),
        'condensate': fields.String(required=True, description='Condensate value for the category'),
        'gas': fields.String(required=True, description='Gas value for the category'),
        'oil': fields.String(required=True, description='Oil value for the category')
    })

    return data_model_tree, data_model_plan_fact, data_model_gas

def create_year_grapth_model(ns, data_model, chart_data):

    year_grapth_model = ns.model(f'{chart_data}', {
        'title': fields.String(required=True, description='Title of the graph'),
        'unit': fields.String(required=True, description='Unit of the graph'),
        'description': fields.String(required=False, description='Description of the graph'),
        'data': fields.List(fields.Nested(data_model), required=True, description='Data for the graph')
    })

    return year_grapth_model

ns_mod = Namespace('Модели', description='модели')

# Define model for Swagger documentation
data_model_tree, data_model_plan_fact, data_model_gas = initiate_base_models(ns_mod)

year_grapth_model = create_year_grapth_model(ns_mod, data_model_plan_fact, 'YearGrapthModel')
tree_model = create_year_grapth_model(ns_mod, data_model_tree, 'TreeGrapthModel')
gas_model = create_year_grapth_model(ns_mod, data_model_gas, 'GasGrapthModel')

# Define the model
productivity_model = ns_mod.model('Productivity', {
    'image': fields.String(description='Image URL', nullable=True),
    'percentage': fields.Nested(ns_mod.model('Percentage', {
        'data': fields.String(required=True, description='Percentage data'),
        'description': fields.String(required=True, description='Description of the percentage')
    })),
    'share': fields.Nested(ns_mod.model('Share', {
        'data': fields.String(required=True, description='Share data'),
        'description': fields.String(required=True, description='Description of the share')
    })),
    'title': fields.String(required=True, description='Title of the report')
})

# Define model for error responses
error_model = ns_mod.model('ErrorModel', {
    'status_code': fields.Integer(required=True, description='HTTP status code of the error'),
    'message': fields.String(required=True, description='Error message explaining the cause of the error')
})