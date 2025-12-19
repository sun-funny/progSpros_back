import sys
import os
sys.path.insert(0,'opt/foresight')

# Импорт стандартной библиотеки
import logging  # Ведение журнала для отладки и мониторинга

# Импорт Flask
from flask import Flask, jsonify, request  # Основные классы и функции Flask

# Импорт SQLAlchemy
from sqlalchemy import select, distinct, not_, literal, func, and_  # Основные функции SQLAlchemy
from werkzeug.exceptions import HTTPException, InternalServerError

# Импорты для конкретного проекта
from progSpros_back.model.db_models_ps import Base, PSDATA, reference_models  # Модели баз данных

from progSpros_back.functions.chart_data_functions_ps import apply_dynamic_filters  # Функции отображения данных на графике
from progSpros_back.functions.utility_functions_ps import create_filter_params  # Полезные функции
from progSpros_back.functions.query_functions_ps import otrasl_query, all_data_query  # Функции запроса

from progSpros_back.library_models_ps import ns_mod, year_grapth_model  # Library models

from progSpros_back.config_ps import Config, changelog, secret_key  # Конфигурация и список изменений

from progSpros_back.namespace.ns_rf_ps import ns_rf_ps
from progSpros_back.namespace.ns_otrasl_ps import ns_otrasl_ps
from progSpros_back.namespace.ns_map_otr_ps import ns_map_otr_ps
from progSpros_back.namespace.ns_map_potr_ps import ns_map_potr_ps
from progSpros_back.namespace.ns_fo_region_ps import ns_fo_region_ps
from progSpros_back.namespace.ns_big_invest_ps import ns_big_invest_ps
from progSpros_back.namespace.ns_sankey_ps import ns_sankey_ps
from progSpros_back.namespace.ns_mapping_otr_ps import ns_mapping_otr_ps
from progSpros_back.namespace.ns_big_invest_xls_ps import ns_big_invest_xls_ps
from progSpros_back.namespace.ns_region_fo_ps import ns_region_fo_ps
from progSpros_back.namespace.ns_years_ps import ns_years_ps
from progSpros_back.namespace.ots_pr_spr.ns_ots_pr_spr_pot_ps import ns_ots_pr_spr_ps

# Импорт сеанса работы с базой данных
from progSpros_back.database_ps import db, engine, cache

# Импорт Flask-Restx
from flask_restx import Api, Resource, Namespace  # Классы Flask-Restx для создания API

app = Flask(__name__)
app.config.from_object(Config)
app.secret_key = secret_key


# Настройка API Flask-Restx
api = Api(app,
          version='1.1',
          title='Прогнозный спрос API',
          description=f'API configuration for the Progn_Spros project\n\n{changelog}'
          )

cache.init_app(app)

# Создание таблиц базы данных
with app.app_context():
    Base.metadata.create_all(bind=engine)

# Настройка ведения журнала
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Добавить namespace в API
api.add_namespace(ns_rf_ps,  path='')
api.add_namespace(ns_otrasl_ps,  path='')
api.add_namespace(ns_map_otr_ps,  path='')
api.add_namespace(ns_map_potr_ps,  path='')
api.add_namespace(ns_fo_region_ps,  path='')
api.add_namespace(ns_region_fo_ps,  path='')
api.add_namespace(ns_big_invest_ps,  path='')
api.add_namespace(ns_sankey_ps,  path='')
api.add_namespace(ns_big_invest_xls_ps,  path='')
api.add_namespace(ns_mapping_otr_ps,  path='')
api.add_namespace(ns_years_ps, path='')
api.add_namespace(ns_ots_pr_spr_ps, path='')

if __name__ == '__main__':
    app.run(debug=True)