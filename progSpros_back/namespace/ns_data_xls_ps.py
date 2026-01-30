from io import BytesIO
from flask_restx import Namespace, Resource
from flask import request, send_file
from werkzeug.exceptions import BadRequest
from datetime import datetime

# Import the database session
from progSpros_back.database_ps import errorhandler, set_db_connection
from progSpros_back.model.db_models_ps import PG, PSDATA, FedState, Regions, Contragent, Otrasl, GroupPost, Proizv, Dogovor, TU, Infr, VersProgn, StPotr
from progSpros_back.model.mappings_ps import yn_mapping, tick_mapping
from progSpros_back.functions.query_functions_ps import all_data_upload_query
#from progSpros_back.functions.utility_functions_ps import set_db_connection, db
from progSpros_back.functions.file_upload_functions_ps import ColumnDescriptor, prepare_all_data, build_export_xlsx




ns_data_xls_ps = Namespace('DataXls', description='Выгрузка на дату версии в Excel')


@ns_data_xls_ps.route('/data_xls_ps')
@ns_data_xls_ps.response(200, 'Success')
@ns_data_xls_ps.doc(params={
    'date': {'description': 'Дата загрузки [DD.MM.YYYY]', 'in': 'query', 'type': 'string'}
})



class DataXls(Resource):
    def get(self):
        """
        Возвращает обратно данные для выгрузки в Excel плоских файлов
        Аргументы:
            - принимает аргумент date: str - дату загрузки [DD.MM.YYYY]
        """
        JOIN_COLS = {
            FedState.__tablename__: PSDATA.tab_fo_d314_ids,
            Regions.__tablename__: PSDATA.tab_region_d314_ids,
            Contragent.__tablename__: PSDATA.tab_contragent_d314_ids,
            Otrasl.__tablename__: PSDATA.tab_otrasl_economy_d314_ids,
            StPotr.__tablename__: PSDATA.tab_status_potreb_d314_ids,
            GroupPost.__tablename__: PSDATA.tab_group_post_d314_ids,
            Proizv.__tablename__: PSDATA.tab_proizvoditel_d314_ids,
            Dogovor.__tablename__: PSDATA.tab_dogovor_visual_d314_ids,
            TU.__tablename__: PSDATA.tab_tu_visual_d314_ids,
            Infr.__tablename__: PSDATA.tab_infr_d314_ids,
            VersProgn.__tablename__: PSDATA.tab_ver_real_pr_d314_ids,
            PG.__tablename__: PSDATA.tab_pg_visual_d314_ids,
        }
        
        DATASET = PSDATA
        
        DATA_COLS = {
            # данные по ключам
            'fedstate': ColumnDescriptor(db_column=FedState.short_name, excel_title='Округ'),
            'subject': ColumnDescriptor(db_column=Regions.name, excel_title='Субъект'),
            'contragent': ColumnDescriptor(db_column=Contragent.name, excel_title='Потребитель'),
            'industry': ColumnDescriptor(db_column=Otrasl.name, excel_title='Отрасль'),
            'contragent_status': ColumnDescriptor(db_column=StPotr.name, excel_title='Статус потребителя'),
            'suppliers': ColumnDescriptor(db_column=GroupPost.name, excel_title='Группа поставщиков'),
            'producer': ColumnDescriptor(db_column=Proizv.name, excel_title='Производитель газа'),
            'contract': ColumnDescriptor(db_column=Dogovor.name, excel_title='Договор', mapping=tick_mapping),
            'tu': ColumnDescriptor(db_column=TU.name, excel_title='ТУ', mapping=tick_mapping),
            'infr': ColumnDescriptor(db_column=Infr.name, excel_title='Наличие инф-ры', mapping=tick_mapping),
            'probability': ColumnDescriptor(db_column=VersProgn.full_name, excel_title='Вероятность реализации проекта'),
            'readiness': ColumnDescriptor(db_column=PG.name, excel_title='Ход выполнения работ', mapping=tick_mapping),


            # Данные не по ключам (маппинги)
            'otl_usl': ColumnDescriptor(db_column=PSDATA.otl_usl, mapping=yn_mapping,  excel_title='Отл усл'),
            'takeorpay': ColumnDescriptor(db_column=PSDATA.takeorpay, mapping=yn_mapping, excel_title='take-or-pay'),

            # Данные не по ключам (без маппингов)
            'sum': ColumnDescriptor(db_column=PSDATA.summ, excel_title='Сумма'),
            'gen_schema': ColumnDescriptor(db_column=PSDATA.gen_schema, excel_title='Ген схема', mapping=yn_mapping),
            'tasks': ColumnDescriptor(db_column=PSDATA.poruch, excel_title='Поручения '),
            'year': ColumnDescriptor(db_column=PSDATA.year)
        }

        DATE_FORMAT = '%d.%m.%Y'
        try:
            db = set_db_connection()

            date = request.args.get("date")
            if not date:
                raise ValueError("Обязательный параметр date")
            date = datetime.strptime(date, DATE_FORMAT)
            
            data = db.execute(all_data_upload_query(db=db, base_table=DATASET, columns=DATA_COLS, join_cols_dict=JOIN_COLS).filter(PSDATA.date==date)).mappings().all()
            
            data, years_range = prepare_all_data(titles=DATA_COLS, rows=data)
            
            

            return send_file(
                BytesIO(build_export_xlsx(template_name='Шаблон Прогнозный спроc.xlsx', headers=DATA_COLS, data=data)),
                mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                as_attachment=True,
                download_name="Прогнозный спрос.xlsx",
                max_age=0,
            )
            return 
        except Exception as e:
            ns_data_xls_ps.abort(*errorhandler(e))
        