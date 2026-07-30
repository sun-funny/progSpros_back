from copy import copy
from io import BytesIO
from typing import List
from docx import Document
from docx.enum.text import WD_BREAK
from flask_restx import Namespace, Resource, reqparse
from flask import request, send_file
from werkzeug.exceptions import BadRequest
from datetime import datetime
from sqlalchemy import DateTime, and_, case, func, RowMapping, cast, Float, or_, Text
from sqlalchemy.orm import scoped_session

# Import the database session
from progSpros_back.database_ps import errorhandler, set_db_connection
from progSpros_back.functions.data_utils_functions import get_region_docx_dict, get_summary_docx_dict
from progSpros_back.functions.utility_functions_ps import add_region_detalization, combine_note_data_sums
from progSpros_back.model.db_models_ps import PG, PSDATA, FedState, Regions, Contragent, Otrasl, GroupPost, Proizv, Dogovor, TU, Infr, VersProgn, StPotr, StGaz, GenSchema
from progSpros_back.model.mappings_ps import yn_mapping, tick_mapping, vers_mapping
from progSpros_back.functions.query_functions_ps import create_simple_query, get_note_query, get_version_info_cte
from progSpros_back.functions.file_upload_functions_ps import CaseDescriptor, ColumnDescriptor, DocxBuilder, ExcelBuilder, TableDescriptor, prepare_all_data#!, build_export_xlsx




ns_data_xls_ps = Namespace('DataXls', description='Выгрузка на дату версии в Excel')


class DatasetInfoMixin(Resource):
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
            StGaz.__tablename__: PSDATA.tab_start_gaz_d314_ids,
            GenSchema.__tablename__: PSDATA.gen_schema

        }
        
    DATASET = PSDATA

    DATE_FORMAT = '%d.%m.%Y'

    @classmethod
    def get_date(cls, date: str) -> datetime:
        return datetime.strptime(date, cls.DATE_FORMAT)
    
    VALUES_MAPPING = {
        'V': True,
        'X': False
    }

@ns_data_xls_ps.route('/data_xls_ps')
@ns_data_xls_ps.response(200, 'Success')
@ns_data_xls_ps.doc(params={
    'date': {'description': 'Дата загрузки [DD.MM.YYYY]', 'in': 'query', 'type': 'string'}
})
class DataXls(DatasetInfoMixin):
    def get(self):
        """
        Возвращает обратно данные для выгрузки в Excel плоских файлов
        Аргументы:
            - принимает аргумент date: str - дату загрузки [DD.MM.YYYY]
        """
        
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

        try:
            db = set_db_connection()

            date = request.args.get("date")
            if not date:
                raise ValueError("Обязательный параметр date")
            date = self.get_date(date)
            
            data = db.execute(create_simple_query(db=db, base_table=self.DATASET, columns=DATA_COLS, join_cols_dict=self.JOIN_COLS, isouter=True).filter(PSDATA.date==date)).mappings().all()
            
            data, years_range = prepare_all_data(titles=DATA_COLS, rows=data)
            
            TABLE = TableDescriptor(list_name='Таблица', data=data, main_cols=DATA_COLS)
            f = ExcelBuilder().build_export_xlsx('Шаблон Прогнозный спроc.xlsx', TABLE)
            
            return send_file(
                BytesIO(f),
                mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                as_attachment=True,
                download_name="Прогнозный спрос.xlsx",
                max_age=0,
            )
        except Exception as e:
            ns_data_xls_ps.abort(*errorhandler(e))
        

parser = reqparse.RequestParser()
parser.add_argument('year1', type=int, help='Год начало', required=True)
parser.add_argument('year2', type=int, help='Год конец', required=True)
parser.add_argument('vers', type=str, help='Версия прогноза')
parser.add_argument('date1', type=str, help='Дата выгрузки (для начала)', required=True)
parser.add_argument('date2', type=str, help='Дата выгрузки (для конца)', required=True)
parser.add_argument('fo', type=str, help='Федеральный округ')
parser.add_argument('region', type=str, help='Регион')


@ns_data_xls_ps.route('/comparison_data_xls_ps')
@ns_data_xls_ps.response(200, 'Success')
@ns_data_xls_ps.expect(parser)
class СomparisonDataXls(DatasetInfoMixin):
    def get(self):
        """
        Возвращает обратно данные для выгрузки в Excel сравнительных таблиц
        Аргументы:
            - year1: str - год начало
            - year2: str - год конец
            - date1 - дата выгрузки (для даты начала) [DD.MM.YYYY]
            - date2 - дата выгрузки (для даты конца) [DD.MM.YYYY]
            - fo: str - [опционально] федеральный округ
            - region: str - [опционально] регион
            - vers: str - [опционально] вероятность
        """

        try:
            db : scoped_session = set_db_connection()

            args = parser.parse_args()
            
            year1 = args['year1']
            year2 = args['year2']

            upload_date1 = args.get('date1', None)
            if upload_date1:
                upload_date1 = datetime.strptime(upload_date1, '%d.%m.%Y').date()
            upload_date2 = args.get('date2', None)
            if upload_date2:
                upload_date2 = datetime.strptime(upload_date2, '%d.%m.%Y').date()

            if ((vers := args.pop('vers', None)) is not None):
                vers = {value: key for key, value in vers_mapping.items()}[vers]
                vers = db.query(VersProgn.name).filter(VersProgn.id == vers).one_or_none()[0]
                
            TABLE_1_OPTIONAL_COLS = {
                'field': ColumnDescriptor(db_column=Otrasl.name, excel_title='Отрасль'), # Отрасль
                'status': ColumnDescriptor(db_column=StPotr.name, excel_title='Статус потребителя'), # Статус потребителя
                'group': ColumnDescriptor(db_column=GroupPost.name, excel_title='Группа поставщиков'), # Группа поставщиков
                'factory': ColumnDescriptor(db_column=Proizv.name, excel_title='Производитель газа'), # Производитель газа
                'contract': ColumnDescriptor(db_column=Dogovor.name, excel_title='Договор', mapping=tick_mapping), # Договор
                'otl': ColumnDescriptor(db_column=self.DATASET.otl_usl, excel_title='Отл усл', mapping=yn_mapping), # Отл усл
                'takeorpay': ColumnDescriptor(db_column=self.DATASET.takeorpay, excel_title='take-or-pay', mapping=yn_mapping), # take-or-pay
                'tu': ColumnDescriptor(db_column=TU.name, excel_title='ТУ', mapping=tick_mapping), # ТУ
                'infr': ColumnDescriptor(db_column=Infr.name, excel_title='Наличие инф-ры', mapping=tick_mapping), # Наличие инф-ры
                'gen_scheme': ColumnDescriptor(db_column=GenSchema.name, excel_title='Ген схема'), # Ген схема
                'tasks': ColumnDescriptor(db_column=self.DATASET.poruch, excel_title='Поручения'), # Поручения
                'work_status': ColumnDescriptor(db_column=PG.name, excel_title='Ход выполнения работ', mapping=tick_mapping), # Ход выполнения работ
                'select_start': ColumnDescriptor(db_column=StGaz.name, excel_title='Начало отбора'), # Начало отбора
            }

            # В Таблице 1 (и только в ней) запись идентифицируется потребителем
            # ПЛЮС всеми этими атрибутами разом: один потребитель с двумя разными
            # наборами значений (напр. двумя отраслями) даёт две отдельных строки.
            # Таблица 2 остаётся на уровне (fo, region, potr).
            RECORD_ID_COLS = list(TABLE_1_OPTIONAL_COLS.keys())
            record_id_db_columns = [TABLE_1_OPTIONAL_COLS[id].db_column for id in RECORD_ID_COLS]
            record_id_mappings = [TABLE_1_OPTIONAL_COLS[id].mapping for id in RECORD_ID_COLS]

            # Узкая пара CTE только для определения "потребитель целиком новый"
            # (появился на date2, отсутствовал на date1) - используется для
            # маркера "*" во второй таблицах.
            NARROW_ID_DATE_CTE_DATA_COLS = {
                'fo': ColumnDescriptor(db_column=FedState.name),
                'region': ColumnDescriptor(db_column=Regions.name),
                'potr': ColumnDescriptor(db_column=Contragent.name),
            }
            # полный id для таблицы 1
            WIDE_ID_DATE_CTE_DATA_COLS = copy(NARROW_ID_DATE_CTE_DATA_COLS)
            WIDE_ID_DATE_CTE_DATA_COLS.update(TABLE_1_OPTIONAL_COLS)

            date1_cte = create_simple_query(db=db, base_table=self.DATASET, columns=WIDE_ID_DATE_CTE_DATA_COLS, join_cols_dict=self.JOIN_COLS, distinct=False, isouter=False).filter(self.DATASET.date==upload_date1).distinct(FedState.name, Regions.name, Contragent.name).cte('date1_data')
            date2_cte = create_simple_query(db=db, base_table=self.DATASET, columns=WIDE_ID_DATE_CTE_DATA_COLS, join_cols_dict=self.JOIN_COLS, distinct=False, isouter=False).filter(self.DATASET.date==upload_date2).distinct(FedState.name, Regions.name, Contragent.name).cte('date2_data')

            version_info_cte = get_version_info_cte(db=db, date1_cte=date1_cte, date2_cte=date2_cte, id_cols=WIDE_ID_DATE_CTE_DATA_COLS)

            # vers может меняться из года в год для одного и того же объекта, поэтому
            # сравнивается погодично, на уровне (fo, region, potr, <13 атрибутов>, year),
            # а не одним произвольно выбранным значением на потребителя.
            WIDE_YEARLY_DATA_CTE_DATA_COLS = {
                'fo': ColumnDescriptor(db_column=FedState.name),
                'region': ColumnDescriptor(db_column=Regions.name),
                'year': ColumnDescriptor(db_column=self.DATASET.year),
                'summ': ColumnDescriptor(db_column=self.DATASET.summ,
                                         aggr_func = lambda col: func.sum(case(
                                             (self.DATASET.date==upload_date1, -cast(col, Float)),
                                             (self.DATASET.date==upload_date2, cast(col, Float)),
                                             else_=None
                                         ))
                                         ),
                'potr': ColumnDescriptor(db_column=Contragent.name),
                'vers_date1': ColumnDescriptor(db_column=VersProgn.name,
                                         aggr_func = lambda col: func.max(case((self.DATASET.date==upload_date1, col), else_=None))
                                         ),
                'vers_date2': ColumnDescriptor(
                                         case_desc=CaseDescriptor(sql_case=func.max(case((self.DATASET.date==upload_date2, VersProgn.name), else_=None)))
                                         ),
                **{id: ColumnDescriptor(db_column=col, mapping=mapping) for id, col, mapping in zip(RECORD_ID_COLS, record_id_db_columns, record_id_mappings)},
            }
            wide_yearly_data_cte = create_simple_query(db=db, base_table=self.DATASET, columns=WIDE_YEARLY_DATA_CTE_DATA_COLS, join_cols_dict=self.JOIN_COLS, distinct=False, isouter=False)
            wide_yearly_data_cte = wide_yearly_data_cte.group_by(FedState.name, Regions.name, Contragent.name, self.DATASET.year, *record_id_db_columns)
            wide_yearly_data_cte = wide_yearly_data_cte.cte('wide_yearly_data')

            # "vers изменилась" - если хотя бы в одном году значение отличается
            # между датами; используется и для Таблицы 1 (по объекту), и для
            # Таблицы 2 (агрегируется дальше, по потребителю).
            vers_diff = and_(
                # wide_yearly_data_cte.c.vers_date1.isnot(None),
                # wide_yearly_data_cte.c.vers_date2.isnot(None),
                wide_yearly_data_cte.c.vers_date1 != wide_yearly_data_cte.c.vers_date2
            )
            vers_changed_expr = func.bool_or(vers_diff)
            vers_case_desc = CaseDescriptor(sql_case=case(
                (vers_changed_expr, func.concat(
                    'изменение значения с ',
                    func.min(case((vers_diff, wide_yearly_data_cte.c.vers_date1), else_=None)),
                    ' на ',
                    func.min(case((vers_diff, wide_yearly_data_cte.c.vers_date2), else_=None))
                )),
                else_=cast(func.min(func.coalesce(wide_yearly_data_cte.c.vers_date1, wide_yearly_data_cte.c.vers_date2)), Text)
            ))

            # Таблица 2: id = (fo, region, potr)
            TABLE_2_DATA_COLS = {
                'fo': ColumnDescriptor(db_column=wide_yearly_data_cte.c.fo, excel_title='Федеральный округ', is_filter=True),
                'region': ColumnDescriptor(db_column=wide_yearly_data_cte.c.region, excel_title='Регион', is_filter=True),
                'potr': ColumnDescriptor(
                    aggr_func=func.max,
                    case_desc=CaseDescriptor(sql_case=case(
                        (version_info_cte.c.potr_is_new.is_(True), func.concat('*', wide_yearly_data_cte.c.potr)),
                        else_=wide_yearly_data_cte.c.potr
                    )),
                    excel_title='Потребитель'),
            }
            TABLE_2_YEARS_COLS = {}
            for year in range(year1, year2+1):
                TABLE_2_YEARS_COLS[str(year)] = ColumnDescriptor(
                                    case_desc=CaseDescriptor(sql_case = func.sum(case((wide_yearly_data_cte.c.year == year, wide_yearly_data_cte.c.summ), else_=None))),
                                    excel_title=f'{year}',
                                    template_col_id='year'
                                  )
            TABLE_2_DATA_COLS.update(TABLE_2_YEARS_COLS)
            TABLE_2_DATA_COLS['vers'] = ColumnDescriptor(excel_title='Вероятность реализации проекта', is_filter=True, case_desc=vers_case_desc)

            # Таблица 1: id = (fo, region, potr, <все опциональные>)
            TABLE_1_DATA_COLS = {
                'fo': ColumnDescriptor(db_column=wide_yearly_data_cte.c.fo, excel_title='Федеральный округ', is_filter=True),
                'region': ColumnDescriptor(db_column=wide_yearly_data_cte.c.region, excel_title='Регион', is_filter=True),
                'potr': ColumnDescriptor(
                    aggr_func=func.max,
                    case_desc=CaseDescriptor(sql_case=case(
                        # Помечаем "*" (и подсвечиваем всю строку) только когда сам
                        # потребитель целиком новый - не когда просто появилась новая
                        # запись (комбинация атрибутов) у уже существующего потребителя.
                        (version_info_cte.c.potr_is_new.is_(True), func.concat('*', wide_yearly_data_cte.c.potr)),
                        else_=wide_yearly_data_cte.c.potr
                    )),
                    excel_title='Потребитель'),
            }

            # объёмы по годам
            TABLE_1_YEARS_COLS = {}
            for year in range(year1, year2+1):
                year_col_desc = ColumnDescriptor(
                                    aggr_func=func.max,
                                    case_desc=CaseDescriptor(sql_case = case((wide_yearly_data_cte.c.year == year, wide_yearly_data_cte.c.summ), else_=None)),
                                    excel_title=f'{year}',
                                    template_col_id='year'
                                  )
                TABLE_1_YEARS_COLS[str(year)] = year_col_desc

            for id in RECORD_ID_COLS:
                TABLE_1_OPTIONAL_COLS[id] = ColumnDescriptor(
                    aggr_func=func.max,
                    db_column=getattr(wide_yearly_data_cte.c, id),
                    excel_title=TABLE_1_OPTIONAL_COLS[id].excel_title,
                )

            TABLE_1_DATA_COLS.update(TABLE_1_YEARS_COLS)
            TABLE_1_DATA_COLS['vers'] = ColumnDescriptor(excel_title='Вероятность реализации проекта', is_filter=True, case_desc=vers_case_desc)

            ALL_TABLE_1_COLS = copy(TABLE_1_DATA_COLS)
            ALL_TABLE_1_COLS.update(TABLE_1_OPTIONAL_COLS)
            
            data_1_query = create_simple_query(db=db, base_table=self.DATASET, columns=ALL_TABLE_1_COLS, 
                                               join_cols_dict=self.JOIN_COLS, isouter=True, 
                                               select_from=wide_yearly_data_cte)
            
            data_2_query = create_simple_query(db=db, base_table=self.DATASET, columns=TABLE_2_DATA_COLS, 
                                               join_cols_dict=self.JOIN_COLS, isouter=True, 
                                               select_from=wide_yearly_data_cte)
            
            data_1_query = data_1_query.join(version_info_cte,
                            and_(wide_yearly_data_cte.c.fo == version_info_cte.c.fo, 
                                wide_yearly_data_cte.c.region == version_info_cte.c.region, 
                                wide_yearly_data_cte.c.potr == version_info_cte.c.potr))

            data_2_query = data_2_query.join(version_info_cte, 
                            and_(wide_yearly_data_cte.c.fo == version_info_cte.c.fo, 
                                wide_yearly_data_cte.c.region == version_info_cte.c.region, 
                                wide_yearly_data_cte.c.potr == version_info_cte.c.potr))
            
            data_filters_common = []
            if vers is not None:
                data_filters_common.append(or_(wide_yearly_data_cte.c.vers_date1 == vers, wide_yearly_data_cte.c.vers_date2 == vers))

            data_filters_1 = list(data_filters_common)
            data_filters_2 = list(data_filters_common)
            for id, col in ALL_TABLE_1_COLS.items():
                if col.is_filter and id in args and (arg:=args[id]):
                    data_filters_1.append(col.db_column == arg)
            for id, col in TABLE_2_DATA_COLS.items():
                if col.is_filter and id in args and (arg:=args[id]):
                    data_filters_2.append(col.db_column == arg)

            data_1_query = data_1_query.filter(and_(*data_filters_1))
            data_2_query = data_2_query.filter(and_(*data_filters_2))
            
            data_1_query = data_1_query.group_by(wide_yearly_data_cte.c.fo,
                                                wide_yearly_data_cte.c.region,
                                                wide_yearly_data_cte.c.potr,
                                                *[getattr(wide_yearly_data_cte.c, id) for id in RECORD_ID_COLS])
            data_2_query = data_2_query.group_by(wide_yearly_data_cte.c.fo,
                                                wide_yearly_data_cte.c.region,
                                                wide_yearly_data_cte.c.potr)

            # Запись попадает в Таблицу 1, если хотя бы за один год (в диапазоне
            # year1..year2) объём отличается между датами, либо изменилась vers
            # (в любом из годов).
            data_1_query = data_1_query.having(or_(
                func.bool_or(and_(
                    wide_yearly_data_cte.c.year >= year1,
                    wide_yearly_data_cte.c.year <= year2,
                    wide_yearly_data_cte.c.summ.isnot(None),
                    wide_yearly_data_cte.c.summ != 0
                )),
                vers_changed_expr
            ))
            # Таблица 2: потребитель попадает в неё, если vers изменилась хотя бы
            # у одного его объекта в любом году.
            data_2_query = data_2_query.having(vers_changed_expr)

            data_1_query = data_1_query.order_by(
                            wide_yearly_data_cte.c.fo,
                            wide_yearly_data_cte.c.region,
                            wide_yearly_data_cte.c.potr,
                            *[getattr(wide_yearly_data_cte.c, id) for id in RECORD_ID_COLS]
                        )
            data_2_query = data_2_query.order_by(
                            wide_yearly_data_cte.c.fo,
                            wide_yearly_data_cte.c.region,
                            wide_yearly_data_cte.c.potr
                        )
            data1 : List[RowMapping] = db.execute(data_1_query).mappings().all()
            data2 : List[RowMapping] = db.execute(data_2_query).mappings().all()
            changes_pattern = '^изменение значения с .+ на .+$'

            TABLE_1 = TableDescriptor(list_name='Таблица 1',
                                      data=data1,
                                      main_cols=TABLE_1_DATA_COLS, optional_cols=TABLE_1_OPTIONAL_COLS,
                                      highlight_pattern=changes_pattern, highlighted_cols=['vers'],
                                      row_highlight_col='potr', row_highlight_pattern=r'^\*',
                                      groupings_headers_height=1)
            TABLE_2 = TableDescriptor(list_name='Таблица 2',
                                      data=data2,
                                      main_cols=TABLE_2_DATA_COLS,
                                      highlight_pattern=changes_pattern, highlighted_cols=['vers'],
                                      row_highlight_col='potr', row_highlight_pattern=r'^\*',
                                      groupings_headers_height=1)
            


            f = ExcelBuilder().build_export_xlsx('Сравнительные таблицы.xlsx', TABLE_1, TABLE_2)


            return send_file(
                BytesIO(f),
                mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                as_attachment=True,
                download_name="Сравнительные таблицы.xlsx",
                max_age=0,
            )
        except Exception as e:
            ns_data_xls_ps.abort(*errorhandler(e))
        

note_parser = reqparse.RequestParser()
note_parser.add_argument('start_year', type=int, help='Год начало', required=True)
note_parser.add_argument('end_year', type=int, help='Год конец', required=True)
note_parser.add_argument('fo', type=str, help='Федеральный округ')
note_parser.add_argument('regions', type=str, help='Регионы, разделённые запятыми')
note_parser.add_argument('industry', type=str, help='Отрасль экономики', required=True)
note_parser.add_argument('date', type=str, help='Дата выгрузки', required=True)


@ns_data_xls_ps.route('/docx_note')
@ns_data_xls_ps.response(200, 'Success')
@ns_data_xls_ps.expect(note_parser)
class IndustryNote(DatasetInfoMixin):
    def get(self):
        """
        Пояснительная записка в docx
        Аргументы:
            - start_eyear: int - год начала
            - end_year: int - год конца 
            - fo: str - [опционально] список ФО
            - regions: str - [опционально] список регионов
            - industry: str -  отрасль экономики
            - date: str - дата выгрузки
        """

        try:
            db : scoped_session = set_db_connection()

            args = note_parser.parse_args()
            
            start_year = args['start_year']
            end_year = args['end_year']

            fos : List[str] = None if not (fos:=args.get('fo', False)) else fos.split(',')
            regions : List[str] = None if not (regions:=args.get('regions', False)) else regions.split(',')
            industry = args.get('industry', None)
            date = args.get('date', None)
            if date:
                date = datetime.strptime(date, '%d.%m.%Y').date()

            (main_query, top3_query) = get_note_query(db=db, start_year=start_year, end_year=end_year, fos=fos, regions=regions, industry=industry, date=date)
            data = db.execute(main_query).mappings().all()
            # print('-\n'*15, data)
            data = combine_note_data_sums(data)
            # print(top3_query.all())
            # print(db.execute(top3_query).mappings().all())
            add_region_detalization(data, db.execute(top3_query).mappings().all())

            base_dict = {
                'year_start': start_year,
                'year_end': end_year,
                'industry_name': str(industry).lower()
            }
            sum_dict = get_summary_docx_dict(data, base_dict)

            base_doc = DocxBuilder().fill_docx_template('Шаблон Пояснительная записка Заголовок.docx', **sum_dict)

            note_doc = DocxBuilder().fill_docx_template('Шаблон Пояснительная записка Примечание.docx')
            DocxBuilder.copy_headers_footers(base_doc, note_doc)

            if sum(len(regions_info) for _, regions_info in data['regions_info'].items()) > 1:
                sum_doc = DocxBuilder().fill_docx_template('Шаблон Пояснительная Записка Всего.docx', **sum_dict)
                DocxBuilder.join_docs(base_doc, sum_doc)

            regions_info = data.get('regions_info', {})

            regions_processed = 0
            for fo_name, regions_dict in regions_info.items():
                for region_name, region_info in regions_dict.items():
                    # last_paragraph = base_doc.paragraphs[-1].runs[-1]
                    # last_paragraph.add_break(WD_BREAK.PAGE)

                    base_dict['region_name'] = region_name
                    region_dict = get_region_docx_dict(region_info, base_dict)
                    region_doc = DocxBuilder().fill_docx_template('Шаблон Пояснительная записка Регион.docx', **region_dict)

                    DocxBuilder.join_docs(base_doc, region_doc)

                    regions_processed += 1

            DocxBuilder.join_docs(base_doc, note_doc)
            base_doc = DocxBuilder.save_file(base_doc)

            return send_file(
                base_doc,
                mimetype='application/vnd.openxmlformats-officedocument.wordprocessingml.document',
                as_attachment=True,
                download_name="Пояснительная записка.docx",
                max_age=0,
            )
        except Exception as e:
            ns_data_xls_ps.abort(*errorhandler(e))