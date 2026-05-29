from collections import defaultdict
from itertools import zip_longest
from typing import Dict, List
from urllib.parse import parse_qs
from decimal import Decimal
from datetime import datetime
from sqlalchemy import Tuple, inspect, create_engine
# from sqlalchemy.orm import scoped_session, sessionmaker

from flask import g

from progSpros_back.functions.query_functions_ps import mapping_query, mapping_vers
from progSpros_back.database_ps import set_db_connection
from progSpros_back.config_ps import Config
from progSpros_back.model.mappings_ps import version_leveled_mappings, tick_mapping

def create_filter_params(request):
    """
        Создает параметры фильтра из параметра запроса 'global_filters' в запросе.

        Аргументы:
            query (Request): Объект запроса Flask.

        Возвращается:
            dict: Словарь параметров фильтра.
    """
    # Извлекает параметр запроса 'global_filters' из запроса
    dynamic_filters = request.args.get('global_filters', '')

    # Инициализирует пустой словарь для хранения параметров фильтра
    filter_params = {}

    # Проверяет, не является ли значение dynamic_filters пустым
    if dynamic_filters:
        # Преобразует строку запроса в словарь
        parsed_filters = parse_qs(dynamic_filters)

        # Преобразует одноэлементные списки в их значения
        for key, value in parsed_filters.items():
            if len(value) == 1:
                # Если в списке есть только один элемент, сохранить этот элемент в качестве значения
                filter_params[key] = value[0]
            else:
                # Если список содержит несколько элементов, сохранить весь список в качестве значения
                filter_params[key] = value

    return filter_params

def get_list_of_global_filters(session, table, reference_models, exclude_columns=None, filter_params=None):
    """
        Генерирует список фильтруемых имен столбцов и их уникальных параметров для данной таблицы.

        Аргументы:
            session (Session): Объект SQLAlchemy session.
            table (Table): Таблица, из которой необходимо извлечь фильтры.
            reference_models (dict): Словарь эталонных моделей для дополнительных параметров фильтрации.
            filter_params (dict, необязательный): Словарь параметров фильтра, которые будут применяться к запросу. По умолчанию нет.
            exclude_columns (список, необязательно): список имен столбцов, которые необходимо исключить из фильтров. По умолчанию - Нет.

        Возвращается:
            dict: Словарь фильтруемых столбцов с их уникальными параметрами.
    """
    if exclude_columns is None:
        exclude_columns = []

    # Базовый запрос с применяемыми параметрами filter_params, если они указаны
    base_query = session.query(table)
    if filter_params:
        for param, value in filter_params.items():
            column = getattr(table, param, None)
            if column is not None:
                if isinstance(value, list):
                    base_query = base_query.filter(column.in_(value))
                else:
                    base_query = base_query.filter(column == value)

    # Список имен столбцов для фильтрации
    filterable_columns = [column.name for column in inspect(table).columns if column.name not in exclude_columns]

    filter_options = {}

    for column in filterable_columns:
        # Получить уникальные значения для каждого столбца из базового запроса
        unique_values_query = base_query.with_entities(getattr(table, column)).filter(getattr(table, column).isnot(None)).distinct()
        unique_values = unique_values_query.all()
        # Сгладить список и отсортировать
        filter_options[column] = sorted([value[0] for value in unique_values if value is not None])

    # Добавить названия эталонных моделей к фильтрам
    for table_name, ref_model in reference_models.items():
        if table_name not in exclude_columns:
            name_column = getattr(ref_model, 'name', None)
            if name_column:
                # Запросить различные значения из эталонной модели, исключая те, которые указаны в EXCLUDE_COMPANY_LIST
                query = session.query(name_column).distinct()
                results = query.all()
                # Сохранить настройки фильтра и сортировать
                filter_options[table_name] = sorted([row[0] for row in results])

    return filter_options

# Функция поиска максимального значения в данных
def find_max_value(data):
    """Finds the maximum value in the data.

    Args:
        data (list): A list of tuples containing the data entries.

    Returns:
        Decimal: The maximum value found in the data, or Decimal('0') if no valid value is found.
    """
    # Iterate over data to find the maximum value, ignoring None entries
    return max((entry[2] for entry in data if entry[2] is not None), default=Decimal('0'))

# Структура Прогнозный спрос РФ с топ-потребителями
def create_structure(name, data, version_mapping, result=None):
    if result is None:
        result = {}
    # Получить структуру
    initialize_structure(name, data, result)
    # Обновить по меппингу ключи версий
    result = substitute_in_json(result, version_mapping)
    return result
def initialize_structure(name, data, result):
    """Инициализация структуры со значениями по умолчанию

    Аргументы:
        name (str):  базовое имя для записей в структуре
        data (list): Список, содержащих записи данных.
        result (dict): Данные, которые должны быть заполнены значениями.

    """
    for year, vers, potr, summ in data:
        if year not in result:
            result[year] = {'sum_year': 0}
        if vers not in result[year]:
            result[year][vers] = {'sum_vers': 0, 'potr_list': []}
            i = 0

        result[year][vers]['sum_vers'] += summ
        result[year]['sum_year'] += summ

        #Добавить только 5 потребителей
        if i < 5:
            result[year][vers]['potr_list'].append({'potr': potr, 'sum': summ})
        i += 1

# Структура для Карты
def create_structure_fo(name, name_cat, data, version_mapping, result=None):
    if result is None:
        result = {}
    # Получить структуру
    initialize_structure_fo(name, data, result, name_cat)
    # Обновить по меппингу ключи версий
    result = substitute_in_json(result, version_mapping)
    return result
def initialize_structure_fo(name, data, result, name_cat):
    """Инициализация структуры со значениями по умолчанию

    Аргументы:
        name (str):  базовое имя для записей в структуре
        data (list): Список, содержащих записи данных.
        result (dict): Данные, которые должны быть заполнены значениями.

    """
    for fo, otr, summ in data:
        if fo not in result:
            result[fo] = {'sum_fo': 0, name_cat: []}
            i = 0

        result[fo]['sum_fo'] += summ

        #Добавить только 10 потребителей
        if i < 10:
            result[fo][name_cat].append({'category': otr, 'sum': summ})
        i += 1
def substitute_in_json(data, mapping):
    if isinstance(data, dict):
        return {mapping.get(k, k): substitute_in_json(v, mapping) for k, v in data.items()}

    elif isinstance(data, list):
        return [substitute_in_json(item, mapping) for item in data]

    elif isinstance(data, str):
        return mapping.get(data, data)
    else:
        return data

def to_date(date_string):
    try:
        formats = ["%d.%m.%Y", "%m-%d-%Y", "%Y-%m-%d"]
        for fmt in formats:
            try:
                return datetime.strptime(date_string, fmt)
            except ValueError:
                continue
        raise ValueError('{} is not valid date'.format(date_string))
    except Exception as e:
        raise ValueError('{} is not valid date: {}'.format(date_string, str(e)))

def sum_prirost(data, sum_param):
    for row in data:
        if row.sum_par == None:
            sum_param = 0
        else:
            sum_param = float(row.sum_par)

    return sum_param

def mapping(map):
    db = set_db_connection()
    base_query = db.query(map)
    query = mapping_query(base_query, map)

    result = {}
    for row in query:
        if row.id not in result:
            result[row.id] = row.name
    return result


def tuple_sum(a: Tuple, b: Tuple) -> Tuple:
    return tuple(x + y if x is not None and y is not None else None for x, y in zip_longest(a, b, fillvalue=None))

def combine_note_data_sums(data: List[Dict]) -> Dict:
    # print(data)
    EXPECT_MAPPER = version_leveled_mappings['expect']
    MAXIMUM_NAME = list(version_leveled_mappings['maximum'].values())[0]
    # print(EXPECT_MAPPER, MAXIMUM_NAME)
    
    result = {
        'summary': defaultdict(lambda: (0, 0)),
        'regions_info': defaultdict(lambda: defaultdict(lambda: {
            'summary': defaultdict(lambda: (0, 0))
            # 'contragents': []
        }))
    }
    summary = result['summary']
    regions_info = result['regions_info']
    
    group_names = set(version_leveled_mappings['ver_real_level1'].values())
    group_names.add(EXPECT_MAPPER.values())
    group_names.add(MAXIMUM_NAME)
    def init_sums(d: Dict) -> Dict:
        for group_name in group_names:
            d[group_name] = (0, 0) # start_year, end_year

    
    init_sums(summary)
    for row in data:
        fo_name = row['fo_name']
        region_name = row['region_name']
        vers_name = row['version_name']
        summs = (row['summ_start'], row['summ_end'])

        regions_info[fo_name]
        region_data = regions_info[fo_name][region_name]
        if 'summary' not in region_data:
            region_data['summary'] = defaultdict(lambda: (0, 0))
            init_sums(region_data['summary'])
        region_sum = region_data['summary']
        summary[vers_name] = tuple_sum(summary[vers_name], summs)
        region_sum[vers_name] = tuple_sum(region_sum[vers_name], summs)
        if (vers_name:=EXPECT_MAPPER.get(vers_name, False)):
            summary[vers_name] = tuple_sum(summary[vers_name], summs)
            region_sum[vers_name] = tuple_sum(region_sum[vers_name], summs)
        summary[MAXIMUM_NAME] = tuple_sum(summary[MAXIMUM_NAME], summs)
        region_sum[MAXIMUM_NAME] = tuple_sum(region_sum[MAXIMUM_NAME], summs)
    # print(result)
    return result

def add_region_detalization(info: Dict, data: List[Dict]):
    for row in data:
        fo_name = row.get('fo_name')
        region_name = row.get('region_name')
        vers_name = row.get('version_name')
        summs = (row.get('summ_start', 0), row.get('summ_end', 0))
        
        if not all([fo_name, region_name, vers_name]):
            continue

        def get_tick(value: str, mapper: Dict) -> bool:
            if value is None:
                return False
            if len(value) > 0:
                value = value.split(',')[0]
            return mapper.get(value, value)
        
        tu = get_tick(row.get('tu_list'), tick_mapping)
        pg = get_tick(row.get('pg_list'), tick_mapping)
        contract = get_tick(row.get('dogovor_list'), tick_mapping)
        
        if 'contragents' not in info['regions_info'][fo_name][region_name]:
            info['regions_info'][fo_name][region_name]['contragents'] = defaultdict(list)

        region_vers_info = info['regions_info'][fo_name][region_name]['contragents'][vers_name]
        

        contragent_info = {'name': row.get('contragent_name'),
                           'summs': summs, 'start_gaz_year': row.get('start_gaz_year'),
                           'tu': tu, 'pg': pg, 'contract': contract}
        region_vers_info.append(contragent_info)
