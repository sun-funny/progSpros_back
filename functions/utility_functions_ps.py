from urllib.parse import parse_qs
from decimal import Decimal
from datetime import datetime
from sqlalchemy import inspect
from progSpros_back.database_ps import db
from flask import g
from progSpros_back.functions.query_functions_ps import mapping_query

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

def set_db_connection():
    return g.session

def mapping(map):
    # db = set_db_connection()
    base_query = db.query(map)
    query = mapping_query(base_query, map)

    result = {}
    for row in query:
        if row.id not in result:
            result[row.id] = row.name
    return result

