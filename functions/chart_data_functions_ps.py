import re

from sqlalchemy import and_
from sqlalchemy.orm.attributes import InstrumentedAttribute
from sqlalchemy.orm.exc import NoResultFound
from progSpros_back.config_ps import format_strings
from flask import current_app as app

"""

========================  ФОРМИРОВАНИЕ JSON-структуры для графиков  ==========================================

"""
def generate_chart_data_year(query, title, format_string, version_mapping,
                                  description=None):
    """
        Создает данные диаграммы Прогнозный спрос РФ в разрезе годов и версий прогноза.

        Аргументы:
            query (SQLAlchemy Query): Объект запроса для выполнения и получения результатов.
            title (str): заголовок диаграммы.
            format_string (str): строка формата для отображения итоговых данных. формат f-строки.
            version_mapping (dict): Словарь, отображающий версии для отображения ключей.
            description (str, необязательно): Описание диаграммы. По умолчанию равно None.

        Возвращается:
            graph data dictionary: Словарь, содержащий графические данные, включая название, описание и данные.
    """
    try:
        # Выполнить запрос и получить результаты
        results = query.all()

        # Инициализировать временный справочник для хранения данных перед структурированием
        temp_data = {category: {key: None for key in version_mapping.values()} for category, _, _ in results}

        # Заполнить временное хранилище с отформатированными итоговыми данными
        for category, version, total in results:
            if version in version_mapping:  # Проверить, есть ли версия в version_mapping
                temp_data[category][version_mapping[version]] = float(total)

        # Преобразует временные данные в требуемый формат списка
        data = [{"year": category, **values} for category, values in temp_data.items()]

        # Создает и возвращает готовый набор графических данных
        return {
            "title": f"{title}",  # Название
            "description": description,  # Описание
            "data": data  # Обработанные данные
        }
    except Exception as e:
        # Если ошибка, то вывести сообщение
        app.logger.error(f"При создании графических данных произошла ошибка: {e}")
        raise

def generate_chart_data_otrasl(query, title, format_string, version_mapping,
                                  description=None):
    """
        Создает данные диаграммы Прогнозный спрос РФ в разрезе годов и версий прогноза.

        Аргументы:
            query (SQLAlchemy Query): Объект запроса для выполнения и получения результатов.
            title (str): заголовок диаграммы.
            format_string (str): строка формата для отображения итоговых данных. формат f-строки.
            version_mapping (dict): Словарь, отображающий версии для отображения ключей.
            description (str, необязательно): Описание диаграммы. По умолчанию равно None.

        Возвращается:
            graph data dictionary: Словарь, содержащий графические данные, включая название, описание и данные.
    """
    try:
        # Выполнить запрос и получить результаты
        results = query.all()
        print(results)

        # Инициализировать временный справочник для хранения данных перед структурированием
        temp_data = {category: {key: None for key in version_mapping.values()} for category, _, _ in results}

        # Заполнить временное хранилище с отформатированными итоговыми данными
        for category, version, total in results:
            if version in version_mapping:  # Проверить, есть ли версия в version_mapping
                temp_data[category][version_mapping[version]] = float(total)

        # Преобразует временные данные в требуемый формат списка
        data = [{"otrasl": category, **values} for category, values in temp_data.items()]

        # Создает и возвращает готовый набор графических данных
        return {
            "title": f"{title}",  # Название
            "description": description,  # Описание
            "data": data  # Обработанные данные
        }
    except Exception as e:
        # Если ошибка, то вывести сообщение
        app.logger.error(f"При создании графических данных произошла ошибка: {e}")
        raise
"""

 ================================================== ФИЛЬТРЫ =================================================
 
"""
def get_related_ids(session, related_model, value):
    """
        Извлекает связанные идентификаторы из связанной модели на основе предоставленных значений.

        Аргументы:
            session: сессия SQLAlchemy.
            related_model: Связанный класс модели.
            value: Значение фильтра.

        Возвращается:
            Список связанных идентификаторов.
    """
    try:
        if isinstance(value, list):
            # Выполнить запрос и получить результаты
            related_ids = session.query(related_model.id).filter(related_model.name.in_(value)).all()
            return [rid[0] for rid in related_ids]
        else:
            # Выполнить запрос и получить результаты
            related_id = session.query(related_model.id).filter_by(name=value).one()[0]
            return [related_id]
    except NoResultFound:
        return []

def apply_filter_conditions(model, param, value, models_dict, session):
    """
        Создает условия фильтрации на основе предоставленных параметров.

        Аргументы:
            model: класс модели SQLAlchemy для фильтрации.
            param: Имя параметра фильтра.
            value: Значение фильтра.
            models_dict: Словарь, отображающий имена таблиц в классы моделей.
            session: сессия SQLAlchemy.

        Возвращается:
            Список условий фильтрации.
    """
    filter_conditions = []
    related_model = models_dict.get(param)

    if related_model:
        # Извлечь связанные идентификаторы, если есть связанная модель
        related_ids = get_related_ids(session, related_model, value)
        if related_ids:
            model_attr = getattr(model, f'{param}_ids', None)
            if isinstance(model_attr, InstrumentedAttribute):
                filter_conditions.append(model_attr.in_(related_ids))
    else:
        # Динамически получить атрибут модели и создать условие фильтрации
        model_attr = getattr(model, param, None)
        if isinstance(model_attr, InstrumentedAttribute):
            if isinstance(value, list):
                filter_conditions.append(model_attr.in_(value))
            else:
                filter_conditions.append(model_attr == value)

    return filter_conditions

def get_year_range(query, years_range=None):
    """
        Получает диапазон годов из результатов запроса.

        Аргументы:
            query (SQLAlchemy Query): Объект запроса для выполнения и получения результатов.

        Возвращается:
            str: строка, представляющая диапазон лет, например, "2000-2020" или "2000", если существует только один уникальный год.
            None: Если результатов нет.
    """
    try:
        if years_range:
            if isinstance(years_range['YEAR'], list):
                years = {int(year) for year in years_range['YEAR']}
            else:
                years = {int(years_range['YEAR'])}
        else:# Выполняет запрос и получите результаты
            results = query.all()

            # Извлечь годы из результа запроса
            years = {year for year, version, total in results}

        if not years:
            return None  # значение по умолчанию None, если результата запроса нет

        # Определите самый маленький и самый большой годы
        smallest_year = min(years)
        highest_year = max(years)

        # Отформатируйте выходную строку в соответствии с количеством уникальных лет
        if smallest_year == highest_year:
            year_range = f"{smallest_year}"
        else:
            year_range = f"{smallest_year}-{highest_year}"

        return year_range
    except Exception as e:
        # Вывести сообщение об ошибке с помощью регистратора current_app
        app.logger.error(f"При создании графических данных произошла ошибка: {e}")
        raise

def apply_dynamic_filters(query, model, filter_params, session, models_dict):
    """
    Динамически применяет фильтры к запросу SQLAlchemy на основе предоставленных параметров фильтра.

        Аргументы:
        query: объект запроса SQLAlchemy, к которому нужно применить фильтры.
        model: класс модели SQLAlchemy для фильтрации.
        filter_params: Словарь параметров фильтра из запроса.
        session: сессия SQLAlchemy.
        models_dict: Словарь, отображающий имена таблиц в классы моделей.

        Возвращается:
            Отфильтрованный запрос SQLAlchemy
    """
    filter_conditions = []

    if filter_params is None:
        filter_params = {}

    for param, value in filter_params.items():
        if param == 'region':
            param = 'T_DICT_NSI_REGIONS_D314'
        if param == 'fo':
            param = 'T_DICT_NSI_FO_D314'
        if param == 'otrasl':
            param = 'T_DICT_NSI_OTRASL_ECONOMY_D314'
        if param == 'vers':
            param = 'TAB_VER_REAL_PR_D314'
        if param == 'grpost':
            param = 'TAB_GROUP_POST_D314'
        if param == 'dogovor':
            param = 'TAB_DOGOVOR_VISUAL_D314'
        if param == 'tu':
            param = 'TAB_TU_VISUAL_D314'
        if param == 'infr':
            param = 'TAB_INFR_D314'

        if param == 'yearfrom':
            param = 'YEAR'
        if param == 'yearto':
            param = 'YEAR'

        if value:
            # Применить условия фильтрации для каждого параметра
            filter_conditions.extend(apply_filter_conditions(model, param, value, models_dict, session))

    if filter_conditions:
        # Применить все условия фильтрации к запросу
        query = query.filter(and_(*filter_conditions))

    return query
