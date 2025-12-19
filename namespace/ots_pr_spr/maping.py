def replace_columns_map(data_dict: dict, data: dict) -> dict:
    """
    Заменяет ключи в data_dict на значения из data, если ключи совпадают.

    Args:
        data_dict: Исходный словарь, в котором нужно заменить ключи.
        data: Словарь с маппингом старых ключей (key) на новые (value).

    Returns:
        Новый словарь с обновленными ключами.
    """
    # Создаем копию, чтобы не изменять исходный словарь
    result = data_dict.copy()

    for old_key, new_key in data.items():
        if old_key in result:
            # Заменяем старый ключ на новый, сохраняя значение
            result[new_key] = result.pop(old_key)

    return result


def replace_value_map(dict1: dict, dict2: dict) -> dict:
    """
    Заменяет значения в dict1 на соответствующие значения из dict2.
    Если значение из dict1 есть в dict2 как ключ, оно заменяется на значение из dict2.

    Args:
        dict1: Исходный словарь, в котором нужно заменить значения
        dict2: Словарь замен формата {старое_значение: новое_значение}

    Returns:
        Новый словарь с замененными значениями
    """
    return {key: dict2.get(value, value) for key, value in dict1.items()}


def reverse_replace(columns_list: list, mapping_dict: dict) -> list:
    """
    Заменяет значения в списке на соответствующие ключи из словаря.

    Args:
        columns_list: Список значений для замены
        mapping_dict: Словарь, где ключи - исходные значения, а значения - их замены

    Returns:
        Список с замененными значениями (ключами из mapping_dict)
    """
    # Создаем обратный словарь {значение: ключ}
    reverse_map = {v: k for k, v in mapping_dict.items()}

    return [reverse_map.get(item, item) for item in columns_list]


def get_column_name(key, shown_columns_map):

    return shown_columns_map.get(key, f"Ошибка: ключ '{key}' не найден в словаре")