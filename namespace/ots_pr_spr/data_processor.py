import pandas as pd
from typing import List, Tuple, Dict, Any
#
from progSpros_back.namespace.ots_pr_spr.data_models import Consumers, RegionData
from progSpros_back.namespace.ots_pr_spr.constants import shown_columns_map, other_columns_map, value_map
from progSpros_back.namespace.ots_pr_spr.maping import replace_value_map, replace_columns_map, get_column_name

def get_data(query, shown_columns, otrasl_total, yearfrom: int, yearto: int, sum_pr: float = None):

    # Отрасль в итогах
    otrasl_total_filter = otrasl_total

    #
    other_columns_map_new = other_columns_map
    other_columns_map_new['prirost'] = f'Прирост {yearfrom}-{yearto}'

    # Колонки мап
    columns_map = {**shown_columns_map, **other_columns_map_new}

    # Суммирование по полям Год и prirost
    years = [str(y) for y in range(yearfrom, yearto + 1)]
    agg_dict = {
        **{f'y{y}': 'sum' for y in years},
        'prirost': 'sum',
    }

    # null
    data_dict_null = {
        **{f'{get_column_name(column, columns_map)}': None for column in shown_columns},
        **{f'{y} год': f"0.0" for y in years},
        f'{get_column_name("prirost", columns_map)}': f"0.0"
    }

    data_dict_null = replace_value_map(data_dict_null, value_map)

    df = pd.DataFrame([row._asdict() for row in query])

    consumers = Consumers()

    if df.empty:
        return consumers.to_dict()

    if sum_pr is not None:
        #df = df[df['prirost'] > sum_pr]
        df = df[df[f'y{yearto}'] > sum_pr]
        if df.empty:
            return consumers.to_dict()

    processed_df = preprocess_data(df, years, shown_columns)

    ver_real_null = f_ver_real_null()
    ####################################################################################################################
    # total
    total = RegionData(region_name="ИТОГО")
    # -------------------------------------------------------------------------------
    # Группировка (ver_real_level2) по вероятности реализации проекта (действующие потребители, перспективные потребители)
    for ver_real2, ver_real_group2 in processed_df.groupby('ver_real_level2'):
        # Итог по вероятности реализации проекта
        subgroup_itog = ver_real_group2.groupby(['ver_real_level2']).agg(agg_dict).reset_index()
        for row in subgroup_itog.itertuples():
            data_dict = {
                **{f'{get_column_name(column, columns_map)}': None for column in shown_columns},
                **{f'{y} год': f"{round(safe_float(getattr(row, f'y{y}')), 1):.1f}" for y in years},
                f'{get_column_name("prirost", columns_map)}': f"{round(safe_float(row.prirost), 2):.1f}"
            }

            data_dict = replace_value_map(data_dict, value_map)

            total.add_total_consumers(
                ver_real=ver_real2,
                name="ИТОГО",
                data=data_dict
            )

        # Группировка (ver_real_level1) по вероятности реализации проекта (Действующие потребители, Ожидаемые, Потенциальные)
        for ver_real, ver_real_group in ver_real_group2.groupby('ver_real_level1'):
            # Итог по вероятности реализации проекта
            subgroup_itog = ver_real_group.groupby(['ver_real_level1']).agg(agg_dict).reset_index()
            for row in subgroup_itog.itertuples():
                data_dict = {
                    **{f'{get_column_name(column,columns_map)}': None for column in shown_columns},
                    **{f'{y} год': f"{round(safe_float(getattr(row, f'y{y}')), 2):.1f}" for y in years},
                    f'{get_column_name("prirost",columns_map)}': f"{round(safe_float(row.prirost), 2):.1f}"
                }

                data_dict = replace_value_map(data_dict, value_map)

                total.add_consumers(
                    ver_real=ver_real,
                    name="ИТОГО",
                    data=data_dict
                )

                ver_real_null[ver_real] = "true"

            # Отрасль в итогах
            if len(otrasl_total_filter):
                filtered = ver_real_group[ver_real_group['otrasl'].isin(otrasl_total_filter)]
                subgroup_itog = filtered.groupby(['otrasl']).agg(agg_dict).reset_index()
            else:
                subgroup_itog = ver_real_group.groupby(['otrasl']).agg(agg_dict).reset_index()

            for row in subgroup_itog.itertuples():
                data_dict = {
                    **{f'{get_column_name(column,columns_map)}': None for column in shown_columns},
                    **{f'{y} год': f"{round(safe_float(getattr(row, f'y{y}')), 2):.1f}" for y in years},
                    f'{get_column_name("prirost",columns_map)}': f"{round(safe_float(row.prirost), 2):.1f}"
                }

                data_dict = replace_value_map(data_dict, value_map)

                total.add_consumers(
                    ver_real=ver_real,
                    name=row.otrasl,
                    data=data_dict
                )

    for ver_real, value in ver_real_null.items():
        if value == "false":
            total.add_consumers(
                ver_real=ver_real,
                name="ИТОГО",
                data=data_dict_null
            )

    # Ожидаемый сценарий спроса
    subgroup = processed_df.groupby(['expect']).agg(agg_dict).reset_index()
    for row in subgroup.itertuples():
        data_dict = {
            **{f'{get_column_name(column,columns_map)}': None for column in shown_columns},
            **{f'{y} год': f"{round(safe_float(getattr(row, f'y{y}')), 2):.1f}" for y in years},
            f'{get_column_name("prirost",columns_map)}': f"{round(safe_float(row.prirost), 2):.1f}"
        }

        data_dict = replace_value_map(data_dict, value_map)

        total.add_expected_demand(
            data=data_dict
        )

    # Максимальный спрос
    subgroup = processed_df.groupby(['maximum']).agg(agg_dict).reset_index()
    for row in subgroup.itertuples():
        data_dict = {
            **{f'{get_column_name(column,columns_map)}': None for column in shown_columns},
            **{f'{y} год': f"{round(safe_float(getattr(row, f'y{y}')), 2):.1f}" for y in years},
            f'{get_column_name("prirost",columns_map)}': f"{round(safe_float(row.prirost), 2):.1f}"
        }

        data_dict = replace_value_map(data_dict, value_map)

        total.add_max_demand(
            data=data_dict
        )

    consumers.add_total(total)

    ####################################################################################################################
    # regions
    for region, region_group in processed_df.groupby('region'):
        ver_real_null = f_ver_real_null()
        # регион
        region = RegionData(region_name=region)

        # Группировка (ver_real_level2) по вероятности реализации проекта (действующие потребители, перспективные потребители)
        for ver_real2, ver_real_group2 in region_group.groupby('ver_real_level2'):
            # Итог по вероятности реализации проекта (перспективные потребители)
            subgroup_itog = ver_real_group2.groupby(['ver_real_level2']).agg(agg_dict).reset_index()
            for row in subgroup_itog.itertuples():
                data_dict = {
                    **{f'{get_column_name(column, columns_map)}': None for column in shown_columns},
                    **{f'{y} год': f"{round(safe_float(getattr(row, f'y{y}')), 2):.1f}" for y in years},
                    f'{get_column_name("prirost", columns_map)}': f"{round(safe_float(row.prirost), 2):.1f}"
                }

                data_dict = replace_value_map(data_dict, value_map)

                region.add_total_consumers(
                    ver_real=ver_real2,
                    name="ИТОГО",
                    data=data_dict
                )

            # Группировка (ver_real_level1) по вероятности реализации проекта (Действующие потребители, Ожидаемые, Потенциальные)
            for ver_real, ver_real_group in ver_real_group2.groupby('ver_real_level1'):
                # Итог по вероятности реализации проекта
                subgroup_itog = ver_real_group.groupby(['ver_real_level1']).agg(agg_dict).reset_index()
                for row in subgroup_itog.itertuples():
                    data_dict = {
                        **{f'{get_column_name(column,columns_map)}': None for column in shown_columns},
                        **{f'{y} год': f"{round(safe_float(getattr(row, f'y{y}')), 2):.1f}" for y in years},
                        f'{get_column_name("prirost",columns_map)}': f"{round(safe_float(row.prirost), 2):.1f}"
                    }

                    data_dict = replace_value_map(data_dict, value_map)

                    region.add_consumers(
                        ver_real=ver_real,
                        name="ИТОГО",
                        data=data_dict
                    )

                    region.add_detalization_consumers(
                        ver_real=ver_real,
                        name="ИТОГО",
                        data=data_dict
                    )

                    ver_real_null[ver_real] = "true"

                # Отрасль в итогах
                if len(otrasl_total_filter):
                    filtered = ver_real_group[ver_real_group['otrasl'].isin(otrasl_total_filter)]
                    subgroup_itog = filtered.groupby(['otrasl']).agg(agg_dict).reset_index()
                else:
                    subgroup_itog = ver_real_group.groupby(['otrasl']).agg(agg_dict).reset_index()

                for row in subgroup_itog.itertuples():
                    data_dict = {
                        **{f'{get_column_name(column,columns_map)}': None for column in shown_columns},
                        **{f'{y} год': f"{round(safe_float(getattr(row, f'y{y}')), 2):.1f}" for y in years},
                        f'{get_column_name("prirost",columns_map)}': f"{round(safe_float(row.prirost), 2):.1f}"

                    }

                    data_dict = replace_value_map(data_dict, value_map)

                    region.add_consumers(
                        ver_real=ver_real,
                        name=row.otrasl,
                        data=data_dict
                    )

                # Детализация по потребителям
                group_cols = ['sort', 'contragent'] + shown_columns
                subgroup_itog = (
                    ver_real_group
                    .groupby(group_cols, dropna=False)  # важно оставить dropna=False
                    .agg(agg_dict)
                    .reset_index()
                )
                subgroup_itog = subgroup_itog.merge(
                    ver_real_group[['sort', 'contragent', 'otrasl_ord'] + shown_columns].drop_duplicates(),
                    on=['sort', 'contragent'] + shown_columns,
                    how='left'
                )

                subgroup_itog = sort_subgroup(subgroup_itog)  # Применяем сортировку
                for row in subgroup_itog.itertuples():
                    data_dict = {
                        **{f'{get_column_name(column, columns_map)}': getattr(row, f'{column}') for column in
                           shown_columns},
                        **{f'{y} год': f"{round(safe_float(getattr(row, f'y{y}')), 2):.1f}" for y in years},
                        f'{get_column_name("prirost", columns_map)}': f"{round(safe_float(row.prirost), 2):.1f}"
                    }

                    data_dict = replace_value_map(data_dict, value_map)

                    region.add_detalization_consumers(
                        ver_real=ver_real,
                        name=row.contragent,
                        data=data_dict
                    )

        for ver_real, value in ver_real_null.items():
            if value == "false":
                region.add_consumers(
                    ver_real=ver_real,
                    name="ИТОГО",
                    data=data_dict_null
                )

                region.add_detalization_consumers(
                    ver_real=ver_real,
                    name="ИТОГО",
                    data=data_dict_null
                )

        # Ожидаемый сценарий спроса
        subgroup = region_group.groupby(['expect']).agg(agg_dict).reset_index()
        for row in subgroup.itertuples():
            data_dict = {
                **{f'{get_column_name(column,columns_map)}': None for column in shown_columns},
                **{f'{y} год': f"{round(safe_float(getattr(row, f'y{y}')), 2):.1f}" for y in years},
                f'{get_column_name("prirost",columns_map)}': f"{round(safe_float(row.prirost), 2):.1f}"
            }

            data_dict = replace_value_map(data_dict, value_map)

            region.add_expected_demand(
                data=data_dict
            )

        # Максимальный спрос
        subgroup = region_group.groupby(['maximum']).agg(agg_dict).reset_index()
        for row in subgroup.itertuples():
            data_dict = {
                **{f'{get_column_name(column,columns_map)}': None for column in shown_columns},
                **{f'{y} год': f"{round(safe_float(getattr(row, f'y{y}')), 2):.1f}" for y in years},
                f'{get_column_name("prirost",columns_map)}': f"{round(safe_float(row.prirost), 2):.1f}"
            }

            data_dict = replace_value_map(data_dict, value_map)

            region.add_max_demand(
                data=data_dict
            )

        consumers.add_consumer(region)

    result = consumers.to_dict()

    return result

def preprocess_data(df: pd.DataFrame, years: List[str], others_columns: List[str]) -> pd.DataFrame:
    """Предварительная обработка данных"""

    # Преобразование категорий
    category_mappings = {
        'ver_real_level1': {
            '0 - действующие потребители': 'действующие потребители',
            '1 - высокая вероятность реализации проекта': 'ожидаемые перспективные потребители',
            '2 - средняя вероятность реализации проекта': 'ожидаемые перспективные потребители',
            '3 - низкая вероятность реализации проекта': 'потенциальные перспективные потребители',
        },
        'ver_real_level2': {
            'действующие потребители': 'действующие потребители',
            'ожидаемые перспективные потребители': 'перспективные потребители',
            'потенциальные перспективные потребители': 'перспективные потребители',
        },
        'expect': {
            'действующие потребители': 'ожидаемый',
            'ожидаемые перспективные потребители': 'ожидаемый'
        },
        'maximum': {
            'действующие потребители': 'максимальный',
            'ожидаемые перспективные потребители': 'максимальный',
            'потенциальные перспективные потребители': 'максимальный'
        },
        'sort': {
            'Действующие потребители': '1',
            'Прочие потребители': '2',
        }
    }

    columns_mappings = {
        'ver_real_level1': 'ver_real',
        'ver_real_level2': 'ver_real_level1',
        'expect': 'ver_real_level1',
        'maximum': 'ver_real_level1',
        'sort': 'contragent',
    }

    for col, mapping in category_mappings.items():
        source_col = columns_mappings[col]  # 'contragent'
        df[col] = df[source_col].map(mapping).fillna('0')

    # Очистка данных для специальных контрагентов
    mask = df['contragent'].isin(['Действующие потребители', 'Прочие потребители'])
    if len(others_columns):
        df.loc[mask, others_columns] = ''
        df.loc[mask, 'otrasl'] = ''
    else:
        df.loc[mask, 'otrasl'] = ''

    return df

def safe_float(value: Any) -> float:
    """Безопасное преобразование в float"""
    return float(value) if value is not None else 0.0

def f_ver_real_null():
    return {
        "действующие потребители": "false",
        "ожидаемые перспективные потребители": "false",
        "потенциальные перспективные потребители": "false",
    }

def sort_subgroup(df: pd.DataFrame) -> pd.DataFrame:
    """Сортировка подгрупп: сначала по otrasl_ord (пустые в конец), затем по contragent"""
    return (
        df.assign(
            # Флаг: True, если пустое значение
            _otrasl_empty=lambda x: x['otrasl_ord'].isna() | (x['otrasl_ord'] == '')
        )
        .sort_values(
            by=['_otrasl_empty', 'otrasl_ord', 'contragent'],
            ascending=[True, True, True],
            na_position='last'
        )
        .drop(columns=['_otrasl_empty'])
        .reset_index(drop=True)
    )