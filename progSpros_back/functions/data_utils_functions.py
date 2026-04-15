from typing import Dict, Optional

from sqlalchemy import Tuple

ID_PREFIX_MAPPER = {
    'ожидаемые перспективные потребители': 'expect', 
    'действующие потребители': 'exist', 
    'потенциальные перспективные потребители': 'potential', 
    'максимальный': 'max', 
    'ожидаемый': 'promised',
}

def _is_none_or_zero(item):
    return (item is None or item == 0)
def format_float(num: float) -> str:
    num_str = f"{num:,.2f}"
    num_str = num_str.replace(',', ' ').replace('.', ',')

    return num_str
def _generate_comparison_string(result_dict: dict, prefix_str: str):
    start_value = result_dict.get(f'{prefix_str}_start')
    end_value = result_dict.get(f'{prefix_str}_end')
    start_year, end_year = result_dict.get(f'year_start'), result_dict.get(f'year_end')
    
    
    can_calculate_diff = True
    
    if _is_none_or_zero(start_value):
        start_text = 'отсутствует'
        can_calculate_diff = False
    else:
        start_text = f'составляет {format_float(start_value)} млн куб. м'
    
    if _is_none_or_zero(end_value):
        end_text = 'отсутствует'
        can_calculate_diff = False
    else:
        end_text = f'{format_float(end_value)} млн куб. м'
    
    if not can_calculate_diff:
        if _is_none_or_zero(start_value) and _is_none_or_zero(end_value):
            result_string = (
                f'в {start_year} году отсутствует, и на {end_year} год также отсутствует.'
            )
        elif _is_none_or_zero(start_value):
            # 
            result_string = (
                f'в {start_year} году отсутствует, а на {end_year} год ожидается {end_text}.'
            )
        else:
            result_string = (
                f'в {start_year} году {start_text}, а на {end_year} год данные отсутствуют.'
            )
    else:
        # оба значения 
        diff = end_value - start_value
        
        if diff > 0:
            diff_title = 'прирост'
            diff_text = f'+{format_float(diff)}'
        elif diff < 0:
            diff_title = 'снижение'
            diff_text = f'-{format_float(abs(diff))}'
        else:
            diff_title = 'изменение'
            diff_text = 'отсутствует'
        
        result_string = (
            f'в {start_year} году {start_text}, {diff_title} потребления '
            f'к {end_year} году оценивается в {diff_text} млн куб. м '
            f'до {end_text}.'
        )
    result_dict[f'{prefix_str}_comparison'] = result_string


def _add_contragents(result_dict: Dict, prefix_str: str, contragents_data: Dict):
    if contragents_data is None:
        return
    # result_dict[f'{prefix_str}_comparison']
    contragents_parts = []
    for data in contragents_data:
        pg_str = 'объект по ПГ' if data.get('pg') else None
        contract_str = 'есть договор' if data.get('contract') else 'нет договора'
        tu_str = 'есть ТУ' if data.get('tu') else 'нет ТУ'
        indicators_str = ', '.join(filter(lambda x: x is not None, [pg_str, contract_str, tu_str]))
        contragents_parts.append(f'{data.get("name")} {format_float(data.get("summs", (None, None))[1])} млн куб м ({indicators_str})')
    nl = ';\n'
    result_dict[f'{prefix_str}_comparison'] += f' в том числе крупные потребители:\n {nl.join(contragents_parts)}.'


def _get_summs_dict(result_dict: Dict, detalisations_data: Dict, contragents_data: Optional[Dict] = None):
    received_keys = set()
    for key, summs in detalisations_data.items():

        if not (prefix_str:=ID_PREFIX_MAPPER.get(key, False)):
            continue
        received_keys.add(key)
        result_dict[f'{prefix_str}_start'], result_dict[f'{prefix_str}_end'] = summs
        _generate_comparison_string(result_dict=result_dict, prefix_str=prefix_str)
        
        if contragents_data is not None and not (_is_none_or_zero(result_dict[f'{prefix_str}_end'])): # end_contragents_exist
            _add_contragents(result_dict=result_dict, prefix_str=prefix_str, contragents_data=contragents_data.get(key))
    for missed_key in set(ID_PREFIX_MAPPER.keys()).difference(received_keys):
        prefix_str = ID_PREFIX_MAPPER[missed_key]
        result_dict[f'{prefix_str}_start'], result_dict[f'{prefix_str}_end'] = (0, 0)
        _generate_comparison_string(result_dict=result_dict, prefix_str=prefix_str)



def get_summary_docx_dict(data: Dict, add_dict: Optional[Dict] = None) -> Dict:
    sum_dict = {}
    if add_dict:
        sum_dict.update(add_dict)
    sum_dict['title_regions'] = []
    for fo_name, regions_info in data.get('regions_info', {}).items():
        sum_dict['title_regions'].append(f'{fo_name}: {", ".join(list(regions_info.keys()))};')
    sum_dict['title_regions'] = '\n'.join(sum_dict['title_regions'])

    _get_summs_dict(sum_dict, data.get('summary', {}))

    return sum_dict



def get_region_docx_dict(data: Dict, add_dict: Optional[Dict] = None) -> Dict:
    sum_dict = {}
    if add_dict:
        sum_dict.update(add_dict)
        
    _get_summs_dict(sum_dict, data.get('summary', {}), data.get('contragents', {}))

    return sum_dict