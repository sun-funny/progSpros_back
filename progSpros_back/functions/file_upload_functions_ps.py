from copy import copy
import os
from typing import Any, Dict, List, Optional
from dataclasses import dataclass
from sqlalchemy import Column, RowMapping
import openpyxl
from flask import current_app
from io import BytesIO
import zipfile

@dataclass
class ColumnDescriptor:
    db_column: Optional[Column] = None
    excel_title: Optional[str] = None
    mapping: Optional[Dict] = None

@dataclass(frozen=True)
class HashableDict:
    data: tuple

    @classmethod
    def from_dict(cls, d: dict):
        return cls(tuple(sorted(d.items())))
    
    def to_dict(self):
        return dict(self.data)
    

def prepare_all_data(titles: Dict[str, ColumnDescriptor], rows: List[Dict]):
    parted_rows = {}
    years_range = (100500, 0)
    row_mapper = {k: col.mapping for k, col in titles.items() if col.mapping is not None}

    titles['start_year'] = ColumnDescriptor(excel_title='Начало отбора')

    def get_row_key_dict(row: Dict) -> Dict:
        key_dict = HashableDict.from_dict({k: v for k, v in row.items() if k not in ['sum', 'year']})
        return key_dict
    def transform_row(row: RowMapping) -> Dict:
        result_row = dict(row)
        for key, mapping in row_mapper.items():
            if key in row:
                result_row[key] = mapping.get(row[key])
        return result_row
    
    for row in rows:
        row = transform_row(row)
        year = row.get('year')
        if year:
            titles[year] = ColumnDescriptor(excel_title=str(year))
            years_range = (min(years_range[0], year), max(years_range[1], year))
        key = get_row_key_dict(row)
        if not parted_rows.get(key):
            
            new_row = {'sum': [row.get('sum')], 'start_year': [year]}
            #if year:
            new_row[year] = [row.get('sum')]
            parted_rows[key] = new_row
        else:
            new_row = parted_rows[key]
            new_row['sum'].append(row.get('sum'))
            new_row['start_year'].append(year)
            if year not in new_row:
                new_row[year] = []
            new_row[year].append(row.get('sum'))
            parted_rows[key] = new_row
    result = []
    for start, end in parted_rows.items():
        end['sum'] = sum(filter((lambda x : x is not None), end['sum']))
        end['start_year'] = min(filter((lambda x : x is not None), end['start_year']))
        if None in end:
            end.pop(None)
        result_row = copy(start.to_dict())
        result_row['sum'] = float(end['sum'])
        result_row['start_year'] = end['start_year']
        end.pop('sum')
        end.pop('start_year')

        for year, value in end.items():
            result_row[year] = float(sum(filter((lambda x : x is not None), value)))
        # result_row.update(end)
        result.append(result_row)
    
    return (result, years_range)

def _get_template_path(template_name: str) -> str:
    """
    Шаблон лежит в app/templates.
    current_app.root_path указывает на папку app.
    """
    return os.path.join(current_app.root_path, "templates", template_name)


def build_export_xlsx(template_name: str, headers: Dict[str, ColumnDescriptor], data):
    template_path = _get_template_path(template_name)
    if not os.path.exists(template_path):
        raise ValueError(f"Не найден шаблон выгрузки: {template_path}")
    
    wb = openpyxl.load_workbook(template_path)
    ws = wb.active

    max_col = ws.max_column

    cols_mapper = {col.excel_title: key for key, col in headers.items()}
    template_headers_mapper = {}
    for c in range(1, max_col + 1):
        template_headers_mapper[cols_mapper.get(str(ws.cell(1, c).value).replace('\n', ' '))] = c 
    
    
    template_cells = [ws.cell(2, c) for c in range(1, max_col + 1)]
    template_styles = [c._style for c in template_cells]
    template_numfmts = [c.number_format for c in template_cells]
    template_row_height = ws.row_dimensions[2].height


    rows_touched = set()
    del_cells = []
    for (r, c) in ws._cells.keys():
        if r >= 3 and 1 <= c <= max_col:
            del_cells.append(ws._cells[(r, c)])
            rows_touched.add(r)

    for cell in del_cells:
        del cell

    for r in rows_touched:
        if r in ws.row_dimensions:
            del ws.row_dimensions[r]

    for c in range(1, max_col + 1):
        ws.cell(2, c).value = None

    if not data or len(data) == 0:
        bio = BytesIO()
        wb.save(bio)
        return bio.getvalue()

    def apply_template_style(row_idx: int) -> None:
        if template_row_height is not None:
            ws.row_dimensions[row_idx].height = template_row_height
        for col_idx in range(1, max_col + 1):
            cell = ws.cell(row_idx, col_idx)
            cell._style = template_styles[col_idx - 1]
            cell.number_format = template_numfmts[col_idx - 1]

    def write_row_values(row_idx: int, row: Dict[str, Any]) -> None:
        for cell_id, cell_value in row.items():
            col_idx = template_headers_mapper.get(cell_id)

            if not col_idx:
                continue
            try:
                ws.cell(row=row_idx, column=col_idx).value = cell_value
            except Exception as e:
                print(cell_id, cell_value)
                raise e

    for i, row in enumerate(data):
        row_idx = 2 + i
        if row_idx != 2:
            apply_template_style(row_idx)
        write_row_values(row_idx, row)

    bio = BytesIO()
    wb.save(bio)
    return bio.getvalue()

