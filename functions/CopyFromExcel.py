import pandas as pd
from sqlalchemy import create_engine

# Load the Excel file
excel_file = 'C:\\Users\\user\\Desktop\\Справочники\\tab_progn_spr_gaz_d314.xlsx'
df = pd.read_excel(excel_file)

# Create a connection to PostgreSQL
engine = create_engine('postgresql+psycopg2://postgres:postgres@localhost:5433/Progn_Spros')

# Insert the data into the PostgreSQL table
df.to_sql('tab_progn_spr_gaz_d314', engine, if_exists='replace', index=False)