# IMPORT MODULE
import json
import pandas as pd
import numpy as np

# IMPORT SCRIPT
from script.mysql import MySQL
from script.postgresql import PostgreSQL

# IMPORT SQL
from sql.query import create_table_dim, create_table_fact


with open ('credential.json', "r") as cred:
        credential = json.load(cred)

def insert_raw_data():
  mysql_auth = MySQL(credential['mysql_lake'])
  engine, engine_conn = mysql_auth.connect()

  with open ('./data/data_covid.json', "r") as data:
    data = json.load(data)

  df = pd.DataFrame(data['data']['content'])

  df.columns = [x.lower() for x in df.columns.to_list()]
  df.to_sql(name='trisna_raw_covid', con=engine, if_exists="replace", index=False)
  engine.dispose()
  
def create_star_schema(schema_name):
  postgre_auth = PostgreSQL(credential['postgresql_warehouse'])
  conn, cursor = postgre_auth.connect(conn_type='cursor')

  query_dim = create_table_dim(schema=schema_name)
  cursor.execute(query_dim)
  conn.commit()

  query_fact = create_table_fact(schema=schema_name)
  cursor.execute(query_fact)
  conn.commit()

  cursor.close()
  conn.close()


def insert_dim_province(data):
    column_start = ["kode_prov", "nama_prov"]
    column_end = ["province_id", "province_name"]

    data = data[column_start]
    data = data.drop_duplicates(column_start)
    data.columns = column_end

    return data


def insert_dim_district(data):
    column_start = ["kode_kab", "kode_prov", "nama_kab"]
    column_end = ["district_id", "province_id", "district_name"]

    data = data[column_start]
    data = data.drop_duplicates(column_start)
    data.columns = column_end

    return data


def insert_dim_case(data):
    column_start = ["suspect_diisolasi", "suspect_discarded", "closecontact_dikarantina", "closecontact_discarded", "probable_diisolasi", "probable_discarded", "confirmation_sembuh", "confirmation_meninggal", "suspect_meninggal", "closecontact_meninggal", "probable_meninggal"]
    column_end = ["id", "status_name", "status_detail", "status"]

    data = data[column_start]
    data = data[:1]
    data = data.melt(var_name="status", value_name="total")
    data = data.drop_duplicates("status").sort_values("status")
    
    data['id'] = np.arange(1, data.shape[0]+1)
    data[['status_name', 'status_detail']] = data['status'].str.split('_', n=1, expand=True)
    data = data[column_end]

    return data


def insert_fact_province_daily(data, dim_case):
    column_start = ["tanggal", "kode_prov", "suspect_diisolasi", "suspect_discarded", "closecontact_dikarantina", "closecontact_discarded", "probable_diisolasi", "probable_discarded", "confirmation_sembuh", "confirmation_meninggal", "suspect_meninggal", "closecontact_meninggal", "probable_meninggal"]
    column_end = ['date', 'province_id', 'status', 'total']

    # AGGREGATE
    data = data[column_start]
    data = data.melt(id_vars=["tanggal", "kode_prov"], var_name="status", value_name="total").sort_values(["tanggal", "kode_prov", "status"])
    data = data.groupby(by=['tanggal', 'kode_prov', 'status']).sum()
    data = data.reset_index()

    # REFORMAT
    data.columns = column_end
    data['id'] = np.arange(1, data.shape[0]+1)

    # MERGE
    dim_case = dim_case.rename({'id': 'case_id'}, axis=1)
    data = pd.merge(data, dim_case, how='inner', on='status')
    
    data = data[['id', 'province_id', 'case_id', 'date', 'total']]
    
    return data


def insert_fact_province_monthly(data, dim_case):
    column_start = ["tanggal", "kode_prov", "suspect_diisolasi", "suspect_discarded", "closecontact_dikarantina", "closecontact_discarded", "probable_diisolasi", "probable_discarded", "confirmation_sembuh", "confirmation_meninggal", "suspect_meninggal", "closecontact_meninggal", "probable_meninggal"]
    column_end = ['month', 'province_id', 'status', 'total']

    # AGGREGATE
    data = data[column_start]
    data['tanggal'] = data['tanggal'].apply(lambda x: x[:7])
    data = data.melt(id_vars=["tanggal", "kode_prov"], var_name="status", value_name="total").sort_values(["tanggal", "kode_prov", "status"])
    data = data.groupby(by=['tanggal', 'kode_prov', 'status']).sum()
    data = data.reset_index()

    # REFORMAT
    data.columns = column_end
    data['id'] = np.arange(1, data.shape[0]+1)

    # MERGE
    dim_case = dim_case.rename({'id': 'case_id'}, axis=1)
    data = pd.merge(data, dim_case, how='inner', on='status')

    data = data[['id', 'province_id', 'case_id', 'month', 'total']]
    
    return data


def insert_fact_province_yearly(data, dim_case):
    column_start = ["tanggal", "kode_prov", "suspect_diisolasi", "suspect_discarded", "closecontact_dikarantina", "closecontact_discarded", "probable_diisolasi", "probable_discarded", "confirmation_sembuh", "confirmation_meninggal", "suspect_meninggal", "closecontact_meninggal", "probable_meninggal"]
    column_end = ['year', 'province_id', 'status', 'total']

    # AGGREGATE
    data = data[column_start]
    data['tanggal'] = data['tanggal'].apply(lambda x: x[:4])
    data = data.melt(id_vars=["tanggal", "kode_prov"], var_name="status", value_name="total").sort_values(["tanggal", "kode_prov", "status"])
    data = data.groupby(by=['tanggal', 'kode_prov', 'status']).sum()
    data = data.reset_index()

    # REFORMAT
    data.columns = column_end
    data['id'] = np.arange(1, data.shape[0]+1)

    # MERGE
    dim_case = dim_case.rename({'id': 'case_id'}, axis=1)
    data = pd.merge(data, dim_case, how='inner', on='status')

    data = data[['id', 'province_id', 'case_id', 'year', 'total']]
    
    return data


def insert_fact_district_monthly(data, dim_case):
    column_start = ["tanggal", "kode_kab", "suspect_diisolasi", "suspect_discarded", "closecontact_dikarantina", "closecontact_discarded", "probable_diisolasi", "probable_discarded", "confirmation_sembuh", "confirmation_meninggal", "suspect_meninggal", "closecontact_meninggal", "probable_meninggal"]
    column_end = ['month', 'district_id', 'status', 'total']

    # AGGREGATE
    data = data[column_start]
    data['tanggal'] = data['tanggal'].apply(lambda x: x[:7])
    data = data.melt(id_vars=["tanggal", "kode_kab"], var_name="status", value_name="total").sort_values(["tanggal", "kode_kab", "status"])
    data = data.groupby(by=['tanggal', 'kode_kab', 'status']).sum()
    data = data.reset_index()

    # REFORMAT
    data.columns = column_end
    data['id'] = np.arange(1, data.shape[0]+1)

    # MERGE
    dim_case = dim_case.rename({'id': 'case_id'}, axis=1)
    data = pd.merge(data, dim_case, how='inner', on='status')

    data = data[['id', 'district_id', 'case_id', 'month', 'total']]
    
    return data


def insert_fact_district_yearly(data, dim_case):
    column_start = ["tanggal", "kode_kab", "suspect_diisolasi", "suspect_discarded", "closecontact_dikarantina", "closecontact_discarded", "probable_diisolasi", "probable_discarded", "confirmation_sembuh", "confirmation_meninggal", "suspect_meninggal", "closecontact_meninggal", "probable_meninggal"]
    column_end = ['year', 'district_id', 'status', 'total']

    # AGGREGATE
    data = data[column_start]
    data['tanggal'] = data['tanggal'].apply(lambda x: x[:4])
    data = data.melt(id_vars=["tanggal", "kode_kab"], var_name="status", value_name="total").sort_values(["tanggal", "kode_kab", "status"])
    data = data.groupby(by=['tanggal', 'kode_kab', 'status']).sum()
    data = data.reset_index()

    # REFORMAT
    data.columns = column_end
    data['id'] = np.arange(1, data.shape[0]+1)
    
    # MERGE
    dim_case = dim_case.rename({'id': 'case_id'}, axis=1)
    data = pd.merge(data, dim_case, how='inner', on='status')

    data = data[['id', 'district_id', 'case_id', 'year', 'total']]
    
    return data


def insert_raw_to_warehouse(table_lakes_name,schema_name):
    mysql_auth = MySQL(credential['mysql_lake'])
    engine, engine_conn = mysql_auth.connect()
    data = pd.read_sql(sql=table_lakes_name, con=engine)
    engine.dispose()

    # filter needed column
    column = ["tanggal", "kode_prov", "nama_prov", "kode_kab", "nama_kab", "suspect_diisolasi", "suspect_discarded", "closecontact_dikarantina", "closecontact_discarded", "probable_diisolasi", "probable_discarded", "confirmation_sembuh", "confirmation_meninggal", "suspect_meninggal", "closecontact_meninggal", "probable_meninggal"]
    data = data[column]

    dim_province = insert_dim_province(data)
    dim_district = insert_dim_district(data)
    dim_case = insert_dim_case(data)

    fact_province_daily = insert_fact_province_daily(data, dim_case)
    fact_province_monthly = insert_fact_province_monthly(data, dim_case)
    fact_province_yearly = insert_fact_province_yearly(data, dim_case)
    fact_district_monthly = insert_fact_district_monthly(data, dim_case)
    fact_district_yearly = insert_fact_district_yearly(data, dim_case)

    postgre_auth = PostgreSQL(credential['postgresql_warehouse'])
    engine, engine_conn = postgre_auth.connect(conn_type='engine')

    dim_province.to_sql('dim_province', schema=schema_name, con=engine, index=False, if_exists='replace')
    dim_district.to_sql('dim_district', schema=schema_name, con=engine, index=False, if_exists='replace')
    dim_case.to_sql('dim_case', schema=schema_name, con=engine, index=False, if_exists='replace')

    fact_province_daily.to_sql('fact_province_daily', schema=schema_name, con=engine, index=False, if_exists='replace')
    fact_province_monthly.to_sql('fact_province_monthly', schema=schema_name, con=engine, index=False, if_exists='replace')
    fact_province_yearly.to_sql('fact_province_yearly', schema=schema_name, con=engine, index=False, if_exists='replace')
    fact_district_monthly.to_sql('fact_district_monthly', schema=schema_name, con=engine, index=False, if_exists='replace')
    fact_district_yearly.to_sql('fact_district_yearly', schema=schema_name, con=engine, index=False, if_exists='replace')

    engine.dispose()


if __name__ == '__main__':
  insert_raw_data()
  create_star_schema(schema_name='project_3_trisna')
  insert_raw_to_warehouse(table_lakes_name='trisna_raw_covid',schema_name='project_3_trisna')