# IMPORT MODULE
import json
import pandas as pd
import numpy as np

# IMPORT SCRIPT
from script.mysql import MySQL
from script.postgresql import PostgreSQL

# IMPORT SQL file dan memasukkan fungsi untuk membuat dimension table dan fact table pada star schema
from sql.query import create_table_dim, create_table_fact

#membuka file credential
with open ('credential.json', "r") as cred:
        credential = json.load(cred)

#fungsi untuk memasukkan raw data kedalam datalake
def insert_raw_data():
  #authentifikasi mySQL pada file credential yang telah disiapkan
  mysql_auth = MySQL(credential['mysql_lake'])
  engine, engine_conn = mysql_auth.connect()

  #membuka raw data json
  with open ('./data/data_covid.json', "r") as data:
    data = json.load(data)

  #mengubah konten dari json kedalam dataframe
  df = pd.DataFrame(data['data']['content'])
 
  #mengubah nama kolom agar memiliki string cast lower 
  df.columns = [x.lower() for x in df.columns.to_list()]
  #memasukkan dataframe kedalam database yang terkoneksi, opsi replace akan mengganti data apabila table dengan nama yang sama telah ada
  df.to_sql(name='trisna_raw_covid', con=engine, if_exists="replace", index=False)
  #engine.dispose() berfungsi untuk menghapus kumpulan koneksi yang tersimpan dan menggantinya dengan yang baru dan amsih kosong
  engine.dispose()

#fungsi untuk membuat star schema pada datawarehouse  
def create_star_schema(schema_name):
  #melakukan autentifikasi pad datawarehouse PostgreSQL
  postgre_auth = PostgreSQL(credential['postgresql_warehouse'])
  conn, cursor = postgre_auth.connect(conn_type='cursor')

  #menjalankan SQL syntax yang terdapat pada fungsi create_table_dim untuk membuat dimension table
  query_dim = create_table_dim(schema=schema_name)
  #execute akan menjalankan query yang terdapat pada parameter fungsi >> quey_dim (query untuk membuat table dimension)
  cursor.execute(query_dim)
  #conn.commit() digunakan untuk menyimpan seluruh modifikasi yang dibuat sejak komit terakhir.
  conn.commit()
 
  #menjalankan operasi yang sama untuk membuat fact table pada datawarehouse
  query_fact = create_table_fact(schema=schema_name)
  cursor.execute(query_fact)
  conn.commit()

  #menutup koneksi pada database yang ada
  cursor.close()
  conn.close()

#fungsi mengembalikan dataframe untuk mengisi table province
def create_dim_province(data):
    #nama kolom awal pada raw data  
    column_input = ["kode_prov", "nama_prov"]
    #nama kolom output dataframe
    column_output = ["province_id", "province_name"]

    data = data[column_input]
    data = data.drop_duplicates(column_input)
    data.columns = column_output

    return data

#fungsi mengembalikan dataframe untuk mengisi table district
def create_dim_district(data):
    column_input = ["kode_kab", "kode_prov", "nama_kab"]
    column_output = ["district_id", "province_id", "district_name"]

    data = data[column_input]
    data = data.drop_duplicates(column_input)
    data.columns = column_output

    return data

#fungsi mengembalikan dataframe untuk mengisi table case
def create_dim_case(data):
    column_input = ["suspect_diisolasi", "suspect_discarded", "closecontact_dikarantina", "closecontact_discarded", "probable_diisolasi", "probable_discarded", "confirmation_sembuh", "confirmation_meninggal", "suspect_meninggal", "closecontact_meninggal", "probable_meninggal"]
    column_output = ["id", "status_name", "status_detail", "status"]

    data = data[column_input]
    data = data[:1]
    #unpivot data
    data = data.melt(var_name="status", value_name="total")
    data = data.drop_duplicates("status").sort_values("status")
    
    data['id'] = np.arange(1, data.shape[0]+1)
    #mengisikan kolom status_name dan status_detail dari hasil pemecahan kolom status
    data[['status_name', 'status_detail']] = data['status'].str.split('_', n=1, expand=True)
    data = data[column_output]

    return data

#fungsi mengembalikan dataframe untuk mengisi table province_daily
def create_fact_province_daily(data, dim_case_df):
    column_input = ["tanggal", "kode_prov", "suspect_diisolasi", "suspect_discarded", "closecontact_dikarantina", "closecontact_discarded", "probable_diisolasi", "probable_discarded", "confirmation_sembuh", "confirmation_meninggal", "suspect_meninggal", "closecontact_meninggal", "probable_meninggal"]
    column_output = ['date', 'province_id', 'status', 'total']

    # AGGREGATE
    data = data[column_input]
    data = data.melt(id_vars=["tanggal", "kode_prov"], var_name="status", value_name="total").sort_values(["tanggal", "kode_prov", "status"])
    data = data.groupby(by=['tanggal', 'kode_prov', 'status']).sum()
    data = data.reset_index()

    # REFORMAT
    data.columns = column_output
    data['id'] = np.arange(1, data.shape[0]+1)

    # MERGE dengan case dataframe
    dim_case_df = dim_case_df.rename({'id': 'case_id'}, axis=1)
    data = pd.merge(data, dim_case_df, how='inner', on='status')
    
    data = data[['id', 'province_id', 'case_id', 'date', 'total']]
    
    return data

#fungsi mengembalikan dataframe untuk mengisi table province_monthly
def create_fact_province_monthly(data, dim_case_df):
    column_input = ["tanggal", "kode_prov", "suspect_diisolasi", "suspect_discarded", "closecontact_dikarantina", "closecontact_discarded", "probable_diisolasi", "probable_discarded", "confirmation_sembuh", "confirmation_meninggal", "suspect_meninggal", "closecontact_meninggal", "probable_meninggal"]
    column_output = ['month', 'province_id', 'status', 'total']

    # AGGREGATE
    data = data[column_input]
    data['tanggal'] = data['tanggal'].apply(lambda x: x[:7])
    data = data.melt(id_vars=["tanggal", "kode_prov"], var_name="status", value_name="total").sort_values(["tanggal", "kode_prov", "status"])
    data = data.groupby(by=['tanggal', 'kode_prov', 'status']).sum()
    data = data.reset_index()

    # REFORMAT
    data.columns = column_output
    data['id'] = np.arange(1, data.shape[0]+1)

    # MERGE
    dim_case_df = dim_case_df.rename({'id': 'case_id'}, axis=1)
    data = pd.merge(data, dim_case_df, how='inner', on='status')

    data = data[['id', 'province_id', 'case_id', 'month', 'total']]
    
    return data

#fungsi mengembalikan dataframe untuk mengisi table province_yearly
def create_fact_province_yearly(data, dim_case_df):
    column_input = ["tanggal", "kode_prov", "suspect_diisolasi", "suspect_discarded", "closecontact_dikarantina", "closecontact_discarded", "probable_diisolasi", "probable_discarded", "confirmation_sembuh", "confirmation_meninggal", "suspect_meninggal", "closecontact_meninggal", "probable_meninggal"]
    column_output = ['year', 'province_id', 'status', 'total']

    # AGGREGATE
    data = data[column_input]
    data['tanggal'] = data['tanggal'].apply(lambda x: x[:4])
    data = data.melt(id_vars=["tanggal", "kode_prov"], var_name="status", value_name="total").sort_values(["tanggal", "kode_prov", "status"])
    data = data.groupby(by=['tanggal', 'kode_prov', 'status']).sum()
    data = data.reset_index()

    # REFORMAT
    data.columns = column_output
    data['id'] = np.arange(1, data.shape[0]+1)

    # MERGE
    dim_case_df = dim_case_df.rename({'id': 'case_id'}, axis=1)
    data = pd.merge(data, dim_case_df, how='inner', on='status')

    data = data[['id', 'province_id', 'case_id', 'year', 'total']]
    
    return data

#fungsi mengembalikan dataframe untuk mengisi table district_daily
def create_fact_district_daily(data, dim_case_df):
    column_input = ["tanggal", "kode_kab", "suspect_diisolasi", "suspect_discarded", "closecontact_dikarantina", "closecontact_discarded", "probable_diisolasi", "probable_discarded", "confirmation_sembuh", "confirmation_meninggal", "suspect_meninggal", "closecontact_meninggal", "probable_meninggal"]
    column_output = ['date', 'district_id', 'status', 'total']

    # AGGREGATE
    data = data[column_input]
    data = data.melt(id_vars=["tanggal", "kode_kab"], var_name="status", value_name="total").sort_values(["tanggal", "kode_kab", "status"])
    data = data.groupby(by=['tanggal', 'kode_kab', 'status']).sum()
    data = data.reset_index()

    # REFORMAT
    data.columns = column_output
    data['id'] = np.arange(1, data.shape[0]+1)

    # MERGE dengan case dataframe
    dim_case_df = dim_case_df.rename({'id': 'case_id'}, axis=1)
    data = pd.merge(data, dim_case_df, how='inner', on='status')
    
    data = data[['id', 'district_id', 'case_id', 'date', 'total']]
    
    return data

#fungsi mengembalikan dataframe untuk mengisi table district_monthly
def create_fact_district_monthly(data, dim_case_df):
    column_input = ["tanggal", "kode_kab", "suspect_diisolasi", "suspect_discarded", "closecontact_dikarantina", "closecontact_discarded", "probable_diisolasi", "probable_discarded", "confirmation_sembuh", "confirmation_meninggal", "suspect_meninggal", "closecontact_meninggal", "probable_meninggal"]
    column_output = ['month', 'district_id', 'status', 'total']

    # AGGREGATE
    data = data[column_input]
    data['tanggal'] = data['tanggal'].apply(lambda x: x[:7])
    data = data.melt(id_vars=["tanggal", "kode_kab"], var_name="status", value_name="total").sort_values(["tanggal", "kode_kab", "status"])
    data = data.groupby(by=['tanggal', 'kode_kab', 'status']).sum()
    data = data.reset_index()

    # REFORMAT
    data.columns = column_output
    data['id'] = np.arange(1, data.shape[0]+1)

    # MERGE
    dim_case_df = dim_case_df.rename({'id': 'case_id'}, axis=1)
    data = pd.merge(data, dim_case_df, how='inner', on='status')

    data = data[['id', 'district_id', 'case_id', 'month', 'total']]
    
    return data

#fungsi mengembalikan dataframe untuk mengisi table district_yearly
def create_fact_district_yearly(data, dim_case_df):
    column_input = ["tanggal", "kode_kab", "suspect_diisolasi", "suspect_discarded", "closecontact_dikarantina", "closecontact_discarded", "probable_diisolasi", "probable_discarded", "confirmation_sembuh", "confirmation_meninggal", "suspect_meninggal", "closecontact_meninggal", "probable_meninggal"]
    column_output = ['year', 'district_id', 'status', 'total']

    # AGGREGATE
    data = data[column_input]
    data['tanggal'] = data['tanggal'].apply(lambda x: x[:4])
    data = data.melt(id_vars=["tanggal", "kode_kab"], var_name="status", value_name="total").sort_values(["tanggal", "kode_kab", "status"])
    data = data.groupby(by=['tanggal', 'kode_kab', 'status']).sum()
    data = data.reset_index()

    # REFORMAT
    data.columns = column_output
    data['id'] = np.arange(1, data.shape[0]+1)
    
    # MERGE
    dim_case_df = dim_case_df.rename({'id': 'case_id'}, axis=1)
    data = pd.merge(data, dim_case_df, how='inner', on='status')

    data = data[['id', 'district_id', 'case_id', 'year', 'total']]
    
    return data

#fungsi memasukkan data menuju data warehouse
def insert_raw_to_warehouse(table_lakes_name,schema_name):
    #autentifikasi datalake MySQL
    mysql_auth = MySQL(credential['mysql_lake'])
    engine, engine_conn = mysql_auth.connect()
    data = pd.read_sql(sql=table_lakes_name, con=engine)
    engine.dispose()

    #filter untuk hanya mengambil kolom yang diperlukan
    column = ["tanggal", "kode_prov", "nama_prov", "kode_kab", "nama_kab", "suspect_diisolasi", "suspect_discarded", "closecontact_dikarantina", "closecontact_discarded", "probable_diisolasi", "probable_discarded", "confirmation_sembuh", "confirmation_meninggal", "suspect_meninggal", "closecontact_meninggal", "probable_meninggal"]
    data = data[column]

    #membuat dimension dataframe
    dim_province = create_dim_province(data)
    dim_district = create_dim_district(data)
    dim_case = create_dim_case(data)

    #membuat fact dataframe
    fact_province_daily = create_fact_province_daily(data, dim_case)
    fact_province_monthly = create_fact_province_monthly(data, dim_case)
    fact_province_yearly = create_fact_province_yearly(data, dim_case)
    fact_district_daily = create_fact_district_daily(data, dim_case)
    fact_district_monthly = create_fact_district_monthly(data, dim_case)
    fact_district_yearly = create_fact_district_yearly(data, dim_case)

    #autentifikasi database PostgreSQL
    postgre_auth = PostgreSQL(credential['postgresql_warehouse'])
    engine, engine_conn = postgre_auth.connect(conn_type='engine')

    #memasukkan dimmension dataframes ke datawarehouse
    dim_province.to_sql('dim_province', schema=schema_name, con=engine, index=False, if_exists='replace')
    dim_district.to_sql('dim_district', schema=schema_name, con=engine, index=False, if_exists='replace')
    dim_case.to_sql('dim_case', schema=schema_name, con=engine, index=False, if_exists='replace')

    #memasukkan fact dataframe ke datawarehouse
    fact_province_daily.to_sql('fact_province_daily', schema=schema_name, con=engine, index=False, if_exists='replace')
    fact_province_monthly.to_sql('fact_province_monthly', schema=schema_name, con=engine, index=False, if_exists='replace')
    fact_province_yearly.to_sql('fact_province_yearly', schema=schema_name, con=engine, index=False, if_exists='replace')
    fact_district_daily.to_sql('fact_district_daily', schema=schema_name, con=engine, index=False, if_exists='replace')
    fact_district_monthly.to_sql('fact_district_monthly', schema=schema_name, con=engine, index=False, if_exists='replace')
    fact_district_yearly.to_sql('fact_district_yearly', schema=schema_name, con=engine, index=False, if_exists='replace')

    #mengkosongkan koneksi
    engine.dispose()


if __name__ == '__main__':
  insert_raw_data()
  create_star_schema(schema_name='project_3_trisna')
  insert_raw_to_warehouse(table_lakes_name='trisna_raw_covid',schema_name='project_3_trisna')