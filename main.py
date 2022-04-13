# IMPORT MODULE
import json
import pandas as pd

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
  df.to_sql(name='aji_raw_covid', con=engine, if_exists="replace", index=False)
  engine.dispose()

def create_star_schema():
  postgre_auth = PostgreSQL(credential['postgresql_warehouse'])
  conn, cursor = postgre_auth.connect(conn_type='cursor')

  query_dim = create_table_dim(schema='aji')
  cursor.execute(query_dim)
  conn.commit()

  query_fact = create_table_fact(schema='aji')
  cursor.execute(query_fact)
  conn.commit()

  cursor.close()
  conn.close()


if __name__ == '__main__':
  # insert_raw_data()
  create_star_schema()