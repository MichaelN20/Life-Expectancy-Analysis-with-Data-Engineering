'''
=================================================
Milestone 3

Nama  : Michael Nathaniel
Batch : FTDS-009-HCK

Program ini dibuat untuk melakukan automatisasi transform dan load data dari PostgreSQL ke ElasticSearch. Adapun dataset yang dipakai adalah dataset mengenai penjualan mobil di Indonesia selama tahun 2020.
=================================================
'''

import datetime as dt
from datetime import timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

import pandas as pd

import psycopg2 as db
# from sql

from elasticsearch import Elasticsearch

def fetch_and_save_data(dbname: str, host: str, user: str, password: str, port: str):
    '''
    Fungsi ini ditujukan untuk mengambil data dari PostgreSQL untuk selanjutnya dilakukan Data Cleaning.

    Parameters:
    dbname: string - nama database dimana data disimpan
    host: string - nama host/ address
    user: string - nama user
    password: string - password user
    port: str - kode port

    Output:
    df: dataframe hasil fetch dari PostgreSQL
    P2M3_michael_nathaniel_data_raw.csv: file csv dari dataframe 'df'
        
    Contoh penggunaan:
    df = pd.read_sql("select * from table_m3", conn)
    df.to_csv('P2M3_michael_nathaniel_data_raw.csv', index=False)
    '''
    
    conn_string = f"host='{host}' dbname='{dbname}' user='{user}' password='{password}' port='{port}'"
    conn = db.connect(conn_string)
    df = pd.read_sql("select * from table_m3", conn)
    df.to_csv('P2M3_michael_nathaniel_data_raw.csv', index=False)


def clean_data(data_path):
    '''
    Fungsi ini ditujukan untuk menjalankan data cleaning terhadap data raw.

    Parameters:
    datapath: alamat file data berada

    Output:
    df: dataframe - dataframe yang telah dilakukan data cleaning
    P2M3_michael_nathaniel_data_clean.csv: csv - file csv dari export dataframe 'df' setelah data cleaning
    
    Contoh penggunaan:
    df['Bedtime'] = pd.to_datetime(df['Bedtime'])
    df['Wakeup time'] = pd.to_datetime(df['Wakeup time'])
    columns_to_convert = ['Awakenings', 'Caffeine consumption', 'Alcohol consumption', 'Exercise frequency']
    df[columns_to_convert] = df[columns_to_convert].astype(float)
    df.columns = df.columns.str.lower()
    df = df.apply(lambda x: x.str.strip() if x.dtype == "object" else x)
    df.columns = df.columns.str.replace(' ', '_')
    df = df.drop_duplicates()
    df = df.dropna()
    df.to_csv('P2M3_michael_nathaniel_data_clean.csv')
    '''

    # 1. Definisikan dataframe
    df = pd.read_csv(data_path)
    
    # 2. Ubah tipe data kolom bedtime dan waketime dari object menjadi datetime
    df['Bedtime'] = pd.to_datetime(df['Bedtime'])
    df['Wakeup time'] = pd.to_datetime(df['Wakeup time'])
    
    # 3. Ubah tipe data kolom Awakenings, Caffeine consumption, Alcohol consumption, dan Exercise frequency
    columns_to_convert = ['Awakenings', 'Caffeine consumption', 'Alcohol consumption', 'Exercise frequency']
    df[columns_to_convert] = df[columns_to_convert].astype(float)
    
    # 4. Ubah semua nama kolom menjadi lowercase
    df.columns = df.columns.str.lower()
    
    # 5. Menghapus leading trailing spaces pada setiap kolom
    df = df.apply(lambda x: x.str.strip() if x.dtype == "object" else x)
    
    # 6. Mengganti spasi menjadi underscore
    df.columns = df.columns.str.replace(' ', '_')

    # 7. Menghapus kolom dupliat
    df = df.drop_duplicates()

    # 8. Menghapus kolom dengan missing value
    df = df.dropna()
    
    # 9. Menyimpan cleaned data
    df.to_csv('P2M3_michael_nathaniel_data_clean.csv')


def insert_data_to_elasticsearch(data_path, index_name):
    '''
    Fungsi ini ditujukan untuk mengambil data dari csv untuk selanjutnya dimasukkan ke Elastic Search.
    
    Parameters:
    data_path: alamat file data berada
    index_name: nama index yang mewakili data

    Output:
    doc
    res
    
    Contoh penggunaan:
    doc = r.to_json()
    res = es.index(index="P2M3_michael_nathaniel_data_clean",body = doc)
    '''

    es = Elasticsearch(hosts='http://elasticsearch:9200') 
    df = pd.read_csv(data_path)
    for i,r in df.iterrows():
        doc = r.to_json()
        res = es.index(index=index_name,body = doc)


# DAG Configuration

default_args = {
    'owner': 'michael',
    'start_date': dt.datetime(2023, 11, 23),
    'retries': 1,
    'retry_delay': timedelta(minutes = 15),
}

fetch_and_save_data_args = {
    'dbname': 'P2M3_michael_nathaniel_database',
    'host': 'postgres',
    'user': 'postgres',
    'password': 'postgres',
    'port': '5432'
}

clean_data_args = {
    'data_path': 'P2M3_michael_nathaniel_data_raw.csv'
}

insert_data_to_elasticsearch_args = {
    'data_path': 'P2M3_michael_nathaniel_data_clean.csv',
    'index_name': 'michael_diabetes_data_clean_container',
}

with DAG('Michael_Fetch_CleanData_ElasticSearch',
         default_args = default_args,
         schedule_interval = "30 23 * * *",  # Setiap jam 06:30 WIB UTC+7
         catchup = False) as dag:

    fetchAndSave = PythonOperator(task_id = 'fetch_save',
                                  python_callable = fetch_and_save_data,
                                  op_kwargs = fetch_and_save_data_args)
    
    cleanData = PythonOperator(task_id = 'clean',
                               python_callable = clean_data,
                               op_kwargs = clean_data_args)
    
    insertToElastic = PythonOperator(task_id = 'insert_elastic',
                                     python_callable = insert_data_to_elasticsearch,
                                     op_kwargs = insert_data_to_elasticsearch_args)

fetchAndSave >> cleanData >> insertToElastic
