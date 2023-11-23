from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

import pandas as pd
import numpy as np
import sqlite3
from datetime import date, datetime


def loading_branch_service():
    df_branch_service = pd.read_json("/opt/airflow/data/branch_service_transaction_info.json")
    df_branch_service.to_parquet("/opt/airflow/data/df_1_branch_service.parquet")

def loading_customer_transaction():
    df_customer_transaction = pd.read_json("/opt/airflow/data/customer_transaction_info.json")
    df_customer_transaction.to_parquet("/opt/airflow/data/df_2_customer_transaction.parquet")

def drop_dups_branch_service():
     df_branch_service = pd.read_parquet('/opt/airflow/data/df_1_branch_service.parquet', engine='pyarrow')
     df_branch_service = df_branch_service.drop_duplicates(subset=['txn_id'])
     df_branch_service.to_parquet("/opt/airflow/data/df_3_branch_service_drop_dups.parquet")

def drop_dups_customer_transaction():
     df_customer_transaction = pd.read_parquet('/opt/airflow/data/df_2_customer_transaction.parquet', engine='pyarrow')
     df_customer_transaction = df_customer_transaction.drop_duplicates(subset=['txn_id'])
     df_customer_transaction.to_parquet("/opt/airflow/data/df_4_customer_transaction_drop_dups.parquet")

def merge_dataframe():
     df_branch_service = pd.read_parquet('/opt/airflow/data/df_3_branch_service_drop_dups.parquet', engine='pyarrow')
     df_customer_transaction = pd.read_parquet('/opt/airflow/data/df_4_customer_transaction_drop_dups.parquet', engine='pyarrow')
     df_merged = pd.merge(df_customer_transaction, df_branch_service)
     df_merged.to_parquet('/opt/airflow/data/df_5_no_dups_merged.parquet')

def fill_branch_name():
    df_merged = pd.read_parquet('/opt/airflow/data/df_5_no_dups_merged.parquet')
    df_merged['branch_name'] = df_merged.replace('',np.nan).groupby('txn_id')['branch_name'].transform('first')
    df_merged['branch_name'] = df_merged['branch_name'].ffill().bfill()
    df_merged.to_parquet('/opt/airflow/data/df_6_branch_name_filled.parquet')
    
def fill_price():
    df_merged = pd.read_parquet('/opt/airflow/data/df_6_branch_name_filled.parquet')
    df_merged['price'] = df_merged['price'].fillna(df_merged.groupby(['branch_name','service'])['price'].transform('mean'))
    df_merged.to_parquet('/opt/airflow/data/df_7_price_filled.parquet')
    
def standardize_last_name():
    df_merged = pd.read_parquet('/opt/airflow/data/df_7_price_filled.parquet')
    df_merged['last_name'] = df_merged['last_name'].str.replace('\W', '', regex=True)
    df_merged['last_name'] = df_merged['last_name'].str.upper()
    df_merged.to_parquet('/opt/airflow/data/df_8_standardized_last_name.parquet')
    
def standardize_first_name():
    df_merged = pd.read_parquet('/opt/airflow/data/df_8_standardized_last_name.parquet')
    df_merged['first_name'] = df_merged['first_name'].str.replace('\W', '', regex=True)
    df_merged['first_name'] = df_merged['first_name'].str.upper()
    df_merged.to_parquet('/opt/airflow/data/df_9_standardized_first_name.parquet')

def validate_dates():
    df_merged = pd.read_parquet('/opt/airflow/data/df_9_standardized_first_name.parquet')
    today = str(date.today())
    df_merged['avail_date'] = pd.to_datetime(df_merged['avail_date'], format='%Y-%m-%d')
    df_merged['birthday'] = pd.to_datetime(df_merged['birthday'], format='%Y-%m-%d')
    df_merged = df_merged[(df_merged['avail_date'] <= today) & (df_merged['birthday'] <= today)]
    df_merged = df_merged[(df_merged['avail_date'] > df_merged['birthday'])]
    df_merged.to_parquet('/opt/airflow/data/df_10_validated_dates.parquet')
    
def validate_price():
    df_merged = pd.read_parquet('/opt/airflow/data/df_10_validated_dates.parquet')
    df_merged['price'] = df_merged['price'].round(2)
    df_merged.to_parquet('/opt/airflow/data/df_11_validated_price.parquet')

def ingestion_to_db():
    df_merged = pd.read_parquet('/opt/airflow/data/df_11_validated_price.parquet')
    
    db_connect = sqlite3.connect('labex3')
    cur = db_connect.cursor()
    cur.execute("create table if not exists labex3(txn_id varchar(45) not null primary key, avail_date datetime default current_date, last_name varchar(20), first_name varchar(20), birthday datetime default current_date, branch_name varchar(30), service varchar(30), price double)")
    db_connect.commit()
    df_merged.to_sql('labex3', db_connect, if_exists='replace', index=False)
    
    cur.execute('CREATE VIEW IF NOT EXISTS weekly_report AS SELECT strftime(\'%W\',avail_date) AS weekNumber, service, SUM(price) AS weekly_sales FROM labex3 GROUP BY weekNumber, service ORDER BY weekNumber ASC')
    latest_df_merged = pd.read_sql_query('SELECT * from weekly_report', db_connect)
    latest_df_merged.to_parquet('/opt/airflow/data/df_12_weekly_view.parquet')
    
    
    
    
    
    
    
    
    
args = {
    'owner': 'Zrey',
    'start_date': days_ago(0),
}

dag = DAG(
    dag_id='labex3_dag',
    default_args=args,
     schedule_interval='@daily'
    
)

with dag:

        loading_branch_service = PythonOperator(
            task_id="loading_branch_service",
            python_callable=loading_branch_service
        )

        loading_customer_transaction = PythonOperator(
            task_id="loading_customer_transaction",
            python_callable=loading_customer_transaction
        )

        drop_dups_branch_service = PythonOperator(
            task_id="drop_dups_branch_service",
            python_callable=drop_dups_branch_service
        )

        drop_dups_customer_transaction = PythonOperator(
            task_id="drop_dups_customer_transaction",
            python_callable=drop_dups_customer_transaction
        )

        merge_dataframe = PythonOperator(
            task_id="merge_dataframe",
            python_callable=merge_dataframe
        )
        
        fill_branch_name = PythonOperator(
            task_id="fill_branch_name",
            python_callable=fill_branch_name
        )
        
        fill_price = PythonOperator(
            task_id="fill_price",
            python_callable=fill_price
        )
        
        standardize_last_name = PythonOperator(
            task_id="standardize_last_name",
            python_callable=standardize_last_name
        )
        
        standardize_first_name = PythonOperator(
            task_id="standardize_first_name",
            python_callable=standardize_first_name
        )
        
        validate_dates = PythonOperator(
            task_id="validate_dates",
            python_callable=validate_dates
        )
        
        validate_price = PythonOperator(
            task_id="validate_price",
            python_callable=validate_price
        )
        
        ingestion_to_db = PythonOperator(
            task_id="ingestion_to_db",
            python_callable=ingestion_to_db
        )


        

loading_branch_service >> drop_dups_branch_service
loading_customer_transaction >> drop_dups_customer_transaction
[drop_dups_branch_service, drop_dups_customer_transaction] >> merge_dataframe
merge_dataframe >> fill_branch_name >> fill_price >> standardize_last_name
standardize_last_name >> standardize_first_name >> validate_dates >> validate_price >> ingestion_to_db