
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.hooks.base_hook import BaseHook
from airflow.operators.python import PythonOperator
from sqlalchemy import create_engine,MetaData, Table
from sqlalchemy.orm import sessionmaker
from sqlalchemy.dialects.postgresql import insert
import pandas as pd
import requests
from bs4 import BeautifulSoup
from datetime import datetime,timedelta
import time

url = 'https://oraculus.mx/presidente2024/'
header = {"user-agent": 'Mozilla/5.0 \
        (Windows NT 10.0; Win64; x64; rv:109.0)\
        Gecko/20100101 Firefox/113.0'}


def _load(df,tbl, if_exist):
    conn = BaseHook.get_connection('postgresown')
    engine = create_engine(f'postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}')
    try:
        with engine.connect() as connection:
            df.to_sql(f'{tbl}', connection, if_exists = f'{if_exist}', index=False, chunksize=1000)
            current_time = time.ctime()
            print(f"Data imported successful on {current_time}")
    except Exception as e:
        print("Data load error: " + str(e))


def get_candidates(url, headers):
    try:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'html.parser')
            container = soup.find(id='table_1')
            conteiner_text = container.find('tbody')
            candidatos = []
            for row in conteiner_text.find_all('tr'):
                columns = row.find_all('td')
                estimacion = columns[0].text.strip()
                encuestadora = columns[1].text.strip()
                fecha = columns[2].text.strip()
                xg = columns[4].text.strip()
                cs = columns[5].text.strip()
                jam = columns[6].text.strip()
                candidatos.append({'Estimacion': estimacion,
                                   'Encuestadora': encuestadora,
                                   'Fecha': fecha,
                                       'XG': xg, 'CS': cs, 'JAM': jam,
                                       'time_insert': pd.to_datetime('today')})
            df_candidatos = pd.DataFrame(candidatos)        
            _load(df_candidatos,'candidatos', 'replace')                    
        else:
            print(f"Failed to retrieve data from {url}. Status code: {response.status_code}")
            return False
    except requests.exceptions.RequestException as e:
        print("Error making HTTP request:", e)
        return False
    except Exception as e:
        print("An unexpected error occurred:", e)
        return False
    
def get_partidos(url, header):
    try:
        response = requests.get(url, headers=header)
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'html.parser')
            container = soup.find(id='table_2')
            conteiner_text = container.find('tbody')
            partidos = []
            for row in conteiner_text.find_all('tr'):
                columns = row.find_all('td')
                estimacion = columns[0].text.strip()
                encuestadora = columns[1].text.strip()
                fecha = columns[2].text.strip()
                pan = columns[4].text.strip()
                pri = columns[5].text.strip()
                prd = columns[6].text.strip()
                pvem = columns[7].text.strip()
                pt = columns[8].text.strip()
                mc = columns[9].text.strip()
                morena = columns[10].text.strip()
                nr = columns[11].text.strip()
                partidos.append({'Estimacion': estimacion,
                                 'Encuestadora': encuestadora,
                                 'Fecha': fecha,
                                 'PAN': pan, 'PRI': pri, 'PRD': prd,
                                 'PVEM': pvem, 'PT': pt, 'MC': mc,
                                 'MORENA': morena, 'NR': nr,
                                 'time_insert': pd.to_datetime('today')})
            df_partidos = pd.DataFrame(partidos)    
            _load(df_partidos,'partidos', 'replace')        
        else:
            print(f"Failed to retrieve data from {url}. Status code: {response.status_code}")
            return False
    except requests.exceptions.RequestException as e:
        print("Error making HTTP request:", e)
        return False
    except Exception as e:
        print("An unexpected error occurred:", e)
        return False
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 3, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG('web_scrap',  default_args=default_args, 
        schedule_interval='@daily', catchup=False,) as dag:
 
    create_table_p = PostgresOperator(
        task_id='create_table_p',
        postgres_conn_id='postgresown',
        sql='''
            CREATE TABLE IF NOT EXISTS partidos(
            id SERIAL UNIQUE PRIMARY KEY,
            Fecha VARCHAR(50),
            Estimacion VARCHAR(50),
            Encuestadora VARCHAR(50),
            PAN INT,
            PRI INT,	
            PRD INT,	
            PVEM INT,
            PT INT,
            MC INT,
            MORENA INT,
            NR INT,
            time_insert TIMESTAMP
            );
        '''
    )
    create_table_c = PostgresOperator(
        task_id='create_table_c',
        postgres_conn_id='postgresown',
        sql='''
            CREATE TABLE IF NOT EXISTS candidatos(
            id SERIAL UNIQUE PRIMARY KEY,	
            Fecha VARCHAR(50),
            Estimacion VARCHAR(50),
            Encuestadora VARCHAR(50),
            XG INT,
            CS INT,	
            JAM INT,
            time_insert TIMESTAMP
            );
        '''
    )

    is_api_available = HttpSensor(
        task_id='is_api_available',
        http_conn_id='oraculus_id',
        endpoint='/presidente2024/'
    )


    get_load_data_c =  PythonOperator(
        task_id='_get_candidates_c',
        python_callable=get_candidates,
        op_args = [url,header]
        
    )

    get_load_data_p =  PythonOperator(
        task_id='_get_candidates_p',
        python_callable=get_partidos,
        op_args = [url,header]
        
    )


    [create_table_p,create_table_c] >> is_api_available >> [get_load_data_c,get_load_data_p]