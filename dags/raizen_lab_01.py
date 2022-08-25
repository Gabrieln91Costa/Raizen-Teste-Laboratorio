from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from sqlalchemy import create_engine
import sqlalchemy as db
import pandas.io.sql as sql
import MySQLdb
import pandas as pd
import datetime as dt
import numpy as np
import psycopg2
import xlrd
from airflow.providers.postgres.hooks.postgres import PostgresHook



def Step_01_Ingestion_Sheets_To_Stage():

    username = 'user'     
    password = 'password'     
    host = 'mysql_local'    
    port = '3306'         
    database = 'db'   

    df_sheet_index_sheet_01 = pd.read_excel('/opt/arquivo_saida/vendas-combustiveis-m3.xls','DPCache_m3')
    df_sheet_index_sheet_02 = pd.read_excel('/opt/arquivo_saida/vendas-combustiveis-m3.xls','DPCache_m3_2')

    df_sheet_index_sheet_01 = df_sheet_index_sheet_01.rename(columns={'Set': 'September', 'Out': 'October'})
    df_sheet_index_sheet_02 = df_sheet_index_sheet_02.rename(columns={'Set': 'September', 'Out': 'October'})

    df_sheet_01 = pd.DataFrame(data=df_sheet_index_sheet_01, columns=['COMBUSTÍVEL','ANO','REGIÃO','ESTADO','Jan','Fev','Mar','Abr','Mai','Jun','Jul','Ago','September','October','Nov','Dez','TOTAL'])
    df_sheet_02 = pd.DataFrame(data=df_sheet_index_sheet_02, columns=['COMBUSTÍVEL','ANO','REGIÃO','ESTADO','Jan','Fev','Mar','Abr','Mai','Jun','Jul','Ago','September','October','Nov','Dez','TOTAL'])


    engine = create_engine(f'mysql+pymysql://{username}:{password}@{host}:{port}/{database}')

    df_sheet_01.to_sql('stg_vendas_combustiveis', engine, index=False, if_exists='append')
    df_sheet_02.to_sql('stg_vendas_combustiveis', engine, index=False, if_exists='append')



def Step_02_Ingestion_Stage_To_Analitics():
    db = MySQLdb.connect(host="mysql_local", user="user", passwd="password", db="db")
    cursor=db.cursor()
    sql= "INSERT INTO db.tb_detalhes_de_vendas(year_and_month,uf,product,unit,volume) "\
        "select STR_TO_DATE(CONCAT(ANO,'01','01'),'%Y%m%d') AS 'year_month', REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(ESTADO,'RONDÔNIA','RO'),'ACRE','AC'),'AMAZONAS','AM'),'RORAIMA','RR'),'PARÁ','PA'),'AMAPÁ','AP'),'TOCANTINS','TO'),'MARANHÃO','MA'),'PIAUÍ','PI'),'CEARÁ','CE'),'RIO GRANDE DO NORTE','RN'),'PARAÍBA','PB'),'PERNAMBUCO','PE'),'ALAGOAS','AL'),'SERGIPE','SE'),'BAHIA','BA'),'MINAS GERAIS','MG'),'ESPÍRITO SANTO','ES'),'RIO DE JANEIRO','RJ'),'SÃO PAULO','SP'),'PARANÁ','PR'),'SANTA CATARINA','SC'),'RIO GRANDE DO SUL','RS'),'MATO GROSSO DO SUL','MS') ,'MATO GROSSO','MT'),'GOIÁS','GO'),'DISTRITO FEDERAL','DF') AS uf, COMBUSTÍVEL AS product, 'm3' AS unit, IFNULL(Jan,0) volume from db.stg_vendas_combustiveis "\
        "union all "\
        "select STR_TO_DATE(CONCAT(ANO,'02','01'),'%Y%m%d') AS 'year_month', REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(ESTADO,'RONDÔNIA','RO'),'ACRE','AC'),'AMAZONAS','AM'),'RORAIMA','RR'),'PARÁ','PA'),'AMAPÁ','AP'),'TOCANTINS','TO'),'MARANHÃO','MA'),'PIAUÍ','PI'),'CEARÁ','CE'),'RIO GRANDE DO NORTE','RN'),'PARAÍBA','PB'),'PERNAMBUCO','PE'),'ALAGOAS','AL'),'SERGIPE','SE'),'BAHIA','BA'),'MINAS GERAIS','MG'),'ESPÍRITO SANTO','ES'),'RIO DE JANEIRO','RJ'),'SÃO PAULO','SP'),'PARANÁ','PR'),'SANTA CATARINA','SC'),'RIO GRANDE DO SUL','RS'),'MATO GROSSO DO SUL','MS') ,'MATO GROSSO','MT'),'GOIÁS','GO'),'DISTRITO FEDERAL','DF') AS uf, COMBUSTÍVEL AS product, 'm3' AS unit, IFNULL(Fev,0) volume from db.stg_vendas_combustiveis "\
        "union all "\
        "select STR_TO_DATE(CONCAT(ANO,'03','01'),'%Y%m%d') AS 'year_month', REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(ESTADO,'RONDÔNIA','RO'),'ACRE','AC'),'AMAZONAS','AM'),'RORAIMA','RR'),'PARÁ','PA'),'AMAPÁ','AP'),'TOCANTINS','TO'),'MARANHÃO','MA'),'PIAUÍ','PI'),'CEARÁ','CE'),'RIO GRANDE DO NORTE','RN'),'PARAÍBA','PB'),'PERNAMBUCO','PE'),'ALAGOAS','AL'),'SERGIPE','SE'),'BAHIA','BA'),'MINAS GERAIS','MG'),'ESPÍRITO SANTO','ES'),'RIO DE JANEIRO','RJ'),'SÃO PAULO','SP'),'PARANÁ','PR'),'SANTA CATARINA','SC'),'RIO GRANDE DO SUL','RS'),'MATO GROSSO DO SUL','MS') ,'MATO GROSSO','MT'),'GOIÁS','GO'),'DISTRITO FEDERAL','DF') AS uf, COMBUSTÍVEL AS product, 'm3' AS unit, IFNULL(Mar,0) volume from db.stg_vendas_combustiveis "\
        "union all "\
        "select STR_TO_DATE(CONCAT(ANO,'04','01'),'%Y%m%d') AS 'year_month', REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(ESTADO,'RONDÔNIA','RO'),'ACRE','AC'),'AMAZONAS','AM'),'RORAIMA','RR'),'PARÁ','PA'),'AMAPÁ','AP'),'TOCANTINS','TO'),'MARANHÃO','MA'),'PIAUÍ','PI'),'CEARÁ','CE'),'RIO GRANDE DO NORTE','RN'),'PARAÍBA','PB'),'PERNAMBUCO','PE'),'ALAGOAS','AL'),'SERGIPE','SE'),'BAHIA','BA'),'MINAS GERAIS','MG'),'ESPÍRITO SANTO','ES'),'RIO DE JANEIRO','RJ'),'SÃO PAULO','SP'),'PARANÁ','PR'),'SANTA CATARINA','SC'),'RIO GRANDE DO SUL','RS'),'MATO GROSSO DO SUL','MS') ,'MATO GROSSO','MT'),'GOIÁS','GO'),'DISTRITO FEDERAL','DF') AS uf, COMBUSTÍVEL AS product, 'm3' AS unit, IFNULL(Abr,0) volume from db.stg_vendas_combustiveis "\
        "union all "\
        "select STR_TO_DATE(CONCAT(ANO,'05','01'),'%Y%m%d') AS 'year_month', REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(ESTADO,'RONDÔNIA','RO'),'ACRE','AC'),'AMAZONAS','AM'),'RORAIMA','RR'),'PARÁ','PA'),'AMAPÁ','AP'),'TOCANTINS','TO'),'MARANHÃO','MA'),'PIAUÍ','PI'),'CEARÁ','CE'),'RIO GRANDE DO NORTE','RN'),'PARAÍBA','PB'),'PERNAMBUCO','PE'),'ALAGOAS','AL'),'SERGIPE','SE'),'BAHIA','BA'),'MINAS GERAIS','MG'),'ESPÍRITO SANTO','ES'),'RIO DE JANEIRO','RJ'),'SÃO PAULO','SP'),'PARANÁ','PR'),'SANTA CATARINA','SC'),'RIO GRANDE DO SUL','RS'),'MATO GROSSO DO SUL','MS') ,'MATO GROSSO','MT'),'GOIÁS','GO'),'DISTRITO FEDERAL','DF') AS uf, COMBUSTÍVEL AS product, 'm3' AS unit, IFNULL(Mai,0) volume from db.stg_vendas_combustiveis "\
        "union all "\
        "select STR_TO_DATE(CONCAT(ANO,'06','01'),'%Y%m%d') AS 'year_month', REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(ESTADO,'RONDÔNIA','RO'),'ACRE','AC'),'AMAZONAS','AM'),'RORAIMA','RR'),'PARÁ','PA'),'AMAPÁ','AP'),'TOCANTINS','TO'),'MARANHÃO','MA'),'PIAUÍ','PI'),'CEARÁ','CE'),'RIO GRANDE DO NORTE','RN'),'PARAÍBA','PB'),'PERNAMBUCO','PE'),'ALAGOAS','AL'),'SERGIPE','SE'),'BAHIA','BA'),'MINAS GERAIS','MG'),'ESPÍRITO SANTO','ES'),'RIO DE JANEIRO','RJ'),'SÃO PAULO','SP'),'PARANÁ','PR'),'SANTA CATARINA','SC'),'RIO GRANDE DO SUL','RS'),'MATO GROSSO DO SUL','MS') ,'MATO GROSSO','MT'),'GOIÁS','GO'),'DISTRITO FEDERAL','DF') AS uf, COMBUSTÍVEL AS product, 'm3' AS unit, IFNULL(Jun,0) volume from db.stg_vendas_combustiveis "\
        "union all "\
        "select STR_TO_DATE(CONCAT(ANO,'07','01'),'%Y%m%d') AS 'year_month', REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(ESTADO,'RONDÔNIA','RO'),'ACRE','AC'),'AMAZONAS','AM'),'RORAIMA','RR'),'PARÁ','PA'),'AMAPÁ','AP'),'TOCANTINS','TO'),'MARANHÃO','MA'),'PIAUÍ','PI'),'CEARÁ','CE'),'RIO GRANDE DO NORTE','RN'),'PARAÍBA','PB'),'PERNAMBUCO','PE'),'ALAGOAS','AL'),'SERGIPE','SE'),'BAHIA','BA'),'MINAS GERAIS','MG'),'ESPÍRITO SANTO','ES'),'RIO DE JANEIRO','RJ'),'SÃO PAULO','SP'),'PARANÁ','PR'),'SANTA CATARINA','SC'),'RIO GRANDE DO SUL','RS'),'MATO GROSSO DO SUL','MS') ,'MATO GROSSO','MT'),'GOIÁS','GO'),'DISTRITO FEDERAL','DF') AS uf, COMBUSTÍVEL AS product, 'm3' AS unit, IFNULL(Jul,0) volume from db.stg_vendas_combustiveis "\
        "union all "\
        "select STR_TO_DATE(CONCAT(ANO,'08','01'),'%Y%m%d') AS 'year_month', REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(ESTADO,'RONDÔNIA','RO'),'ACRE','AC'),'AMAZONAS','AM'),'RORAIMA','RR'),'PARÁ','PA'),'AMAPÁ','AP'),'TOCANTINS','TO'),'MARANHÃO','MA'),'PIAUÍ','PI'),'CEARÁ','CE'),'RIO GRANDE DO NORTE','RN'),'PARAÍBA','PB'),'PERNAMBUCO','PE'),'ALAGOAS','AL'),'SERGIPE','SE'),'BAHIA','BA'),'MINAS GERAIS','MG'),'ESPÍRITO SANTO','ES'),'RIO DE JANEIRO','RJ'),'SÃO PAULO','SP'),'PARANÁ','PR'),'SANTA CATARINA','SC'),'RIO GRANDE DO SUL','RS'),'MATO GROSSO DO SUL','MS') ,'MATO GROSSO','MT'),'GOIÁS','GO'),'DISTRITO FEDERAL','DF') AS uf, COMBUSTÍVEL AS product, 'm3' AS unit, IFNULL(Ago,0) volume from db.stg_vendas_combustiveis "\
        "union all "\
        "select STR_TO_DATE(CONCAT(ANO,'09','01'),'%Y%m%d') AS 'year_month', REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(ESTADO,'RONDÔNIA','RO'),'ACRE','AC'),'AMAZONAS','AM'),'RORAIMA','RR'),'PARÁ','PA'),'AMAPÁ','AP'),'TOCANTINS','TO'),'MARANHÃO','MA'),'PIAUÍ','PI'),'CEARÁ','CE'),'RIO GRANDE DO NORTE','RN'),'PARAÍBA','PB'),'PERNAMBUCO','PE'),'ALAGOAS','AL'),'SERGIPE','SE'),'BAHIA','BA'),'MINAS GERAIS','MG'),'ESPÍRITO SANTO','ES'),'RIO DE JANEIRO','RJ'),'SÃO PAULO','SP'),'PARANÁ','PR'),'SANTA CATARINA','SC'),'RIO GRANDE DO SUL','RS'),'MATO GROSSO DO SUL','MS') ,'MATO GROSSO','MT'),'GOIÁS','GO'),'DISTRITO FEDERAL','DF') AS uf, COMBUSTÍVEL AS product, 'm3' AS unit, IFNULL(September,0) volume from db.stg_vendas_combustiveis "\
        "union all "\
        "select STR_TO_DATE(CONCAT(ANO,'10','01'),'%Y%m%d') AS 'year_month', REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(ESTADO,'RONDÔNIA','RO'),'ACRE','AC'),'AMAZONAS','AM'),'RORAIMA','RR'),'PARÁ','PA'),'AMAPÁ','AP'),'TOCANTINS','TO'),'MARANHÃO','MA'),'PIAUÍ','PI'),'CEARÁ','CE'),'RIO GRANDE DO NORTE','RN'),'PARAÍBA','PB'),'PERNAMBUCO','PE'),'ALAGOAS','AL'),'SERGIPE','SE'),'BAHIA','BA'),'MINAS GERAIS','MG'),'ESPÍRITO SANTO','ES'),'RIO DE JANEIRO','RJ'),'SÃO PAULO','SP'),'PARANÁ','PR'),'SANTA CATARINA','SC'),'RIO GRANDE DO SUL','RS'),'MATO GROSSO DO SUL','MS') ,'MATO GROSSO','MT'),'GOIÁS','GO'),'DISTRITO FEDERAL','DF') AS uf, COMBUSTÍVEL AS product, 'm3' AS unit, IFNULL(October,0) volume from db.stg_vendas_combustiveis "\
        "union all "\
        "select STR_TO_DATE(CONCAT(ANO,'11','01'),'%Y%m%d') AS 'year_month', REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(ESTADO,'RONDÔNIA','RO'),'ACRE','AC'),'AMAZONAS','AM'),'RORAIMA','RR'),'PARÁ','PA'),'AMAPÁ','AP'),'TOCANTINS','TO'),'MARANHÃO','MA'),'PIAUÍ','PI'),'CEARÁ','CE'),'RIO GRANDE DO NORTE','RN'),'PARAÍBA','PB'),'PERNAMBUCO','PE'),'ALAGOAS','AL'),'SERGIPE','SE'),'BAHIA','BA'),'MINAS GERAIS','MG'),'ESPÍRITO SANTO','ES'),'RIO DE JANEIRO','RJ'),'SÃO PAULO','SP'),'PARANÁ','PR'),'SANTA CATARINA','SC'),'RIO GRANDE DO SUL','RS'),'MATO GROSSO DO SUL','MS') ,'MATO GROSSO','MT'),'GOIÁS','GO'),'DISTRITO FEDERAL','DF') AS uf, COMBUSTÍVEL AS product, 'm3' AS unit, IFNULL(Nov,0) volume from db.stg_vendas_combustiveis "\
        "union all "\
        "select STR_TO_DATE(CONCAT(ANO,'12','01'),'%Y%m%d') AS 'year_month', REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(ESTADO,'RONDÔNIA','RO'),'ACRE','AC'),'AMAZONAS','AM'),'RORAIMA','RR'),'PARÁ','PA'),'AMAPÁ','AP'),'TOCANTINS','TO'),'MARANHÃO','MA'),'PIAUÍ','PI'),'CEARÁ','CE'),'RIO GRANDE DO NORTE','RN'),'PARAÍBA','PB'),'PERNAMBUCO','PE'),'ALAGOAS','AL'),'SERGIPE','SE'),'BAHIA','BA'),'MINAS GERAIS','MG'),'ESPÍRITO SANTO','ES'),'RIO DE JANEIRO','RJ'),'SÃO PAULO','SP'),'PARANÁ','PR'),'SANTA CATARINA','SC'),'RIO GRANDE DO SUL','RS'),'MATO GROSSO DO SUL','MS') ,'MATO GROSSO','MT'),'GOIÁS','GO'),'DISTRITO FEDERAL','DF') AS uf, COMBUSTÍVEL AS product, 'm3' AS unit, IFNULL(Dez,0) volume from db.stg_vendas_combustiveis "\



    cursor.execute(sql)
    db.commit()
    db.close()



default_args = {'owner': 'admin', 'start_date': dt.datetime(2022, 8, 22), 'retries': 1, 'retry_delay': dt.timedelta(minutes = 10),}


with DAG('Raizen', default_args = default_args, schedule_interval = '0 * * * *',) as dag:
    
# Tarefa 1
    Step_01_Ingestion_Sheets_To_Stage = PythonOperator(task_id = 'Step_01_Ingestion_Sheets_To_Stage', python_callable = Step_01_Ingestion_Sheets_To_Stage)



# Tarefa 2
    Step_02_Ingestion_Stage_To_Analitics = PythonOperator(task_id = 'Step_02_Ingestion_Stage_To_Analitics', python_callable = Step_02_Ingestion_Stage_To_Analitics)



# Workflow
Step_01_Ingestion_Sheets_To_Stage >> Step_02_Ingestion_Stage_To_Analitics
