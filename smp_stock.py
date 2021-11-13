from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from datetime import date, datetime, timedelta

import yfinance as yf
import pandas as pd


default_args = {
            "owner": "airflow",
            "start_date": datetime(2021, 11, 13),
            "depends_on_past": False,
            "email_on_failure": False,
            #"retries": 2, # retry twice
            #"retry_delay": timedelta(minutes=5) # five minutes interval
        }
		
dag = DAG(dag_id='marketvol1',
			#schedule_interval="6 0 * * 1-5", # running at 6pm for weekdays
			schedule_interval='*/5 * * * *', # run every 5 mins 
			
			default_args=default_args,
			description='source Apple and Tesla data'
			)
		
#new temp directory taskkkk
t0 = BashOperator(
    task_id='CreateTempDir_task',
    bash_command='''mkdir -p /tmp/data/''' + str(date.today()),
    dag=dag
    )

def download_stock_data(stock_name):
    start_date = date.today()- timedelta(days=2) #yesterday
    end_date = date.today()  #today
    #print(start_date)
    df = yf.download(stock_name, start=start_date, end=end_date, interval='1m')
    df.to_csv('/tmp/data/' + str(date.today()) +'/' + stock_name + "_data.csv", header=True)

def get_last_stock_spread():
    apple_data = pd.read_csv("/tmp/data/out/AAPL_data.csv").sort_values(by = "Datetime", ascending = False)
    tesla_data = pd.read_csv("/tmp/data/out/TSLA_data.csv").sort_values(by = "Datetime", ascending = False)
    spread = [apple_data['High'][0] - apple_data['Low'][0], tesla_data['High'][0] - tesla_data['Low'][0]]
    return spread

t1 = PythonOperator(
    task_id='DownloadStockDataAAPL_task',
    python_callable=download_stock_data,
	op_kwargs={'stock_name':'AAPL'},
    dag=dag)

t2 = PythonOperator(
    task_id='DownloadStockDataTSLA_task',
    python_callable=download_stock_data,
	op_kwargs={'stock_name':'TSLA'},
    dag=dag)


t3 = BashOperator(
    task_id='move_csv_AAPL_task',
    bash_command='cp /tmp/data/' + str(date.today()) + '/AAPL* /tmp/data/out/' ,
    dag=dag
    )

t4 = BashOperator(
    task_id='move_csv_TSLA_task',
    bash_command='cp /tmp/data/' + str(date.today()) + '/TSLA* /tmp/data/out/',
    dag=dag
    )


t5 = PythonOperator(
    task_id='Analze_task',
    python_callable=get_last_stock_spread,
	dag=dag)

t0>>t1
t0>>t2

t1>>t3
t2>>t4

t3>>t5
t4>>t5
