from airflow.utils.edgemodifier import Label
from datetime import datetime, timedelta
from textwrap import dedent
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow import DAG
from airflow.models import Variable
import sqlite3
import pandas as pd

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['camila.bosa@indicium.tech'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


## Do not change the code below this line ---------------------!!#
def export_final_answer():
    import base64

    # Import count
    with open('count.txt') as f:
        count = f.readlines()[0]

    my_email = Variable.get("my_email")
    message = my_email+count
    message_bytes = message.encode('ascii')
    base64_bytes = base64.b64encode(message_bytes)
    base64_message = base64_bytes.decode('ascii')

    with open("final_output.txt","w") as f:
        f.write(base64_message)
    return None
## Do not change the code above this line-----------------------##

#definicao das variveis
def my_email():
    return 'camila.bosa@indicium.tech'

def dextract_sqlite():
    return 'Extracted! at {{ds}}'

def dload_sqlite():
    return "Loaded sqlite data to db!"

def dorder():
   con = sqlite3.connect("/home/carmilla/airflow_tooltorial/data/Northwind_small.sqlite")
   Order = pd.read_sql_query("SELECT * from 'Order'", con)
   Order.to_csv("output_orders.csv")
   print("output_orders.csv exists!")
   con.close()

def dorder_detail():
    con = sqlite3.connect("/home/carmilla/airflow_tooltorial/data/Northwind_small.sqlite")
    OrderDetail = pd.read_sql_query("SELECT * from 'OrderDetail'", con)
    OrderDetail.to_csv("output_ordersdetail.csv")
    print("output_ordersdetail.csv exists!")
    con.close()

def dcount():
    Order = pd.read_csv('output_orders.csv')
    OrderDetail = pd.read_csv('output_ordersdetail.csv')
    output = pd.merge(Order, OrderDetail, how="inner", left_on="Id",right_on='OrderId')
    ds = output.groupby('ShipCity').sum()
    with open('count.txt', 'w') as w:
        w.write(str(int(ds.loc['Rio de Janeiro']['Quantity'])))
    print("count.txt exists!")



#criacao do DAG
with DAG(
    'DesafioAirflow',
    default_args=default_args,
    description='Desafio de Airflow da Indicium',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['example'],
) as dag:
    dag.doc_md = """
        Esse é o desafio de Airflow da Indicium.
    """

#operador   
    extract_sqlite3_task = PythonOperator(
        task_id="extract_sqlite",
        python_callable=dextract_sqlite,
    )

#documentacao
    load_sqlite3_data_to_db_task = PythonOperator(
        task_id='load_sqlite',
        python_callable=dload_sqlite,
    )

    load_sqlite3_data_to_db_task.doc_md = dedent(
        """\
    #### Task Documentation

    Essa task faz load dos dados extraidos do sqlite para hd, load para o banco de dados.

    """
    )


#criando dependencias
    extract_sqlite3_task >> load_sqlite3_data_to_db_task


#importação da tabela 
    task1= PythonOperator(
        task_id='task1',
        python_callable= dorder)

    task1.doc_md = dedent(
        """\
    #### Task Documentation
    Desafio 1:
    Essa task escreve um arquivo chamado "output_orders.csv" com os dados da tabela 'Order' do banco de dados disponível em `data/Northwhind_small.sqlite`.
    """
    )
    
#criando dependencias
    extract_sqlite3_task >> load_sqlite3_data_to_db_task >> task1

    task2 = PythonOperator(
        task_id='task2',
        python_callable= dorder_detail)

    task2.doc_md = dedent(
        """\
    #### Task Documentation
        Desafio 2:
    Essa task escreve um arquivo chamado "output_ordersdetail.csv" com os dados da tabela 'OrderDetail' do banco de dados disponível em `data/Northwhind_small.sqlite`.
    """
    )

#criando dependencias
    extract_sqlite3_task >> load_sqlite3_data_to_db_task >> task1 >> task2

    export_final_output= PythonOperator(
        task_id='export_final_output',
        python_callable= dcount)

    export_final_output.doc_md = dedent(
        """\
    #### Task Documentation
        Desafio 2:
    Essa task faz um `JOIN` do arquivo "output_orders.csv" com "output_ordersdetail.csv". 
    Depois calcula qual a soma da quantidade vendida (*Quantity*) com destino (*ShipCity*) para o Rio de Janeiro. 
    A seguir exporta essa contagem em arquivo "count.txt".
    """
    )
    #criando dependencias
    extract_sqlite3_task >> load_sqlite3_data_to_db_task >> task1 >> task2 >> export_final_output
