# Indicium Lighthouse - Desafio Airflow

## Observações Iniciais
Utilizei como base o README da aula "Osquestrando Airflow 10/02".

Na parte da criação do DAG, por alguma razão desconhecida por mim, meu airflow não constava que existia um dag quando inicialmente colocava o código do "dag_desafio.py"(criado por mim). Então adicionei uma intermediária que era utilizar o código do "example_desafio.py" e depois trocar para "dag_desafio.py".


## Clonando o repositório
O primeiro passo é clonar o repositório para um novo repositório no seu computador:

```
git clone git@bitbucket.org:indiciumtech/airflow_tooltorial.git
```

Depois abrir a pasta do arquivo:

```
cd airflow_tooltorial
```

Utilize o README.md presente no "airflow_tooltorial" para instalar o airflow na sua máquina e fazer as configurações até o final do "Limpando os Dags de Exemplo".


## DAG

Inicialmente vamos instalar os pacotes:

```
pip install -r requirements.txt
```

Já que o Airflow procura por DAGs na em arquivos .py no diretório:

```
AIRFLOW_HOME/dags
```

E nosso caso AIRFLOW_HOME é airflow-data, então criaremos uma pasta "dags" e um arquivo "desafio.py" dentro de airflow-data.
No arquivo "desafio.py" colocaremosconteúdo do "example_desafio.py". 
Dê um refresh no airflow e veja se o dag apareceu. Depois que apareceu, apague o conteúdo que está no "desafio.py" e coloque o do "dag_desafio.py" dentro.
Com isso, temos o nosso DAG pronto para rodar.

## Por dentro do DAG do Airflow

Pegando o trecho da parte de DAG do nosso primeiro dag:

```py
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
(...)
with DAG(
    'DesafioAirflow',
    default_args=default_args,
    description='Desafio de Airflow da Indicium',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2021, 2, 14),
    catchup=False,
    tags=['example'],
) as dag:
    dag.doc_md = """
        Esse é o desafio de Airflow da Indicium.
    """
```

O que estamos fazendo aqui é declarando e configurando um DAG e as propriedades comuns a todas as tasks na propriedade default_args.

Todas as tasks do nosso dag vão ter:
  - o dono 'airflow'
  - nao vão depender de execuções passadas
  - vão mandar email em retry e falha para engineering@indicium.tech
  -   ...

## Definindo das Variáveis
No trecho abaixo definimos as variáveis que iremos utilizar, incluindo as contas que usamos para fazer o desafio 1,2 e 3.

```py
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
    with open('final_output.txt', 'w') as w:
        w.write(str(int(ds.loc['Rio de Janeiro']['Quantity'])))
    print("final_output.txt exists!")
```


## Conceito Operator do  Airflow

Da documentação:

*An operator represents a single, ideally idempotent, task. Operators determine what actually executes when your DAG runs.*

Ou seja, quem de fato executa alguma tarefa são os operators e não o código do DAG. O código que vimos na logo acima apenas declara um dag, nada de fato vai ser executado naquele código.

Essa distinção é importante porque o Airflow interpreta o código do dag com frequencia bem alta, algumas vezes por minuto(isso é configurável).

Se alguma operação grande for executada no código do dag em si, essa operação vai ser executada o tempo todo, e não apenas no agendamento declarado no contexto do DAG.

No nosso caso, o operator é:

```py
 extract_sqlite3_task = PythonOperator(
        task_id="extract_sqlite",
        python_callable=dextract_sqlite,
    )
```

O código que vai ser de fato excecutado nesse caso é o python_callable que definimos nas váriaviesa, nesse exemplo:

```
dextract_sqlite
```


## Declarando Dependencias e Tasks

Para resolver o problema, utilizamos 5 tasks na seguinte ordem:
- extract_sqlite3_task : extração do banco de dados
- load_sqlite3_data_to_db_task: carregamento do banco de dados
- task1: desafio 1
- task2: parte do desafio 2
- export_final_output: parte do desafio 2

Então foi criado as seguintes dependencias:

```
    extract_sqlite3_task >> load_sqlite3_data_to_db_task >> task1 >> task2 >> export_final_output
```

Mais detalhes das tasks:

    task1
    Desafio 1:
    Essa task escreve um arquivo chamado "output_orders.csv" com os dados da tabela 'Order' do banco de dados disponível em `data/Northwhind_small.sqlite`.


    task2 
    Desafio 2:
    Essa task escreve um arquivo chamado "output_ordersdetail.csv" com os dados da tabela 'OrderDetail' do banco de dados disponível em `data/Northwhind_small.sqlite`.
 

    export_final_output
    Desafio 2:
    Essa task faz um `JOIN` do arquivo "output_orders.csv" com "output_ordersdetail.csv". 
    Depois calcula qual a soma da quantidade vendida (*Quantity*) com destino (*ShipCity*) para o Rio de Janeiro. 
    A seguir exporta essa contagem em arquivo "final_output.txt".
    