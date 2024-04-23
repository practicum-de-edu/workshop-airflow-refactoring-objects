import csv
import datetime as dt
import logging

import psycopg
import requests
from airflow.hooks.base import BaseHook
from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

args = {
    "owner": "airflow",
    "start_date": dt.datetime(2023, 9, 1),
    "retries": 1,
    "retry_delay": dt.timedelta(minutes=1),
}


def connection_url(
    host: str,
    port: str,
    db_name: str,
    user: str,
    pw: str,
    sslmode: str = "disable",
) -> str:
    return """
            host={host}
            port={port}
            dbname={db_name}
            user={user}
            password={pw}
            target_session_attrs=read-write
            sslmode={sslmode}
        """.format(
        host=host,
        port=port,
        db_name=db_name,
        user=user,
        pw=pw,
        sslmode=sslmode,
    )


def download_titanic_dataset():
    logging.info("Downloading titanic dataset")

    url = "https://web.stanford.edu/class/archive/cs/cs109/cs109.1166/stuff/titanic.csv"
    connection = BaseHook.get_connection("POSTGRES_DB")

    with requests.Session() as s:
        download = s.get(url)

    decoded_content = download.content.decode("utf-8")

    cr = csv.reader(decoded_content.splitlines(), delimiter=",")
    my_list = list(cr)

    conn_url = connection_url(
        connection.host,
        str(connection.port),
        connection.schema,
        connection.login,
        connection.password,
    )

    print(conn_url)

    with psycopg.connect(conn_url) as conn:
        with conn.cursor() as cur:
            cur.execute("""DROP TABLE IF EXISTS public.titanic""")
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS public.titanic (
                    PassengerId SERIAL PRIMARY KEY,
                    Survived INT,
                    Pclass INT,
                    Name varchar,
                    Sex varchar,
                    Age FLOAT,
                    Siblings_Spouses_Abroad INT,
                    Parents_Children_Abroad INT,
                    Fare FLOAT
                )
                """
            )

        conn.commit()

    with psycopg.connect(conn_url) as conn:
        with conn.cursor() as cur:
            for row in my_list[1:]:
                cur.execute(
                    """
                    INSERT INTO public.titanic (
                        Survived,
                        Pclass,
                        Name,
                        Sex,
                        Age,
                        Siblings_Spouses_Abroad,
                        Parents_Children_Abroad,
                        Fare
                    )
                    VALUES (
                        %(survived)s, 
                        %(pclass)s, 
                        %(name)s, 
                        %(sex)s, 
                        %(age)s, 
                        %(siblings_spouses_abroad)s, 
                        %(parents_children_abroad)s, 
                        %(fare)s
                    )
                """,
                    {
                        "survived": row[0],
                        "pclass": row[1],
                        "name": row[2],
                        "sex": row[3],
                        "age": row[4],
                        "siblings_spouses_abroad": row[5],
                        "parents_children_abroad": row[6],
                        "fare": row[7],
                    },
                )

    for row in my_list:
        print(row)

    logging.info("Downloaded titanic dataset")


dag = DAG(
    dag_id="titanic_dag",
    schedule_interval="0/15 * * * *",
    start_date=dt.datetime(2024, 4, 24),
    catchup=False,
    tags=["demo", "stg", "cdm", "titanic"],
    is_paused_upon_creation=False,
    default_args=args,
)


start = BashOperator(
    task_id="start",
    bash_command='echo "Here we start! "',
    dag=dag,
)

create_titanic_dataset = PythonOperator(
    task_id="download_titanic_dataset",
    python_callable=download_titanic_dataset,
    dag=dag,
)

titanic_sex_dm = PostgresOperator(
    task_id="create_titanic_sex_dm",
    postgres_conn_id="PG_CONN",
    sql="""
            DROP TABLE IF EXISTS public.titanic_sex_dm;

            CREATE TABLE public.titanic_sex_dm AS
            SELECT
                t."sex"                     AS "sex",
                count(DISTINCT t."name")    AS name_uq,
                avg("age")                  AS age_avg,
                sum("fare")                 AS fare_sum
            FROM public.titanic t
            GROUP BY t."sex"
          """,
)


start >> create_titanic_dataset >> titanic_sex_dm
