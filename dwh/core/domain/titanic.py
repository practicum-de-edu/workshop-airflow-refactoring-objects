import csv
import logging

import psycopg
import requests

from dwh.core.pg_connect import PgConnect


def download_titanic_dataset(
    url: str,
    db_connection: PgConnect,
):
    logging.info("Downloading titanic dataset")

    with requests.Session() as s:
        download = s.get(url)

    decoded_content = download.content.decode("utf-8")

    cr = csv.reader(decoded_content.splitlines(), delimiter=",")
    my_list = list(cr)

    conn_url = db_connection.url()

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
            conn.commit()

    logging.info("Downloaded titanic dataset")


def calculate_sex_dm(
    db_connection: PgConnect,
):
    logging.info("Downloading titanic dataset")

    with psycopg.connect(db_connection.url()) as conn:
        with conn.cursor() as cur:
            cur.execute("""DROP TABLE IF EXISTS public.titanic_sex_dm;""")
            cur.execute(
                """
                    CREATE TABLE public.titanic_sex_dm AS
                    SELECT
                        t."sex"                     AS "sex",
                        count(DISTINCT t."name")    AS name_uq,
                        avg("age")                  AS age_avg,
                        sum("fare")                 AS fare_sum
                    FROM public.titanic t
                    GROUP BY t."sex"
                """
            )

        conn.commit()

    logging.info("Downloaded titanic dataset")
