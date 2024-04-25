import logging

import psycopg

from dwh.core.connectors.titanic_api_connector import ITitanicApiConnector
from dwh.core.pg_connect import PgConnect
from dwh.core.repository.titanic_passenger_psycopg_repository import ITitanicPassengerRepository


class TitanicPassengersDownloadJob:
    def __init__(self, api_connector: ITitanicApiConnector, passenger_repository: ITitanicPassengerRepository):
        self.api_connector = api_connector
        self.passenger_repository = passenger_repository

    def download_titanic_dataset(self):
        logging.info("Download Titanic Job started")

        passengers = self.api_connector.download_titanic_dataset()
        self.passenger_repository.save_many(passengers)

        logging.info("Download Titanic Job finished")


def create_titanic_table(
    db_connection: PgConnect,
):
    logging.info("Downloading titanic dataset")

    with psycopg.connect(db_connection.url()) as conn:
        with conn.cursor() as cur:
            cur.execute("""DROP TABLE IF EXISTS public.titanic;""")
            cur.execute(
                """
                    CREATE TABLE IF NOT EXISTS public.titanic (
                        id SERIAL PRIMARY KEY,
                        survived BOOLEAN,
                        p_class INT,
                        name varchar NOT NULL UNIQUE,
                        sex varchar,
                        age FLOAT,
                        siblings_spouses_aboard INT,
                        parents_children_aboard INT,
                        Fare FLOAT
                    );
                """
            )

        conn.commit()

    logging.info("Titanic Table Created")


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
