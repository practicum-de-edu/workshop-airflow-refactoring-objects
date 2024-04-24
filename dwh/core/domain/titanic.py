import csv
import logging

import psycopg
import requests

from dwh.core.domain.entities.gender import Gender
from dwh.core.domain.entities.passenger import Passenger
from dwh.core.pg_connect import PgConnect
from dwh.core.repository.titanic_passenger_psycopg_repository import ITitanicPassengerRepository


def resolve_gender(sex: str) -> Gender:
    if sex == "male":
        return Gender.MALE

    if sex == "female":
        return Gender.FEMALE

    raise ValueError(f"sex {sex} is not recognized.")


class TitanicPassengersDownloadJob:
    def __init__(self, url: str, passenger_repository: ITitanicPassengerRepository):
        self.url = url
        self.passenger_repository = passenger_repository

    def download_titanic_dataset(self):
        logging.info("Downloading titanic dataset")

        with requests.Session() as s:
            download = s.get(self.url)

        decoded_content = download.content.decode("utf-8")

        cr = csv.reader(decoded_content.splitlines(), delimiter=",")
        my_list = list(cr)

        passengers = [
            Passenger(
                survived=bool(row[0]),
                p_class=int(row[1]),
                name=row[2],
                gender=resolve_gender(row[3]),
                age=float(row[4]),
                siblings_spouses_aboard=int(row[5]),
                parents_children_aboard=int(row[6]),
                fare=float(row[7]),
            )
            for row in my_list[1:]
        ]

        self.passenger_repository.save_many(passengers)

        logging.info("Downloaded titanic dataset")


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
