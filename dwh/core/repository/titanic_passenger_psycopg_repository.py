from abc import ABC, abstractmethod
from typing import List

from dwh.core.domain.entities.passenger import Passenger
from dwh.core.pg_connect import PgConnect


class ITitanicPassengerRepository(ABC):
    @abstractmethod
    def save(self, passenger: Passenger):
        raise NotImplementedError

    @abstractmethod
    def save_many(self, passengers: list[Passenger]):
        raise NotImplementedError


class TitanicPassengerPsycopgRepository(ITitanicPassengerRepository):
    def __init__(self, db_connection: PgConnect):
        self._db_connection = db_connection

    def save(self, passenger: Passenger):
        with self._db_connection.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO public.titanic (
                        age,
                        fare,
                        name,
                        p_class,
                        parents_children_aboard,
                        sex,
                        siblings_spouses_aboard,
                        survived
                    )
                    VALUES (
                        %(age)s,
                        %(fare)s,
                        %(name)s,
                        %(p_class)s,
                        %(parents_children_aboard)s,
                        %(gender)s,
                        %(siblings_spouses_aboard)s,
                        %(survived)s
                    )
                    ON CONFLICT (name) DO NOTHING;

                """,
                    passenger.model_dump(),
                )

    def save_many(self, passengers: List[Passenger]):
        with self._db_connection.connection() as conn:
            with conn.cursor() as cur:
                cur.executemany(
                    """
                    INSERT INTO public.titanic (
                        age,
                        fare,
                        name,
                        p_class,
                        parents_children_aboard,
                        sex,
                        siblings_spouses_aboard,
                        survived
                    )
                    VALUES (
                        %(age)s,
                        %(fare)s,
                        %(name)s,
                        %(p_class)s,
                        %(parents_children_aboard)s,
                        %(gender)s,
                        %(siblings_spouses_aboard)s,
                        %(survived)s
                    )
                    ON CONFLICT (name) DO NOTHING;

                """,
                    [p.model_dump() for p in passengers],
                )
