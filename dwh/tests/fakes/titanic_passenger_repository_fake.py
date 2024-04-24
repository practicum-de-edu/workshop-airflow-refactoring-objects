from dwh.core.domain.entities.passenger import Passenger
from dwh.core.repository.titanic_passenger_psycopg_repository import ITitanicPassengerRepository


class TitanicPassengerRepositoryFake(ITitanicPassengerRepository):
    def __init__(self):
        self.passengers = []

    def save(self, passenger: Passenger) -> None:
        self.passengers.append(passenger)

    def save_many(self, passengers: list[Passenger]) -> None:
        self.passengers.extend(passengers)

    def list(self) -> list[Passenger]:
        return self.passengers
