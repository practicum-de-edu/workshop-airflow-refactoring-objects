import csv
from abc import ABC, abstractmethod
from logging import getLogger

import requests

from dwh.core.domain.entities.gender import Gender
from dwh.core.domain.entities.passenger import Passenger

logger = getLogger(__name__)


class ITitanicApiConnector(ABC):
    @abstractmethod
    def download_titanic_dataset(self) -> list[Passenger]:
        raise NotImplementedError


class TitanicConnector(ITitanicApiConnector):
    def __init__(self, url: str):
        self.url = url

    def resolve_gender(self, sex: str) -> Gender:
        if sex == "male":
            return Gender.MALE

        if sex == "female":
            return Gender.FEMALE

        raise ValueError(f"sex {sex} is not recognized.")

    def download_titanic_dataset(self) -> list[Passenger]:
        logger.info("Downloading titanic dataset")

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
                gender=self.resolve_gender(row[3]),
                age=float(row[4]),
                siblings_spouses_aboard=int(row[5]),
                parents_children_aboard=int(row[6]),
                fare=float(row[7]),
            )
            for row in my_list[1:]
        ]

        logger.info(f"Downloaded titanic dataset, len: {len(passengers)}.")
        return passengers
