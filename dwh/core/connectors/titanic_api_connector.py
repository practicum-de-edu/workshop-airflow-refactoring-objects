import csv
from abc import ABC, abstractmethod
from logging import getLogger
from typing import Iterable

import requests

from dwh.core.domain.entities.passenger import Passenger

logger = getLogger(__name__)


class ITitanicApiConnector(ABC):
    @abstractmethod
    def download_titanic_dataset(self) -> list[Passenger]:
        raise NotImplementedError


class TitanicConnector(ITitanicApiConnector):
    def __init__(self, url: str):
        self.url = url

    def download_titanic_dataset(self) -> list[Passenger]:
        logger.info("Downloading titanic dataset")

        with requests.Session() as s:
            download = s.get(self.url)

        decoded_content = download.content.decode("utf-8")

        passenger_reader: Iterable[dict] = csv.DictReader(decoded_content.splitlines(), delimiter=",")

        passengers = [Passenger(**p) for p in passenger_reader]

        logger.info(f"Downloaded titanic dataset, len: {len(passengers)}.")
        return passengers
