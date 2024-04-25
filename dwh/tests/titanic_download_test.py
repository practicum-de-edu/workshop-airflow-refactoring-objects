from unittest.mock import Mock

import pytest

from dwh.core.connectors.titanic_api_connector import ITitanicApiConnector
from dwh.core.domain.entities.passenger import Passenger
from dwh.core.domain.titanic import TitanicPassengersDownloadJob, calculate_sex_dm
from dwh.core.pg_connect import PgConnect
from dwh.tests.fakes.titanic_passenger_repository_fake import TitanicPassengerRepositoryFake


@pytest.fixture(scope="function")
def mocked_titanic_connector():
    mock = Mock(spec=ITitanicApiConnector)
    mock.download_titanic_dataset.return_value = [
        Passenger(
            **{
                "Age": 30,
                "Fare": 12.0,
                "Name": "John",
                "Pclass": 1,
                "Parents/Children Aboard": 1,
                "Siblings/Spouses Aboard": 0,
                "Sex": "male",
                "Survived": True,
            }
        )
    ]
    return mock


class TestTitanicDownload:

    def test_titanic_download(self, mocked_titanic_connector):

        repo = TitanicPassengerRepositoryFake()
        job = TitanicPassengersDownloadJob(mocked_titanic_connector, repo)

        job.download_titanic_dataset()

        assert len(repo.list()) == 1
        test_passenger = repo.list()[0]
        assert test_passenger.survived
        assert test_passenger.p_class == 1
        assert test_passenger.name == "John"
        assert test_passenger.age == 30
        assert test_passenger.siblings_spouses_aboard == 0
        assert test_passenger.parents_children_aboard == 1
        assert test_passenger.fare == 12.0

    def test_titanic_sex_dm(self):
        conn = PgConnect(host="localhost", port="6001", db_name="de", user="airflow", pw="airflow")

        calculate_sex_dm(conn)
