from dwh.core.domain.titanic import TitanicPassengersDownloadJob, calculate_sex_dm
from dwh.core.pg_connect import PgConnect
from dwh.tests.fakes.titanic_passenger_repository_fake import TitanicPassengerRepositoryFake


class TestTitanicDownload:

    def test_titanic_download(self):
        url = "https://web.stanford.edu/class/archive/cs/cs109/cs109.1166/stuff/titanic.csv"

        repo = TitanicPassengerRepositoryFake()
        job = TitanicPassengersDownloadJob(url, repo)

        job.download_titanic_dataset()
        assert len(repo.list()) == 887

    def test_titanic_sex_dm(self):
        conn = PgConnect(host="localhost", port="6001", db_name="de", user="airflow", pw="airflow")

        calculate_sex_dm(conn)
