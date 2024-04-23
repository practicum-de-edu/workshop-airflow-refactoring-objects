from dwh.core.pg_connect import PgConnect
from dwh.core.titanic import calculate_sex_dm, download_titanic_dataset


class TestTitanicDownload:

    def test_titanic_download(self):
        url = "https://web.stanford.edu/class/archive/cs/cs109/cs109.1166/stuff/titanic.csv"
        db_connection = PgConnect(host="localhost", port="6001", db_name="de", user="airflow", pw="airflow")

        download_titanic_dataset(url, db_connection)

    def test_titanic_sex_dm(self):
        db_connection = PgConnect(host="localhost", port="6001", db_name="de", user="airflow", pw="airflow")

        calculate_sex_dm(db_connection)
