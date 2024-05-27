from dwh.core.domain.titanic import calculate_sex_dm, download_titanic_dataset


class TestTitanicDownload:

    def test_titanic_download(self):
        url = "https://web.stanford.edu/class/archive/cs/cs109/cs109.1166/stuff/titanic.csv"
        conn_url = """
            host=localhost
            port=6001
            dbname=de
            user=airflow
            password=airflow
            target_session_attrs=read-write
        """

        download_titanic_dataset(url, conn_url)

    def test_titanic_sex_dm(self):
        conn_url = """
            host=localhost
            port=6001
            dbname=de
            user=airflow
            password=airflow
            target_session_attrs=read-write
        """

        calculate_sex_dm(conn_url)
