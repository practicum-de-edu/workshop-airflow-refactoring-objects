from airflow.hooks.base import BaseHook

from dwh.core.pg_connect import PgConnect


class EnvConfig:
    POSTGRES_DB = "POSTGRES_DB"


class DependencyConfig:

    default_owner = "airflow"

    @staticmethod
    def db_connection() -> PgConnect:
        connection = BaseHook.get_connection(EnvConfig.POSTGRES_DB)
        return PgConnect(
            str(connection.host),
            str(connection.port),
            str(connection.schema),
            str(connection.login),
            str(connection.password),
        )

    @staticmethod
    def titanic_source_url() -> str:
        return "https://web.stanford.edu/class/archive/cs/cs109/cs109.1166/stuff/titanic.csv"