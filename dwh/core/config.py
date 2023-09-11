from airflow.hooks.base import BaseHook

from dwh.core.lib.postgres import PgConnect


class ConfigConst:
    TITANIC_RAW_REPOSITORY_CONN_ID = "TITANIC_RAW_REPOSITORY_CONN_ID"


class ConnectionBuilder:
    @staticmethod
    def pg_conn(conn_id: str) -> PgConnect:
        conn = BaseHook.get_connection(conn_id)

        sslmode = "require"
        if "sslmode" in conn.extra_dejson:
            sslmode = conn.extra_dejson["sslmode"]

        pg = PgConnect(str(conn.host), str(conn.port), str(conn.schema), str(conn.login), str(conn.password), sslmode)

        return pg


class AppConfig:
    @staticmethod
    def titanic_raw_repository() -> PgConnect:
        return ConnectionBuilder.pg_conn(ConfigConst.TITANIC_RAW_REPOSITORY_CONN_ID)
