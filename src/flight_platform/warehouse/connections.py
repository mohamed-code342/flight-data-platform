"""Connection to SQL Server (pure Python driver: pymssql, no ODBC install needed)."""

import pymssql

from flight_platform.config import settings


def get_connection(database: str | None = None, autocommit: bool = False):
    """Open a connection. `database=None` connects to the server's default database (master)."""
    return pymssql.connect(
        server=settings.sqlserver_host,
        port=str(settings.sqlserver_port),
        user=settings.sqlserver_user,
        password=settings.sqlserver_password,
        database=database,
        login_timeout=15,
        autocommit=autocommit,
    )
