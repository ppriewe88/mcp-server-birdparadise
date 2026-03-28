import os
from typing import Any

import pymssql  # type: ignore[import]


def establish_database_connection() -> Any:
    """Establish a connection to the SQL database."""
    server = "bird-paradise-db-server.database.windows.net"
    database = "bird_paradise_sqldatabase"
    username = os.getenv("SQL_DB_ADMIN")
    password = os.getenv("SQL_DB_ADMIN_PWD")

    try:
        connection = pymssql.connect(
            server=server,
            user=username,
            password=password,
            database=database,
            tds_version="7.3",
            login_timeout=10,
        )
        print("Connection successfully established")
        return connection
    except pymssql.Error as e:
        print("Connection error:", e)
        raise

