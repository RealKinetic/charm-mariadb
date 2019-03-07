"""
Helpers for the mariadb-k8s charm.
"""

from contextlib import contextmanager
import mysql.connector


def get_connection(host, password, user):
    """
    A helper function that returns a connection object.
    """
    return mysql.connector.connect(
        user=user,
        password=password,
        host=host,
    )


@contextmanager
def cursor(connection):
    cursor = connection.cursor()

    try:
        yield cursor
    except Exception:
        connection.rollback()

        raise
    else:
        connection.commit()
    finally:
        cursor.close()


def create_database(cursor, db_name):
    cursor.execute(
        "CREATE DATABASE IF NOT EXISTS %s",
        (db_name,),
    )


def grant_exists(cursor, db_name, username, address):
    try:
        cursor.execute("SHOW GRANTS for %s@%s", (username, address))
        grants = [i[0] for i in cursor.fetchall()]
    except mysql.connector.Error:
        return False
    else:
        # TODO: ???
        return "GRANT ALL PRIVILEGES ON `{}`".format(db_name) in grants


def create_grant(cursor, db_name, username, password, address):
    cursor.execute(
        "GRANT ALL PRIVILEGES ON %s.* TO %s@%s IDENTIFIED BY %s",
        (db_name, username, address, password),
    )


def cleanup_grant(cursor, db_name, username, address):
    cursor.execute(
        "REVOKE ALL ON %s FROM %s@%s",
        (db_name, username, address),
    )


def ensure_grant(connection, db_name, username, password, address=None):
    exists = grant_exists(
        cursor,
        db_name,
        username,
        address,
    )

    if exists:
        return

    create_grant(
        cursor,
        db_name,
        username,
        password,
        address,
    )
