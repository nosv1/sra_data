from os import getenv

import mysql.connector
from mysql.connector.abstracts import MySQLConnectionAbstract, MySQLCursorAbstract
from neo4j import BoltDriver, GraphDatabase, Session
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine as SQLAEngine


class MySqlDatabase:
    @staticmethod
    def connect_database(
        db_name: str,
        verbose=True,
    ) -> tuple[MySQLConnectionAbstract, MySQLCursorAbstract]:
        if verbose:
            print(f"Connecting to database {db_name}...", end="")

        connection: MySQLConnectionAbstract = mysql.connector.connect(
            host=getenv("SQL_DB_HOST"),
            user="SRA",
            password="SRA",
            database=db_name,
        )
        cursor: MySQLCursorAbstract = connection.cursor()

        if verbose:
            print("connected")
        return connection, cursor

    @staticmethod
    def handle_bad_string(string: str) -> str:
        return string.replace("'", "''").replace("\\", "\\\\")

    @staticmethod
    def close_connection(
        connection: MySQLConnectionAbstract, cursor: MySQLCursorAbstract, verbose=True
    ):
        if verbose:
            print(f"Closing connection...", end="")

        cursor.close()
        connection.close()

        if verbose:
            print("closed")

    @staticmethod
    def create_engine() -> SQLAEngine:
        DATABASE_URI = f"mysql+mysqlconnector://SRA:SRA@{getenv('SQL_DB_HOST')}/SRA"
        return create_engine(DATABASE_URI)


class Neo4jDatabase:
    @staticmethod
    def connect_database(db_name: str, verbose=True) -> tuple[BoltDriver, Session]:
        if verbose:
            print(f"Connecting to Neo4j database {db_name}...", end="")

        driver = GraphDatabase.driver(
            uri=f"bolt://{getenv('NEO_DB_HOST')}:7687",
            auth=("SRA", "simracingalliance"),
        )
        session = driver.session(database=db_name)

        if verbose:
            print("connected")
        return driver, session

    @staticmethod
    def close_connection(driver: BoltDriver, session: Session, verbose=True):
        if verbose:
            print("Closing connection...", end="")

        session.close()
        driver.close()

        if verbose:
            print("closed")

    @staticmethod
    def handle_bad_string(string: str) -> str:
        return string.replace("'", "\\'")


if __name__ == "__main__":
    pass
