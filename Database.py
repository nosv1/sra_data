import mysql.connector
from mysql.connector.abstracts import MySQLConnectionAbstract, MySQLCursorAbstract


class Database:
    @staticmethod
    def connect_database(
        db_name: str,
    ) -> tuple[MySQLConnectionAbstract, MySQLCursorAbstract]:
        connection: MySQLConnectionAbstract = mysql.connector.connect(
            host="10.0.0.227", user="SRA", password="SRA", database=db_name
        )
        cursor: MySQLCursorAbstract = connection.cursor()
        return connection, cursor

    @staticmethod
    def handle_bad_string(string: str) -> str:
        return string.replace("'", "''").replace("\\", "\\\\")


if __name__ == "__main__":
    connection, cursor = Database.connect_database("SRA")
    cursor.execute("SELECT * FROM races LIMIT 10")
    for row in cursor.fetchall():
        print(row)
    cursor.close()
    connection.close()
