from mysql.connector import connection
from dotenv import load_dotenv


def connect():
    load_dotenv()
    return connection.MySQLConnection(
        user=os.getenv("MYSQL_USER"),
        password=os.getenv("MYSQL_PASS"),
        host=os.getenv("MYSQL_HOST"),
        database=os.getenv("MYSQL_DB"),
    )

