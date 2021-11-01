import os
from collections import OrderedDict

from mysql.connector import connection
from dotenv import load_dotenv
import attr


@attr.s
class MySQL:
    def _connect(self):
        load_dotenv()
        return connection.MySQLConnection(
            user=os.getenv("MYSQL_USER"),
            password=os.getenv("MYSQL_PASS"),
            host=os.getenv("MYSQL_HOST"),
            database=os.getenv("MYSQL_DB"),
        )

    def _disconnect(self):
        self.cnx.close()

    def select(self, query, extra=None):
        with self.cnx.cursor() as cursor:
            if extra is None:
                cursor.execute(query)
            else:
                cursor.execute(query, extra)
            fields = [f[0] for f in cursor.description]
            for row in cursor:
                yield dict(zip(fields, row))

    def insert(self, table, data, ignore=False):
        if isinstance(data, list):
            values = data[0]
            insert_function = self.executemany
        else:
            values = data
            insert_function = self.execute

        statement = ["INSERT"]
        if ignore:
            statement.append("IGNORE")
        statement += ["INTO", self._table_name(table)]
        statement += [
            "(" + ",".join([self._field_name(i) for i in values.keys()]) + ")"
        ]
        statement += ["VALUES"]
        statement += ["(" + ",".join(["%({})s".format(i) for i in values.keys()]) + ")"]

        insert_function(" ".join(statement), data)

    def execute(self, statement, data):
        with self.cnx.cursor() as cursor:
            cursor.execute(statement, data)
            self.cnx.commit()

    def executemany(self, statement, data):
        with self.cnx.cursor() as cursor:
            cursor.executemany(statement, data)
            self.cnx.commit()

    def _table_name(self, table):
        return ".".join([self._field_name(i) for i in table.split(".")])

    def _field_name(self, field):
        return "`{}`".format(field)

    def __enter__(self):
        self.cnx = self._connect()
        return self

    def __exit__(self, *args, **kwargs):
        self._disconnect()
