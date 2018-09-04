import pymysql
from .exception import *


class Session(object):
    def __init__(self, host, user, password, database, port, autocommit=True):
        self.connected = False
        self.__host = host
        self.__user = user
        self.__password = password
        self.__database = database
        self.__port = port
        self.__auto_commit = autocommit
        self.__connection = self.session()

    def session(self):
        try:
            db = pymysql.connect(host=self.__host, user=self.__user, password=self.__password, db=self.__database,
                                 port=self.__port, cursorclass=pymysql.cursors.DictCursor, connect_timeout=30)
            db.autocommit(self.__auto_commit)
            self.connected = True
            return db
        except MysqlConnectException as e:
            print(str(e))
            self.connected = False
            return None

    def query(self, query, fetch_all=True):
        if self.connected is False:
            return None
        try:
            with self.__connection.cursor() as cursor:
                rows = cursor.execute(query)
                return cursor.fetchall() if fetch_all else cursor.fetchone()
        except MysqlQueryException as e:
            print(str(e))
            try:
                self.__connection.ping(reconnect=False)
            except MysqlConnectException as er:
                self.connected = False
            return None

    def reconnect(self):
        self.__connection = self.session()

    def commit(self):
        self.__connection.commit()

    def rollback(self):
        self.__connection.rollback()

    def close(self):
        if self.connected:
            self.__connection.close()
            self.connected = False

    def get_connection(self):
        return self.__connection

    def execute(self, query):
        if self.connected is False:
            raise MysqlConnectException

        with self.__connection.cursor() as cursor:
            rows = cursor.execute(query)

        return rows
