import pymysql
import sched, time
import collections
import threading


class MysqlConnectException(Exception):
    pass


class MysqlQueryException(Exception):
    pass


class PySession(object):
    def __init__(self, host, user, password, database, port, autocommit=True):
        self.connected = False
        self.__host = host
        self.__user = user
        self.__password = password
        self.__database = database
        self.__port = port
        self.__connection = self.session()
        self.__auto_commit = autocommit

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
        except Exception as e:
            print(str(e))
            self.connected = False
            return None

    def reconnect(self):
        self.__connection = self.session()

    def close(self):
        if self.connected:
            self.__connection.close()
            self.connected = False

    def get_connection(self):
        return self.__connection


class PyConnection(object):
    def __init__(self, host, user, password, database, name, port=3306, connections=1, init_thread=True, autocommit=True):
        self.__connection_pool = collections.defaultdict(list)
        self.__default_pool = name
        self.add_multiple_connections(host, user, password, database, name, port, connections, autocommit)
        if init_thread:
            threading.Thread(target=self.run_scheduler)
            self.__scheduler = sched.scheduler(time.time, time.sleep)
            self.__scheduler.enter(60, 1, self.thread_reconnect)

    def run_scheduler(self):
        while True:
            self.__scheduler.run()

    def execute(self, query, name_pool=None, fetch_all=True):
        if name_pool is None:
            name_pool = self.__default_pool

        response = ''
        try:
            for conn in self.__connection_pool[name_pool]:
                if conn.connected:
                    res = conn.query(query, fetch_all)
                    if res is None and conn.connected is False:
                        continue
                    response = res
                    break
            return response
        except Exception as e:
            print(str(e))
            return None

    def get_conn(self, name_pool=None):
        if name_pool is None:
            name_pool = self.__default_pool
        return self.__connection_pool[name_pool][0].get_connection()

    def thread_reconnect(self):

        for key in self.__connection_pool.keys():
            for conn in self.__connection_pool[key]:
                if conn.connected is False:
                    conn.reconnect()

        self.__scheduler.enter(60, 1, self.thread_reconnect)

    def set_default_name_pool(self, name):
        self.__default_pool = name

    def add_new_connection(self, host, user, password, database, name, port, autocommit):
        self.__connection_pool[name].append(PySession(host, user, password, database, port, autocommit=autocommit))

    def add_multiple_connections(self, host, user, password, database, name, port, connections, autocommit):
        for i in range(0, connections):
            self.add_new_connection(host, user, password, database, name, port, autocommit)
