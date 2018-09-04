import time
import collections
import threading
from .session import *


class Connection(object):
    def __init__(self, host, user, password, database, name, port=3306, connections=1, init_thread=True, autocommit=True):
        self.__connection_pool = collections.defaultdict(list)
        self.__default_pool = name
        self.add_multiple_connections(host, user, password, database, name, port, connections, autocommit)
        self.__thread = threading.Thread(target=self.thread_reconnect)
        if init_thread:
            self.__thread.start()

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
        while True:
            for key in self.__connection_pool.keys():
                for conn in self.__connection_pool[key]:
                    if conn.connected is False:
                        conn.reconnect()
            time.sleep(30)

    def set_default_name_pool(self, name):
        self.__default_pool = name

    def add_new_connection(self, host, user, password, database, name, port, autocommit):
        self.__connection_pool[name].append(Session(host, user, password, database, port, autocommit=autocommit))

    def add_multiple_connections(self, host, user, password, database, name, port, connections, autocommit):
        for i in range(0, connections):
            self.add_new_connection(host, user, password, database, name, port, autocommit)

    def close_pool(self, name=None):
        if name is None:
            name = self.__default_pool

        for con in self.__connection_pool[name]:
            con.close()
