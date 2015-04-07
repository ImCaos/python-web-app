#!/Users/caoshuai/develop/PycharmProjects/awesome-python-webapp/www/transwarp/db.py
# -*- coding: utf-8 -*-

__author__ = 'Caos'

'''
Database operation module.
'''

import time, uuid, logging, threading

# Dict object
class Dict(dict):
    '''
    Simple dict but support access as x.y style.
    '''


    def __init__(self, names=(), values=(), **kw):
        super(Dict, self).__init__(**kw)

        for k, v in zip(names, values):
            self[k] = v


    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError:
            raise ArithmeticError(r"'Dict' object has no attribute '%s'" % key)


    def __setattr__(self, key, value):
        self[key] = value



def next_id(t=None):
    '''

    :param t: unix timestamp, default to None and using time.time().
    :return: next id as 50-char string.
    '''
    if t is None:
        t = time.time()

    return '%015d%s000' %(int(t * 1000), uuid.uuid4().hex)


def _profiling(start, sql=''):
    t = time - start
    if t > 0.1:
        logging.warning('[PROFILING] [DB] %s: %s' % (t, sql))
    else:
        logging.info('[PROFILING] [DB] %s: %s' % (t, sql))

class DBError(Exception):
    pass

class MultiColumnsError(DBError):
    pass

class _LasyConnection(object):

    def __init__(self):
        self.connection = None

    def cursor(self):
        if self.connection is None:
            connection = engine.connect()
            logging.info('open connection <%s>...' % hex(id(connection)))
            self.connection = connection
        return self.connection.cursor()

    def commit(self):
        self.connection.commit()

    def rollback(self):
        self.connection.rollback()

    def cleanup(self):
        if self.connection:
            connection = self.connection
            self.connection = None
            logging.info('close connection <%s>...' % hex(id(connection)))
            connection.close()




class _DbCtx(threading.local):
    '''
    Thread local object taht holds connection info.
    '''
    def __init__(self):
        self.connection = None
        self.transactions = 0

    def is_init(self):
        return not self.connection is None

    def init(self):
        logging.info('open lazy connection...')
        self.connection = _LasyConnection()
        self.transactions = 0

    def cleanup(self):
        self.connection.cleanup()
        self.connection = None

    def cursor(self):
        '''

        :return: cursor
        '''
        return self.connection.cursor()


# thread-local db context:
_db_ctx = _DbCtx()


# golbal engine object:
engine = None


class _Engine(object):
    def __init__(self, connect):
        self.connect = connect

    def connect(self):
        return self.connect
