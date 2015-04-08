#!/Users/caoshuai/develop/PycharmProjects/awesome-python-webapp/www/transwarp/db.py
# -*- coding: utf-8 -*-

__author__ = 'Caos'

'''
Database operation module.
'''

import time, uuid, logging, threading, functools

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


def create_engine(user, password, database, host='127.0.0.1', port=3306, **kw):
    import mysql.connector
    global engine
    if engine is not None:
        raise DBError('Engine is already initialized.')
    params = dict(user=user, password=password, database=database, host=host, port=port)
    defaults = dict(use_unicode=True, charset='utf8', collation='utf8_general_ci', autocommit=False)
    for k, v in defaults.iteritems():
        params[k] = kw.pop(k, v)
    params.update(kw)
    params['buffered'] = True
    engine = _Engine(lambda: mysql.connector.connect(**params))
    #test connction...
    logging.info('Init mysql engine <%s> ok.' % hex(id(engine)))

class _ConnectionCtx(object):
    '''
    提供了上下文的对象，实现了__enter__()与__exit__()方法，在with中使用，完成初始化和链接的关闭

    代码如下：
     with connection():
        pass
        with connection():
            pass
    '''
    def __enter__(self):
        global _db_ctx
        self.should_cleanup = False
        if not _db_ctx.is_init():
            _db_ctx.init()
            self.should_cleanup = True
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        global _db_ctx
        if self.should_cleanup:
            _db_ctx.cleanup()


def connection():
    '''
    返回可提供上下文的 _ConnectionCtx 对象
    :return:_ConnectionCtx
    '''

    return _ConnectionCtx()

def with_connection(func):
    '''
    重复使用 connection链接的装饰器
    :param func: 需要被装饰的函数
    :return:  装饰器
    '''
    @functools.wraps(func)
    def _wrapper(*args, **kw):
        #: 创建上下文对象
        with _ConnectionCtx():
            return func(*args, **kw)

    return _wrapper


class _TransactionCtx(object):
    '''
    _TransactionCtx 同样提供执行上下文，用来管理事务。
    代码如下:
    with _TransactionCtx():
        pass
    '''

    def __enter__(self):
        global _db_ctx
        self.should_close_conn = False
        if not _db_ctx.is_init():
            #需要先打开链接
            _db_ctx.init()
            self.should_close_conn = True
        _db_ctx.transactions = _db_ctx.transactions + 1
        logging.info('begin transaction...' if _db_ctx.transactions==1 else 'join current transaction..')
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        global _db_ctx
        _db_ctx.transactions = _db_ctx.transactions - 1
        try:
            if _db_ctx.transactions==0:
                if exc_type is None:
                    self.commit()
                else:
                    self.rollback()

        finally:
            if self.should_close_conn:
                _db_ctx.cleanup()


    def commit(self):
        global  _db_ctx
        logging.info('commit transaction...')
        try:
            _db_ctx.coonection.commit()
            logging.info('commit ok.')
        except:
            logging.warning('comit failed. try rollback..')
            _db_ctx.coonection.rollback()
            logging.warning('rollback ok.')
            raise

    def rollback(self):
        global _db_ctx
        logging.warning('rollback transaction...')
        _db_ctx.coonection.rollback()
        logging.info('rollback ok.')


def transaction():
    '''
    用来返回事务上下文对象
    :return:
    '''

    return _TransactionCtx()


def with_transaction(func):
    '''
    为事务添加增强方法的装饰器
    :param func:
    :return:
    '''
    @functools.wraps(func)
    def _wrapper(*args, **kw):
        _start = time.time()
        with _TransactionCtx():
            return func(*args, **kw)
        _profiling(_start)
    return _wrapper

def _select(sql, first, *args):
    '''
    执行选择查询，返回一条记录或一个记录集合。
    :param sql: 查询语句，使用？作为占位符，防止SQL注入攻击
    :param first:
    :param args:
    :return:
    '''
    global _db_ctx
    cursor = None
    sql = sql.replace('?', '%s')
    logging.info('SQL: %s, ARGS: %s' %(sql, args))

    try:
        cursor = _db_ctx.coonection.cursor()
        cursor.execute(sql, args)
        if cursor.description:
            names = [x[0] for x in cursor.description]
        if first:
            #: 查询记录
            values = cursor.fetchone()
            if not values:
                return None
            return Dict(names, values)
        return [Dict(names, x) for x in cursor.fetchall()]
    finally:
        if cursor:
            cursor.close()


@with_connection
def select_one(sql, *args):
    '''
    执行查询返回一条记录，如果没有对应的记录，返回None，如果查询到多条，则返回第一条。
    :param sql:
    :param args:
    :return:
    '''

    return _select(sql, True, *args)

@with_connection
def select_int(sql, *args):
    '''
    查询一条int类型的数据，只要一条int数据
    :param sql:
    :param args:
    :return:
    '''
    d = _select(sql, True, *args)
    if len(d)!=1:
        raise MultiColumnsError('Expect only one column.')
    return d.values()[0]


@with_connection
def select(sql, *args):
    '''
    查询集合，如果没有数据则放回空集合。
    :param sql:
    :param args:
    :return:
    '''

    return _select(sql, False, *args)


@with_connection
def _update(sql, *args):
    global _db_ctx
    cursor = None
    sql = sql.replace('?', '%s')
    logging.info('SQL: %s, ARGS: %s' %(sql, args))
    try:
        cursor = _db_ctx.connection.cursor()
        cursor.execute(sql, *args)
        r = cursor.rowcount
        if _db_ctx.transactions==0:
            #没有事务的上下文环境
            logging.info('auto commit')
            _db_ctx.coonection.commit()
        return r
    finally:
        if cursor:
            cursor.close()


def insert(table, **kw):
    '''
    执行插入操作
    :param table:
    :param kw:
    :return:
    '''
    cols, args = zip(*kw.iteritems())
    sql = 'insert into `%s` (%s) values (%s)' % (table, ','.join(['`%s`' % col for col in cols]), ','.join(['?' for i in range(len(cols))]))
    return _update(sql, *args)


def update(sql, *args):
    '''
    执行更新操作
    :param sql:
    :param args:
    :return:
    '''
    return _update(sql, *args)

if __name__=='__main__':
    logging.basicConfig(level=logging.DEBUG)
    create_engine('www-data', 'www-data', 'test')
    update('drop table if exists user')
    update('create table user(id int primary key, name test, email text, passwd text, last_modified real)')
    import doctest
    doctest.testmod()