# -*- coding: utf-8 -*-
import aiomysql
import logging
import asyncio

__author__ = 'xueqian'


# 记录操作日志
def log(sql,args=()):
    logging.info('SQL:%s' % sql)


# 创建数据库连接池，避免频繁打开或关闭数据库连接
@asyncio.coroutine
def create_pool(loop,**kw):
    log('create database connection pool……')
    global __pool
    # 调用一个子协程来创建全局连接池，create_pool返回一个pool实例对象
    __pool= yield from aiomysql.create_pool(
        # 连接的基本属性设置
        host=kw.get('host', 'localhost'), # 数据库服务器位置，本地
        port=kw.get('port', 3306), # MySQL端口号
        user=kw['user'], # 登录用户名
        password=kw['password'], # 登录密码
        db=kw['db'], # 数据库名
        charset=kw.get('charset','utf8'), # 设置连接使用的编码格式utf-8
        autocommit=kw.get('autocommit',True),  # 是否自动提交，默认false

        # 以下是可选项设置
        maxsize=kw.get('maxsize',10), # 最大连接池大小，默认10
        minsize=kw.get('minsize',1), # 最小连接池大小，默认1
        loop=loop # 设置消息循环
    )

# select 返回结果集
@asyncio.coroutine
def select(sql, args, size=None):
    # sql:sql语句
    # args:填入sql的参数,list类型，如['20111101','xue']
    # size:取多少行记录
    log(sql, args)
    global __pool
    # 从连接池中获取一个连接
    with (yield from __pool) as conn: # with...as...的作用就是try...exception...
        # 打开一个DictCursor，已dict形式返回结果的游标
        cur = yield from conn.cursor(aiomysql.DictCursor)
        # sql的占位符为? 而MySQL的占位符为%s 替换
        yield from cur.execute(sql.replace('?', '%s'), args or ())
        # 如果size不为空，则取一定量的结果集
        if size:
            rs = yield from cur.fetchmany(size)
        else:
            rs = yield from cur.fetchall()
        yield from cur.close()
        logging.info('rows returned: %s' % len(rs))
        return rs



def execute(sql, args, autocommit=True):
    log(sql)
    with (yield from __pool) as conn:
    # with __pool.get() as conn:
        if not autocommit:
            yield from conn.begin()
        try:
            cur = yield from conn.cursor()
            yield from cur.execute(sql.replace('?','%s'), args)    # 问题出在这里！
            affected = cur.rowcount
            print('affected:',affected)
            yield from cur.close()
            if not autocommit:
                yield from conn.commit()
        except BaseException as e:
            if not autocommit:
                yield from conn.rollback()
            raise
        return affected


# 该方法用来将其占位符拼接起来成'?,?,?'的形式，num表示为参数的个数
def create_args_string(num):
    L = []
    for n in range(num):
        L.append('?')
    return ', '.join(L)


# 父域
class Field(object):
    # 字段名称，字段类型，是否为主键，default不太懂
    def __init__(self, name, column_type, primary_key, default):
        self.name = name
        self.column_type = column_type
        self.primary_key = primary_key
        self.default = default

    # 返回类名(域名)，字段类型，字段名
    def __str__(self):
        return '<%s, %s:%s>' % (self.__class__.__name__, self.column_type, self.name)


# 字符串域。映射varchar
class StringField(Field):
    # ddl用于定义数据类型
    def __init__(self, name=None, primary_key=False, default=None, ddl='varchar(100)'):
        super().__init__(name, ddl, primary_key, default) # 属性排列顺序按照Field类中__init__的顺序


# 整型域，映射Integer
class IntegerField(Field):

    def __init__(self, name=None, primary_key=False, default=0):
        super().__init__(name, 'bigint', primary_key, default)


# 布尔域，映射boolean
class BooleanField(Field):
    def __init__(self,name=None,default=False):
        super().__init__(name,'boolean',False,default)


# 浮点数域
class FloatField(Field):
    def __init__(self,name=None,primary_key=False,default=0.0):
        super().__init__(name,'real',primary_key,default)


# 文本域
class TextField(Field):
    def __init__(self,name=None,default=None):
        super().__init__(name,'text',False,default)


# 将具体的子类如User的映射信息读取出来
class ModelMetaclass(type):
    # cls: 当前准备创建的类对象,相当于self
    # name: 类名,比如User继承自Model,当使用该元类创建User类时,name=User
    # bases: 父类的元组
    # attrs: Model子类的属性和方法的字典,比如User有__table__,id,等,就作为attrs的keys
    def __new__(cls, name, bases, attrs):
        # 排除Model类本身:
        if name == 'Model':
            return type.__new__(cls, name, bases, attrs)
        # 获取table名称，若没有定义__table__属性,将类名作为表名.此处注意 or 的用法:
        tableName = attrs.get('__table__', None) or name
        logging.info('found model: %s (table: %s)' % (name, tableName))
        # 获取所有的Field和主键名:
        mappings = dict()    # 用字典来存储类属性与数据库表的列的映射关系
        fields = []          # 用于保存除主键以外的属性
        primaryKey = None    # 用于保存主键
        # k是属性名，v是定义域。如name=StringField(ddl="varchar50"),k=name,v=StringField(ddl="varchar50")
        for k, v in attrs.items():
            if isinstance(v, Field):
                logging.info('  found mapping: %s ==> %s' % (k, v))
                mappings[k] = v
                # 找到主键:
                if v.primary_key:
                    # 主键已存在，报错，不可能俩主键
                    if primaryKey:
                        raise RuntimeError('Duplicate primary key for field: %s' % k)
                    primaryKey = k
                else:
                    fields.append(k)
        # 没找到主键报错
        if not primaryKey:
            raise RuntimeError('Primary key not found.')
        # 从类属性中删除已经加入了映射字典的键，以免重名
        for k in mappings.keys():
            attrs.pop(k)
        # 将非主键的属性变形,放入escaped_fields中,方便增删改查语句的书写
        escaped_fields = list(map(lambda f: '`%s`' % f, fields))
        attrs['__mappings__'] = mappings # 保存属性和列的映射关系
        attrs['__table__'] = tableName
        attrs['__primary_key__'] = primaryKey # 主键属性名
        attrs['__fields__'] = fields # 除主键外的属性名
        # 构造默认的SELECT, INSERT, UPDATE和DELETE语句:
        attrs['__select__'] = 'select `%s`, %s from `%s`' % (primaryKey, ', '.join(escaped_fields), tableName)
        # attrs['__insert__'] = 'insert into `%s` (%s, `%s`) values (%s)' % (tableName, ', '.join(escaped_fields), primaryKey, create_args_string(len(escaped_fields) + 1))
        attrs['__insert__'] = 'insert into `%s` (%s, `%s`) values (%s)' % (tableName, ', '.join(escaped_fields), primaryKey, create_args_string(len(escaped_fields) + 1))
        attrs['__update__'] = 'update `%s` set %s where `%s`=?' % (tableName, ', '.join(map(lambda f: '`%s`=?' % (mappings.get(f).name or f), fields)), primaryKey)
        attrs['__delete__'] = 'delete from `%s` where `%s`=?' % (tableName, primaryKey)
        return type.__new__(cls, name, bases, attrs)


# 所有ORM映射的基类，继承自dict，通过ModelMetaclass元类来构造类
class Model(dict, metaclass=ModelMetaclass):

    # 初始化函数,调用其父类(dict)的方法
    def __init__(self, **kw):
        super(Model, self).__init__(**kw)

    # 增加__getattr__方法,使获取属性更方便,即可通过"a.b"的形式
    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError:
            raise AttributeError(r"'Model' object has no attribute '%s'" % key)

    # 增加__setattr__方法,使设置属性更方便,可通过"a.b=c"的形式
    def __setattr__(self, key, value):
        self[key] = value

    # 通过键取值,若值不存在,返回None
    def getValue(self, key):
        return getattr(self, key, None)

    # 通过键取值,若值不存在,则返回默认值
    # 这招很妙！
    def getValueOrDefault(self, key):
        value = getattr(self, key, None)
        if value is None:
            field = self.__mappings__[key]  # field是一个定义域!比如FloatField
            if field.default is not None:
                # id的StringField.default=next_id,因此调用该函数生成独立id。实现自增
                # FloatFiled.default=time.time数,因此调用time.time函数返回当前时间。当前时间做id
                # 普通属性的StringField默认为None,因此还是返回None
                value = field.default() if callable(field.default) else field.default
                logging.debug('using default value for %s: %s' % (key, str(value)))
                # 通过default取到值之后再将其作为当前值
                setattr(self, key, value)
        return value

    # classmethod，装饰器，定义该方法为类方法，必备参数为cls(类似于self);staticmethod,可以不传任何参数;其他的方法，必备参数self
    @classmethod
    @asyncio.coroutine
    def findAll(cls, where=None, args=None, **kw):
        # cls表示当前类或类的对象可调用该方法，where表示sql中的where，args记录下所有的需要用占位符'?'的参数
        # **kw是一个tuple，里面有多个dict键值对，如{'name',Mary} 多为筛选条件
        sql = [cls.__select__]
        # 我们定义的默认的select语句并不包括where子句
        # 因此若指定有where,需要在select语句中追加关键字
        if where:
            sql.append('where')
            sql.append(where)
        if args is None:
            args = []
        orderBy = kw.get('orderBy', None)
        if orderBy:
            sql.append('order by')
            sql.append(orderBy)
        limit = kw.get('limit', None)
        if limit is not None:
            sql.append('limit')
            if isinstance(limit, int):   # 如果是一个整型数，直接在sql语句的limit字段后添加占位符'?'
                sql.append('?')
                args.append(limit)
            elif isinstance(limit, tuple) and len(limit) == 2:  # limit 有两个参数
                sql.append('?, ?')
                args.extend(limit)
            else:
                raise ValueError('Invalid limit value: %s' % str(limit))  # %s输出只能是str类型
        # ''.join(list/tuple/dict)   "|".join(['a','b','c']) -> 'a|b|c'
        rs = yield from select(' '.join(sql), args)  # 转list为str.
        return [cls(**r) for r in rs]  # cls(**r)调用本类的__init__(方法)



    # 查找某列
    @classmethod
    @asyncio.coroutine
    def findNumber(cls,selectField,where=None,args=None):
        sql = ['select %s _num_ from `%s`' % (selectField,cls.__table__)]
        if where:
            sql.append('where')
            sql.append(where)
        rs = yield from select(''.join(sql),args,1)   # Q: 1是什么意思？size=1?为什么按列查找指取1个？
        if len(rs) == 0:
            return None
        return rs[0]['_num_']

    # 按主键查找
    @classmethod
    @asyncio.coroutine
    def find(cls, pk):
        rs = yield from select('%s where `%s`=?' % (cls.__select__, cls.__primary_key__), [pk], 1)
        if len(rs) == 0:
            return None
        # **kw表示关键字参数
        # 注意,我们在select函数中,打开的是DictCursor,它会以dict的形式返回结果
        return cls(**rs[0])

    # 插入
    @asyncio.coroutine
    def save(self):
        # 我们在定义__insert__时,将主键放在了末尾.因为属性与值要一一对应,因此通过append的方式将主键加在最后
        # 使用getValueOrDefault方法,可以调用time.time这样的函数来获取值
        print("进入save")
        args = list(map(self.getValueOrDefault, self.__fields__))
        args.append(self.getValueOrDefault(self.__primary_key__))
        rows = yield from execute(self.__insert__, args)
        print('返回行数：',rows)
        if rows != 1:
            logging.warn('failed to insert record: affected rows: %s' % rows)

    # 更新
    @asyncio.coroutine
    def update(self):
        # 像time.time,next_id之类的函数在插入的时候已经调用过了,没有其他需要实时更新的值,因此调用getValue
        args = list(map(self.getValue, self.__fields__))
        args.append(self.getValue(self.__primary_key__))
        rows = yield from execute(self.__update__, args)
        print('更新成功！')
        if rows != 1:
            logging.warn('failed to update by primary key: affected rows: %s' % rows)

    # 删除
    @asyncio.coroutine
    def remove(self):
        args = [self.getValue(self.__primary_key__)] # 取得主键作为参数
        rows = yield from execute(self.__delete__, args)
        print('删除成功！')
        if rows != 1:
            logging.warn('failed to remove by primary key: affected rows: %s' % rows)


