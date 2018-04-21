import aiomysql
import logging
import asyncio


def log(sql, args=()):
    logging.info('SQL: {}'.format(sql))


async def create_pool(loop, **kw):
    logging.info('create database connection pool...')
    global __pool
    __pool = await aiomysql.create_pool(
        host=kw.get('host', 'localhost'),
        port=kw.get('port', 3306),
        user=kw['user'],
        password=kw['password'],
        db=kw['db'],
        charset=kw.get('charset', 'utf8'),
        autocommit=kw.get('autocommit', True),
        maxsize=kw.get('maxsize', 10),
        minsize=kw.get('minsize', 1),
        loop=loop
    )


async def select(sql, args, size=None):
    log(sql, args)
    global __pool
    with await __pool as conn:
        cur = await conn.cursor(aiomysql.DictCursor)
        await cur.execute(sql.replace('?', '%s'), args or ())
        if size:
            rs = await cur.fetchmany(size)
        else:
            rs = await cur.fetchall()
        await cur.close()
        logging.info('row returned: {}'.format(len(rs)))
        return rs


async def execute(sql, args):
    log(sql)
    with await __pool as conn:
        try:
            cur = await conn.cursor()
            await cur.execute(sql.replace('?', '%s'), args)
            affected = cur.rowcount
            await cur.close()
        except BaseException as e:
            raise
        return affected


class ModelMetaClass(type):
    def __new__(cls, name, bases, attrs):
        # exclude 'Model' itself
        if name == 'Model':
            return type.__new__(cls, name, bases, attrs)

        # get table name
        tableName = attrs.get('__table__', None) or name
        logging.info('found model: {} (table: {})'.format(name, tableName))

        # get all fields and the primary key
        mappings = dict()
        fields = []
        primaryKey = None
        for k, v in attrs.items():
            if isinstance(v, Field):
                logging.info('  found mapping: {} ==> {}'.format(k, v))
                mappings[k] = v
                if v.primary_key:
                    if primaryKey:
                        raise RuntimeError('Duplicate primary key for field: {}'.format(k))
                    primaryKey = k
                else:
                    fields.append(k)
        if not primaryKey:
            raise RuntimeError('Primary key not found.')
        for k in mappings.keys():
            attrs.pop(k)

        escaped_fields = list(map(lambda f: '`{}`'.format(f), fields))
        attrs['__mappings__'] = mappings
        attrs['__table__'] = tableName
        attrs['__primary_key__'] = primaryKey
        attrs['__fields__'] = fields
        attrs['__select__'] = 'select `{}`, {} from `{}`'.format(primaryKey, ', '.join(escaped_fields), tableName)
        attrs['__insert__'] = 'insert into `{}` ({}, `{}`) values ({})'.format(tableName, ', '.join(escaped_fields),
                                                                               primaryKey, create_args_string(len(escaped_fields) + 1))
        attrs['__update__'] = 'update `{}` set {} where `{}`=?'.format(tableName,
                                                                       ', '.join(map(lambda f: '`{}`=?'.format(mappings.get(f).name or f), fields)), primaryKey)
        attrs['__delete__'] = 'delete from `{}` where `{}`=?'.format(tableName, primaryKey)
        return type.__new__(cls, name, bases, attrs)


class Field(object):
    def __init__(self, name, column_type, primary_key, default):
        self.name = name
        self.column_type = column_type
        self.primary_key = primary_key
        self.default = default

    def __str__(self):
        return "<{}, {}:{}>".format(self.__class__.__name__, self.column_type, self.name)


class Model(dict, metaclass=ModelMetaClass):
    def __init__(self):
        super(Model, self).__init__(**kw)

    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError as e:
            raise AttributeError(r"'Model' object has no attribute '{}".format(key))

    def __setattr__(self, key, value):
        self[key] = value

    def getValue(self, key):
        return getattr(self, key, None)

    def getValueOrDefault(self, key):
        value = getattr(self, key, None)
        if value is None:
            field = self.__mappings__[key]
            if field.default is not None:
                value = field.default() if callable(field.default) else field.default
                logging.debug('using default value for {}: {}'.format(key, str(value)))
                setattr(self, key, value)

        return value

    @classmethod
    async def find(cls, pk):
        # find object by primary key.
        rs = await select('{} where `{}`=?'.format(cls.__select__, cls.__primary_key__), [pk], 1)
        if len(rs) == 0:
            return None
        return cls(**rs[0])

    async def save(self):
        args = list(map(self.getValueOrDefault(), self.__fields__))
        args.append(self.getValueOrDefault(self.__primary_key__))
        rows = await execute(self.__insert__, args)
        if rows != 1:
            logging.warn('failed to insert record: affected rows: {}'.format(rows))


class StringField(Field):
    def __init__(self, name=None, primary_key=False, default=None, ddl='varchar(100)'):
        super().__init__(name, ddl, primary_key, default)


class BooleanField(Field):
    def __init__(self, name=None, default=None):
        super().__init__(name, 'boolean', False, default)


class IntegerField(Field):
    def __init__(self, name=None, primary_key=False, default=0):
        super().__init__(name, 'bigint', primary_key, default)


class FloatField(Field):
    def __init__(self, name=None, primary_key=False, default=0.0):
        super().__init__(name, 'real', primary_key, default)


class TextField(Field):
    def __init__(self, name=None, primary_key=False, default=None):
        super().__init__(name, 'text', primary_key, default)


