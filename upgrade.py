# *-* coding=utf8 *-*

from sqlalchemy.engine import create_engine
from sqlalchemy.orm.session import sessionmaker


class Metadata(object):

    def __init__(self, session):
        self._session = session

    def get(self, name):
        sql = "select value from metadata where name='{name}'".format(name=name)
        row = self._session.execute(sql).first()
        return row.value if row else None

    def set(self, name, value):
        sql = "update metadata set value='{value}' where name= '{name}'".format(value=value, name=name)
        result = self._session.execute(sql)
        self._session.commit()
        if result.rowcount == 0:
            sql = "insert into metadata (name, value) values ('%s', '%s') where name='version'" % (name, value)
            self._session.execute(sql)
            self._session.commit()

def open(current_version, username, password, host, port, db_name, upgrade):
    url = "mysql://{username}:{password}@{host}:{port}/?charset=utf8".format(username=username, password=password, host=host, port=port)
    engine = create_engine(url)
    _init_metadata(engine, db_name)
    url = "mysql://{username}:{password}@{host}:{port}/{db_name}?charset=utf8".format(username=username, password=password, host=host, port=port, db_name=db_name)
    engine = create_engine(url)
    session = sessionmaker(bind=engine)()
    session.execute("select * from metadata where name='version' for update")
    session.metadata = Metadata(session)
    version = session.metadata.get('version')
    if current_version > int(version):
        # session.execute("update metadata set version={current_version} where name='version'")
        upgrade(version, session)
        session.metadata.set('version', current_version)


def _init_metadata(engine, db_name):
    engine.execute("create database if not exists %s"%db_name)
    engine.execute("create table if not exists {db_name}.metadata (name varchar(64) primary key, value varchar(256))".format(db_name=db_name))
    try:
        engine.execute("insert into {db_name}.metadata (name, value) values ('version', '0')".format(db_name=db_name))
    except Exception as e:
        pass


def upgrade(current_version, session):
    if int(current_version) < 1:
        session.execute('''create table test (name varchar(256) primary key,
                        age int);
                     ''')

# VERSION = 1
# open(VERSION, 'root', 123456, 'localhost', 3306, 'test2', upgrade)