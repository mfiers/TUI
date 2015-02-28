
import os

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Table, Column, Integer, String, MetaData, DateTime
from sqlalchemy import Binary, Index


Base = declarative_base()


class TuiFile(Base):
    __tablename__ = "tui_files"

    id = Column(Integer, primary_key=True)
    fullpath = Column(String, nullable=False, unique=True)
    sha1sum = Column(Binary, nullable=False)
    filesize = Column(Integer)
    mtime = Column(DateTime)
    Index("fullpath")

    def __repr__(self):
        return "<File(..%s sha1='%s..')>" % (
            self.fullpath[-40:], self.sha1sum[:12]
        )


def get_dbpath(app):
    pth = app.conf.get('dbpath', '~/.local/db/tui')
    pth = os.path.expanduser(pth)
    if not os.path.exists(pth):
        os.makedirs(pth)
    return pth.rstrip('/') + '/'


def get_engine(app):
    dbpath = get_dbpath(app)
    engine = create_engine('sqlite:///{}/tui.sqlite'.format(dbpath))
    engine.connect()
    return engine


def get_session(app):
    engine = get_engine(app)

    Session = sessionmaker(bind=engine)
    Session.configure(bind=engine)
    session = Session()

    Base.metadata.create_all(engine)

    return session
