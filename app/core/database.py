from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from core import config


class Database:
    def __init__(self, database: str):
        self.engine = create_engine(config.get_connection_string(database))
        self.session_local = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)

    def get_db(self) -> Session:
        db = self.session_local()
        try:
            yield db
        finally:
            db.close()


# from core import config
# import pyodbc


# async def connect(database: str):
#     pyodbc.drivers()
#     global conexao
#     conexao = pyodbc.connect(config.connection_string)
#     global cursor
#     cursor = conexao.cursor()


# async def disconnect():
#     cursor.close()
#     conexao.close()
