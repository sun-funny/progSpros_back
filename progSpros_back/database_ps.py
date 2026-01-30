from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy import create_engine
from progSpros_back.config_ps import Config
from flask import g
# # Создание движка SQLAlchemy
# engine = create_engine(Config.SQLALCHEMY_DATABASE_URI, pool_size=30)
# db = scoped_session(sessionmaker(bind=engine))

class SingletonMeta(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super().__call__(*args, **kwargs)
        return cls._instances[cls]

class DB(metaclass=SingletonMeta):
    def __init__(self, direct: bool):
        self._direct = direct
        if direct:
            self.engine = create_engine(Config.SQLALCHEMY_DATABASE_URI, pool_size=30)
            self.session = scoped_session(sessionmaker(bind=self.engine))
    def session(self):
        return self.session if self._direct else g.session

def set_db_connection():
    db = DB(direct=True) 
    return db.session
def get_db_engine():
    db = DB(direct=True)
    return db.engine   

from flask_caching import Cache

cache = Cache()

def errorhandler(e):
    # Check for specific error messages
    if str(e) == "max() arg - это пустая последовательность":
        return 502, "Нет данных для указанных условий"
    elif isinstance(e, ValueError):
        return 501, f"Неверный ввод: {str(e)}"
    elif isinstance(e, KeyError):
        return 504, f"Ключ не найден: {str(e)}"
    elif isinstance(e, PermissionError):
        return 503, f"Доступ запрещен: {str(e)}"
    # You can add more specific exception types as needed
    else:
        # Default error message for unhandled exceptions
        return 500, f"Произошла непредвиденная ошибка: {str(e)}"