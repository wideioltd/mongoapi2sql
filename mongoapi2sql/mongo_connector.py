from mongo_syntax import MongoSyntax
from mongo_nuodb import MongoNuodb
from mongo_sqlite3 import MongoSqlite3
from mongo_sqlalchemy import MongoSqlAlchemy

class NuodbConnector(MongoSyntax):
    DB = MongoNuodb

class Sqlite3Connector(MongoSyntax):
    DB = MongoSqlite3

class SqlAlchemyConnector(MongoSyntax):
    DB = MongoSqlAlchemy
