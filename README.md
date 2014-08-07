mongo-syntax
============

Mongodb syntax for database

Currently mongo_sqlite3 seems to work fine, although there is still a lot more to change

how to use
----------

from mongo.mongo_syntax import MongoSyntax
from mongo.mongo_sqlite3 import MongoSqlite3

db = MongoSyntax(MongoSqlite3)
db.connect("/path/to/db/sqlite3")
db.collection_names()

test = db.test
print test.find()
test.insert({"field":"value"})
print test.find()

how to implement your database
------------------------------

Take a look at MongoSqlite3 and MongoBasic
Create your own class !
