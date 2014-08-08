mongo-syntax
============

Mongodb syntax for database

Currently mongo_sqlite3 seems to work fine, although there is still a lot more to change

how to use
----------

### connection sqlite3

```python
from mongo.mongo_syntax import MongoSyntax
from mongo.mongo_sqlite3 import MongoSqlite3

db = MongoSyntax(MongoSqlite3)
db.connect("/path/to/db/sqlite3")
```

### connection nuodb

```python
from mongo.mongo_syntax import MongoSyntax
from mongo.mongo_sqlite3 import MongoNuodb

db = MongoSyntax(MongoNuodb)
options = {
    "schema": "name"
}
db.connect("db", "hostname", "user", "password", options)
```

### basic usage

```python
db.collection_names()
--> ["collection1", "collection2", ...]


test = db.test || test = db["test"]
print test.find()
--> []


print test.insert( {"field": "string", "field2": 42} )
--> [1]


print test.find()
--> [ {"field": "string", "field2": 42} ]


print test.insert( [ {"a": 1}, {"b": 2}, {"a": 42, "b": 3}, {"a": 2, "b": 3}  ] )
--> [2, 3, 4, 5]


print test.find({"a": 5})
--> None


print test.find({"b": 2})
--> [{"b": 2, "_id": 3}]


print test.find({"a": {"$lt": 2}})
--> [{"a": 1, "_id": 2}]


print test.find({"b":3})
--> [{"a": 42, "b": 3, "_id": 4 }, {"a": 2, "b": 3, "_id": 5} ]


print test.find({"b":3}).find({"a": 42})
--> [{"a": 42, "b": 3, "_id": 4}]


print test.find({"b":3}).find({"a": 42}).find({"a": 1})
--> None


print test.find({"b": 3}).sort("a")
--> [{"a": 2, "b": 3, "_id": 5}, {"a": 42, "b": 3, "_id": 4}]


print test.find({"b": 3}).sort("a", MongoSyntax.DESC_SORT)
--> [{"a": 42, "b": 3, "_id": 4}, {"a": 2, "b": 3, "_id": 5}]


print test.find({"b": 3}).count()
--> 2


test.find({"b": 3}).remove()
print test.find({"b": 3}).count()
--> 1


print test.count()
--> 4


test.remove(limit=None)
print test.find()
--> []
```

how to implement your database
------------------------------

Take a look at MongoSqlite3 and MongoBasic
Create your own class !
