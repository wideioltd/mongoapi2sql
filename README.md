mongo-syntax
============

Mongodb syntax for database

Currently mongo_sqlite3 seems to work fine, although there is still a lot more to change

how to use
----------

### connection sqlite3

```python
from mongo.mongo_connector import Sqlite3Connector

db = Sqlite3Connector()
db.connect("/path/to/db/sqlite3")
```

### connection nuodb

```python
from mongo.mongo_connector import NuodbConnector

options = {
    "schema": "name"
}
db = NuodbConnector("db", "hostname", "user", "password", options)
```

### basic usage

test = db.test # test = db["test"] works fine too

```python
db.collection_names()
1  --> []


print test.find()
2  --> None


print test.insert( [ {"a": 1}, {"b": 2}, {"a": 42, "b": 3}, {"a": 2, "b": 3}  ] )
3  --> [1, 2, 3, 4]


print test.find({"a": 5})
4  --> []


print test.find({"b": 2})
5  --> [{'a': None, 'b': 2, '_id': 2}]


print test.find({"a": {"$lt": 2}})
6  --> [{'a': 1, 'b': None, '_id': 1}]


print test.find({"b":3})
7  --> [{'a': 42, 'b': 3, '_id': 3}, {'a': 2, 'b': 3, '_id': 4}]


print test.find({"b":3}).find({"a": 42})
8  --> [{'a': 42, 'b': 3, '_id': 3}]


print test.find({"b":3}).find({"a": 42}).find({"a": 1})
9  --> []


print test.find({"b": 3}).sort("a")
10 --> [{'a': 2, 'b': 3, '_id': 4}, {'a': 42, 'b': 3, '_id': 3}]


print test.find({"b": 3}).sort("a", MongoSyntax.DESC_SORT)
11 --> [{'a': 42, 'b': 3, '_id': 3}, {'a': 2, 'b': 3, '_id': 4}]


print test.find({"b": 3}).count()
12--> 2


print test.find({"b": 3}).remove()
13 --> None


print test.find({"b": 3}).count()
14 --> 1


print test.count()
15 --> 3


print test.remove(limit=None)
16 -->


print test.find()
17 --> []


print db.collection_names()
18 --> [u"test"]


print test.drop()
19 --> None


print db.collection_names()
10 --> []
```

how to implement your database
------------------------------

Take a look at MongoSqlite3 and MongoBasic
Create your own class !
