mongo-syntax
============

Mongodb syntax for database

Currently mongo_sqlite3 seems to work fine, although there is still a lot more to change

### Installation:
sudo python ./setup.py install

how to use (new)
----------------
new usage:
# import mongoapi2sql.mongo_connector as mongo_connector  #not anymore
# new usage:
import mongoapi2sql
mongoapi2sql.NuodbConnector
# or:
from mongoapi2sql.mongo_connector import NuodbConnector


how to use (old)
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

#### how to select a collection

```python
test = db.test
test = db["test"]
```

#### how to query a document

```python
test = db.test
test.find() # get all elements
test.find({"field": {"$gt": 42}}) # get alls elements with the <field> "field" > 42
test.find_one() # get one element
```

#### how to insert a document

```python
test = db.test
test.insert({"field": "value"})
test.insert([{"field": "value"}, {"field": "value2"}])
test.insert({"a": {"b": 1, "c": 2}}) # this will create field a__b and a__c
```

This will create all necessary fields, for yours objects

#### how to remove a document

```python
test = db.test
test.remove() # remove one element
test.remove({"a": 42}) # remove one element matching the query
test.remove(limit=None) # remove all elements
```

#### indexes

```python
test = db.test
test.index_information()
test.create_index([("field", "index_name"), ("field2", "index2")])
test.drop_index("index_name")
test.create_index(("field", "unique_index"), unique=True)
test.drop_indexes()
```

#### some other functions

```python
test = db.test # test = db["test"] works fine too

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

Take a look at MongoSqlite3 and MongoDb
Create your own class !
