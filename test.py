#!/usr/bin/env python2

from mongo.mongo_connector import Sqlite3Connector

db = Sqlite3Connector()
db.connect("test.sqlite3")
test = db.test
print "1  ----- ", db.collection_names()
print "2  ----- ", test.find()
print "3  ----- ", test.insert( [ {"a": 1}, {"b": 2}, {"a": 42, "b": 3}, {"a": 2, "b": 3}  ] )
print "4  ----- ", test.find({"a": 5})
print "5  ----- ", test.find({"b": 2})
print "6  ----- ", test.find({"a": {"$lt": 2}})
print "7  ----- ", test.find({"b": 3})
print "8  ----- ", test.find({"b": 3}).find({"a": 42})
print "9  ----- ", test.find({"b": 3}).find({"a": 42}).find({"a": 1})
print "10 ----- ", test.find({"b": 3}).sort("a")
print "11 ----- ", test.find({"b": 3}).sort("a", Sqlite3Connector.DESC_SORT)
print "12 ----- ", test.find({"b": 3}).count()
print "13 ----- ", test.find({"b": 3}).remove()
print "14 ----- ", test.find({"b": 3}).count()
print "15 ----- ", test.count()
print "16 ----- ", test.remove(limit=None)
print "17 ----- ", test.find()
print "18 ----- ", db.collection_names()
print "19 ----- ", test.drop()
print "20 ----- ", db.collection_names()
