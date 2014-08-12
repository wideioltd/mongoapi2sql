#!/usr/bin/env python
from mongo_error import MongoError
from mongo_collection import MongoCollection

class MongoSyntax(object):
    """
    A class implementing the mongodb syntax for db
    """
    ASC_SORT = False
    DESC_SORT = True
    DB = None

    def __init__(self, *args, **kwargs):
        """
        Init MongoSyntax
        """
        if callable(self.DB) is False:
            raise MongoError("DB not set to any class")
        self._class_db = self.DB()
        if len(args) > 0 or len(kwargs) > 0:
            self.connect(*args, **kwargs)

    def __getattribute__(self, attr):
        """
        Return a collection of the db
        """
        try:
            return object.__getattribute__(self, attr)
        except AttributeError:
            return self.__getitem__(attr)

    def __getitem__(self, key):
        """
        Return a collection of the db
        """
        return MongoCollection(key, self._class_db)

    def connect(self, *args, **kwargs):
        """
        Connect to the db with *args and **kwargs arguments
        necessary for your db, path or ip for example
        """
        self._class_db.connect(*args, **kwargs)

    def create_collection(self, name):
        """
        Create a collection if it does not exists
        """
        ret = self.__getitem__(name)
        ret._create_collection()
        return ret

    def drop_collection(self, name):
        """
        Drop a collection
        """
        return MongoCollection(name, self._class_db).drop()

    def collection_names(self):
        """
        Return a list of all collections
        """
        return self._class_db.collection_names()

    def logout(self, *args, **kwargs):
        """
        Logout
        """
        self._class_db.logout(*args, **kwargs)

    def close(self, *args, **kwargs):
        """
        Close the connection
        """
        self._class_db.close(*args, **kwargs)
