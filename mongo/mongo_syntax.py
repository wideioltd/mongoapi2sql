#!/usr/bin/env python
from mongo_error import MongoError
from mongo_collection import MongoCollection

class MongoSyntax(object):
    """
    A class implementing the mongodb syntax for db
    """
    ASC_SORT = False
    DESC_SORT = True

    def __init__(self, class_db):
        """
        Init MongoSyntax, class_db should be the class implementation
        of your db inheriting from MongoBasic
        """
        self._class_db = class_db()
        self.args = None

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
        if self.args is None:
            raise MongoError("Mongosyntax is not connected to any db.")
        return MongoCollection(key, self._class_db)

    def connect(self, *args, **kwargs):
        """
        Connect to the db with *args and **kwargs arguments
        necessary for your db, path or ip for example
        """
        self.args = (args, kwargs)
        return self._class_db.connect(*args, **kwargs)

    def create_collection(self, name):
        """
        Create a collection if it does not exists
        """
        if self.args is None:
            raise MongoError("Mongosyntax is not connected to any db.")
        ret = self.__getitem__(name)
        ret._create_collection()
        return ret

    def drop_collection(self, name):
        """
        Drop a collection
        """
        if self.args is None:
            raise MongoError("Mongosyntax is not connected to any db.")
        return MongoCollection(name, self._class_db).drop()

    def collection_names(self):
        """
        Return a list of all collections
        """
        if self.args is None:
            raise MongoError("Mongosyntax is not connected to any db.")
        return self._class_db.collection_names()

    def logout(self, *args, **kwargs):
        """
        Logout
        """
        if self.args is None:
            raise MongoError("Mongosyntax is not connected to any db.")
        self._class_db.logout(*args, **kwargs)

    def close(self, *args, **kwargs):
        """
        Close the connection
        """
        if self.args is None:
            raise MongoError("Mongosyntax is not connected to any db.")
        self._class_db.close(*args, **kwargs)
