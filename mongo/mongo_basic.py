#!/usr/bin/env python
from mongo_error import MongoError


class MongoBasic(object):
    """
    The db you want to use should inherit from this class
    """
    def connect(self, *args, **kwargs):
        """
        Connect yourself to your db
        """
        raise MongoError("connect not yet implemented")

    def collection_names(self):
        """
        Return a list of all collections
        """
        raise MongoError("collections_names not yet implemented")

    def close(self, *args, **kwargs):
        """
        Close your db
        """
        raise MongoError("close not yet implemented")

    def select(self, name, filters, limit):
        """
        Return <limit> documents of the collection <name> matching <filter>
        """
        raise MongoError("all_element not yet implemented")

    def create_collection(self, name, fields):
        """
        Create the collection named <name> with a set of field <fields>
        """
        raise MongoError("create_table not yet implemented")

    def drop_collection(self, name):
        """
        Drop a collection and delete its documents
        """
        raise MongoError("drop_collection not yet implemented")

    def insert_document(self, name, fields, values):
        """
        Insert a single document with fields=values in collection <name>
        with <fields> and <values> being list
        """
        raise MongoError("insert_element not yet implemented")

    def update_by_ids(self, name, fields, values, ids):
        """
        Update by ids with fields=values in table <name>
        with <fields> and <values> as tuples
        """
        raise MongoError("update_by_ids not yet implemented")

    def delete_documents(self, name, ids):
        """
        Delete all documents in the collection <name>
        with matching ids <ids>
        """
        raise MongoError("delete_documents not yet implemented")

    def add_fields(self, name, fields):
        """
        Add fields <fields> to the collection <name>
        if fields <fields> does not already exist
        """
        raise MongoError("add_fields not yet implemented")

    def create_index(self, name, keys, options):
        """
        Create indexes <keys.values()> on <key.keys()> of
        the collection <name> with <options>
        """
        raise MongoError("create_index not yet implemented")

    def begin_transaction(self):
        """
        Begin a transaction
        """
        raise MongoError("begin_transaction not yet implemented")

    def commit_transaction(self):
        """
        Commit the transaction
        """
        raise MongoError("commit_transaction not yet implemented")

    def logout(self, *args, **kwargs):
        """
        Logout
        """
        raise MongoError("logout not yet implemented")
