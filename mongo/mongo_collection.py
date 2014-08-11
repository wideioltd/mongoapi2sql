#!/usr/bin/env python
from mongo_match import MongoMatch
from mongo_error import MongoError
from functools import wraps


class MongoCollection(MongoMatch):
    def __init__(self, key, db, collection=None,
                 parent=None, query=[], max=None):
        """
        This should be called from MongoSyntax
        """
        self.name = key
        self.max = max
        self._db = db
        self.collection = collection
        self._parent = parent
        self._cur_query = []
        self._query = query

    def set_max(self, max):
        self.max = max
        return self

    def __str__(self):
        """
        Return string corresponding to the documents
        """
        return repr(self.collection)

    def __getitem__(self, key):
        """
        Return index of collection
        """
        return self.collection[key]

    def _transaction(f):
        """
        Start end end a transaction if everything went well
        """
        @wraps(f)
        def transaction(*args, **kwargs):
            args[0]._db.begin_transaction()
            ret = f(*args, **kwargs)
            args[0]._db.commit_transaction()
            return ret
        return transaction

    def _clear_parent(self):
        """
        Clear all collections
        """
        c = self
        while c is not None:
            c.collection = None
            c = c._parent

    def _get_documents(self, filters, limit):
        """
        Get a list of documents matching filters
        """
        query = MongoMatch.match_object(filters)
        self._cur_query = list(self._query)
        self._cur_query.append(query)
        return self._db.select(self.name, self._cur_query, limit)

    @_transaction
    def _create_collection(self, objects=None):
        """
        Create a collection if it does not exist
        Add missing fields to the collection
        """
        d = {}
        if objects is not None:
            for object in objects:
                for key, val in object.items():
                    if key not in d:
                        d[key] = type(val).__name__
        if self.collection is None:
            self._db.create_collection(self.name, d)
        self._db.add_fields(self.name, d)
        return None

    def _ids_matching_document(self, limit, filters={}):
        """
        Returns _id of matching documents
        """
        res = self._find(filters, limit)
        if res is None:
            return None
        collection = res.collection
        try:
            return [document["_id"] for document in collection]
        except KeyError:
            return [document["_ID"] for document in collection]

    def rename(self, name):
        """
        Rename current collection
        """
        if self._db.rename(name) is True:
            self.name = name
            return True
        return False

    def _find(self, filters, limit):
        """
        Return a new MongoCollection with the current query
        """
        collection = self._get_documents(filters, limit)
        if collection is None:
            return None
        formated = []
        for item in collection:
            d = {}
            for k in item.keys():
                d.update({k: item[k]})
            formated.append(d)
        return MongoCollection(self.name, self._db, formated,
                               self, self._cur_query, self.max)

    def find_one(self, filters={}):
        """
        Return the first element matching with the filters
        """
        return self._find(filters, limit=1)

    def find(self, filters={}):
        """
        Return elements matching with the filters
        """
        return self._find(filters, limit=self.max)

    @_transaction
    def _insert(self, objects):
        """
        Insert new objects in the current collections and return their ids
        """
        ids = []
        objects = filter(lambda obj: obj != {}, objects)
        for obj in objects:
            ids.append(self._db.insert_document(
                self.name, obj.keys(), obj.values()))
        self._clear_parent()
        return ids

    @_transaction
    def _insert_bulk(self, objects):
        """
        Insert new objects by bulk in the current collections
        and return their ids
        """
        ids = self.db.insert_document_bulk(self.name, objects)
        self._clear_parent()
        return ids

    def insert_bulk(self, objects):
        """
        Insert new objects by bulk in the current collections
        and return their ids
        """
        if isinstance(objects, list) is False:
            object = [objects]
        self._create_collection(objects)
        return self._insert_bulk(objects)

    def insert(self, objects):
        """
        Insert new objects in the current collections and return their ids
        """
        if isinstance(objects, list) is False:
            objects = [objects]
        self._create_collection(objects)
        return self._insert(objects)

    @_transaction
    def _drop_collection(self):
        """
        Drop the current collection
        """
        return self._db.drop_collection(self.name)

    def drop(self):
        """
        Drop the current collection
        """
        return self._drop_collection()

    def save(self, document):
        """
        Save the document in the collection,
        if document has an '_id' and a document already have this id
        it will be replaced
        """
        # TODO: implementation
        raise MongoError("save not yet implemented")

    @_transaction
    def _update(self, object, ids):
        """
        Update documents by object all matching ids
        """
        return self._db.update_by_ids(self.name, object.keys(), object.values(), ids)

    def update(self, object, rules={}, upsert=False, limit=1):
        """
        Update documents by object all matching ids
        """
        if "_id" in object and (limit > 1 or limit is None):
            raise MongoError("Multiple documents with the same_id, error")
        ids = self._ids_matching_document(limit, rules)
        if ids is not None:
            self._clear_parent()
            return self._update(object, ids)
        elif upsert is True:
            return self.insert(object)

    @_transaction
    def _remove(self, ids):
        """
        Remove documents matching ids
        """
        return self._db.delete_documents(self.name, ids)

    def remove(self, rules={}, limit=1):
        """
        Remove documents matching ids
        """
        ids = self._ids_matching_document(limit, rules)
        if ids is None:
            return None
        self._clear_parent()
        return self._remove(ids)

    def count(self):
        """
        Return the length of the collection or
        return the length of a collection matching precedents queries
        """
        if isinstance(self.collection, dict):
            return 1
        elif self.collection is not None:
            return len(self.collection)
        collection = self.find()
        if collection is None:
            return 0
        return len(collection.collection)

    def sort(self, key, direction=False):
        """
        Sort the collection matching precedent queries  by key
        """
        if self.collection is None:
            return None
        elif isinstance(self.collection, list) is False:
            return self.collection
        self.collection.sort(key=lambda document: document[key], reverse=direction)
        return self

    def create_index(self, indexes):
        """
        Create indexes on the current collection
        """
        for index in indexes:
            self._db.create_index(self.name, index[0], index[1])

    def find_and_modify(self, query={}, update=None, new=False,
                        remove=False, upsert=False, full_response=False):
        if remove is True:
            return self.remove(query)
        else:
            if new is False:
                if self.find(query).collection == [] and upsert is True:
                    return self.insert(update)
                return self.update(update, query, upsert)
            else:
                old = self.find(query)[0]
                if old.collection != []:
                    self._update([old["_id"]])
                elif upsert is True:
                    self.insert(update)
                return old
