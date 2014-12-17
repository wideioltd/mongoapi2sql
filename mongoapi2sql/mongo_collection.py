#!/usr/bin/env python
from mongo_match import MongoMatch
from mongo_error import MongoError
from mongo_vars import MongoVars
from functools import wraps
from time import time
import json

VERBOSE=1

#todo: refactor as a field or a function
import os
DISABLE_AUTOMATIC_SCHEMA_MODIFICATIONS = os.getenv('MONGOAPI2SQL_DISABLE_AUTOMATIC_SCHEMA_MODIFICATIONS', "1")
DISABLE_AUTOMATIC_SCHEMA_MODIFICATIONS = int(DISABLE_AUTOMATIC_SCHEMA_MODIFICATIONS)
if VERBOSE:
    if DISABLE_AUTOMATIC_SCHEMA_MODIFICATIONS:
        print "DISABLED "+str(DISABLE_AUTOMATIC_SCHEMA_MODIFICATIONS)
    else:
        print "ENABLED "+str(DISABLE_AUTOMATIC_SCHEMA_MODIFICATIONS)


class MongoCollection(MongoVars, MongoMatch):
    IDFIELD="id"

    def __init__(self, key, db, collection=None,
                 parent=None, query=[], max=None):
        """
        This should be called from MongoSyntax
        """
        self.name = key
        self.max = max
        self.collection = collection
        self._db = db
        self._parent = parent
        self._cur_query = []
        self._query = query
        self._index_cache = {}

    def __iter__(self):
        if self.collection == []:
            raise StopIteration
        for i in self.collection:
            yield i

    def set_max(self, max):
        """
        Limit the numbers of matching objects in queries
        """
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
        if self.collection == []:
            return None
        return self.collection[key]

    def _transaction(f):
        """
        Start and end a transaction if everything went well
        """
        @wraps(f)
        def transaction(*args, **kwargs):
            #try:
            args[0]._db.begin_transaction()
            ret = f(*args, **kwargs)
            #except Exception as e:
            #raise e
            #finally:
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

    def _get_documents(self, filters, limit, order=""):
        """
        Get a list of documents matching filters
        """
        for k in filters.keys():
            if "." in k:
                filters[k.replace(".", "__")] = filters[k]
                del filters[k]

        filters = self._restruct_object(filters)
        filters = MongoMatch.match_object(filters)
        self._cur_query = list(self._query)
        self._cur_query.append(filters + order)
        return self._db.select(self.name, self._cur_query, limit)

    @_transaction
    def _create_collection(self, objects=None, prefix=""):
        """
        Create a collection if it does not exist
        Add missing fields to the collection
        """
        objects = [self._restruct_object(object) for object in objects]
        d = {}
        if objects is not None:
            for object in objects:
                for key, val in object.items():
                    if key != "$set" and key != "$push" \
                    and key != "$pull" and key != "$inc":
                        if type(val) != dict:
                            d[prefix + key] = type(val).__name__
                        else:
                            d[prefix + key] = "str"
                    else:
                        for k, v in val.items():
                            d[prefix + k] = type(v).__name__

        if self.collection is None:
            if not DISABLE_AUTOMATIC_SCHEMA_MODIFICATIONS:
                self._db.create_collection(self.name, d)
        if not DISABLE_AUTOMATIC_SCHEMA_MODIFICATIONS:
            self._db.add_fields(self.name, d)
        return None

    def _get_documents_ids(self, documents):
        """
        Return the ids of the documents
        """
        if documents == []:
            return []
        return [document[MongoCollection.IDFIELD] for document in documents]

    def _matching_document(self, limit, filters={}, order=""):
        """
        Returns _id of matching documents
        """
        res = self._find(filters, limit, order)
        if res is None:
            return None
        return res.collection

    def rename(self, name):
        """
        Rename current collection
        """
        if self._db.rename(name) is True:
            self.name = name
            return True
        return False

    def _find(self, filters, limit, order=""):
        """
        Return a new MongoCollection with the current query
        """
        collection = self._get_documents(filters, limit)
        if collection is None or collection == []:
            return MongoCollection(self.name, self._db, [], self, self._cur_query, self.max)
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
            obj = self._restruct_object(obj)
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

    def _restruct_object(self, object, prefix=""):
        """
        Return a simple restructured dict
        {a: {b:42, c:"42"}} --> {a__b: 42, a__c:"42"}
        """
        if type(object) != dict:
            return object
        d = {}
        for k, v in object.items():
            if k in ["$push", "$inc", "$pull"]:
                d.update({k: self._restruct_object(v, prefix)})
            else:
                k = k.replace(".", "__")
                t = type(v)
                if t is dict and len(v) != 0 and "$" not in k and "$" not in v.keys()[0]:
                    d.update(self._restruct_object(v, prefix + k + self.SEP))
                #elif t is list or t is tuple:
                #    d[prefix + k] = json.dumps(v)
                else:
                    d[prefix + k] = v
        return d

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
        object = self._restruct_object(object)
        self._create_collection([object])
        return self._db.update_by_ids(self.name, object.keys(), object.values(), ids)

    def update(self, rules={}, object={}, upsert=False, limit=1):
        """
        Update documents by object all matching ids
        """
        #print("U12 %r"%(rules))
        #bug!! rules and object are confused!
        if object == {}:
            return []
        if (MongoCollection.IDFIELD.lower() in object or MongoCollection.IDFIELD.upper() in object) and (limit > 1 or limit is None):
            raise MongoError("Multiple documents with the same_id, error")
        object = self._restruct_object(object)
        documents = self._matching_document(limit, rules)
        if documents != []:
            ids = self._get_documents_ids(documents)
            self._clear_parent()
            self._update(object, ids)
            return documents
        elif upsert is True:
            self.insert(object)
            return []

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
        documents = self._matching_document(limit, rules)
        if documents == []:
            return []
        self._clear_parent()
        self._remove(self._get_documents_ids(documents))
        return documents

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

    @_transaction
    def create_index(self, indexes, unique=False):
        """
        Create indexes on the current collection
        """
        if isinstance(indexes, list) is False:
            indexes = [indexes]
        for index in indexes:
            self._db.create_index(self.name, index[0], index[1], unique)

    @_transaction
    def drop_index(self, index):
        """
        Drop index <index> of the current collection
        """
        self._db.drop_index(self.name, index)

    @_transaction
    def ensure_index(self, key_or_list, unique=False, ttl=300):
        """
        Create index if not exist and cache it for <ttl> seconds
        """
        if type(key_or_list) != list:
            key_or_list = [key_or_list]
        for index in key_or_list:
            v = self._index_cache.get(index, 0)
            t = time()
            if t - v < ttl:
                continue
            else:
                self._index_cache.update({index: t})
                self._db.create_index(self.name, index[0], index[1], unique)

    def index_information(self):
        """
        Display informations on the current index
        """
        return self._db.index_information(self.name)

    def drop_indexes(self):
        """
        Drop all indexes in the current collection
        """
        for index in self.index_information().keys():
            self.drop_index(index)

    def find_and_modify(self, query={}, update=None, new=False, sort="",
                        remove=False, upsert=False, full_response=False):
        """
        Find and modify a single document, update or remove must be set
        full_response not yet implemented
        """

        d = {1: "ASC", -1: "DESC"}
        if sort != "":
            s_list = []
            for k, v in sort.items():
                s_list.append("%s %s" % (k, d[v]))
            sort = " order by %s" % ", ".join(s_list)

        self._db.begin_transaction()
        documents = self._matching_document(limit=1, filters=query, order=sort)
        if documents != []:
            if remove is True:
                res = self._remove(self._get_documents_ids(documents))
            else:
                if "$set" in update and len(update) == 1:
                    update = update["$set"]
                res = self._update(update, self._get_documents_ids(documents))
            if new is False:
                res = documents[0]
        else:
            if upsert is True:
                # FIXME: check self_insert
                res= self._insert([update])
            if new is False:
                res= None
        self._db.commit_tranaction()
        return res
