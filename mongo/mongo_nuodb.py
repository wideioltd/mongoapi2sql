#!/usr/bin/python2
import pynuodb
from uuid import uuid4 as uuid

from mongo_basic import MongoBasic


class MongoNuodb(MongoBasic):
    types = {
        "str": "string",
        "bool": "boolean",
        "NoneType": "string",
    }

    indexes = {
	1: "ASC",
	-1: "DESC",
    }

    def __init__(self):
        self.db = None
        self.c = None

    def connect(self, db, host, user, password, options={"schema": "mongo-syntax"}):
        self.db = pynuodb.connect(db, host, user, password, options)
        self.c = self.db.cursor()
        return self.c

    def close(self):
        self.db.close()

    def collection_names(self):
        self.c.execute("show tables")
        return self.c.fetchall()

    @staticmethod
    def _restruct_subobject(val, key, dict):
        index = key.find("__")
        k = key[index + 2:]
        d = dict.get(k, {})
        if "__" in k:
            val = MongoNuodb.restruct_subobject(val, k, d)
        else:
            d.update({k: val})
        dict[key[:index]] = d
        return dict

    @staticmethod
    def _restruct_object(object, keys, l):
        dict = {}
        for i in range(l):
            key = keys[i][0].lower()
            if "__" in key:
                MongoNuodb._restruct_subobject(object[i], key, dict)
            else:
                dict[key] = object[i]
        return dict

    def select(self, name, filters, limit):
        all = "select * from %s" % name
        l = []
        for filter in filters:
            if filter != "":
                l.append("%s where %s" % (all, filter))
            else:
                l.append(all)
        res = " intersect ".join(l)
        if res == "":
            res = all
        if limit is not None:
            res += " limit %s" % limit
	try:
	    self.c.execute(res)
        except pynuodb.ProgrammingError:
            return None
        fetched = self.c.fetchall()
        ids = []
        try:
            l = len(fetched[0])
        except IndexError:
            return None
        for item in fetched:
            dict = MongoNuodb._restruct_object(item, self.c.description, l)
            ids.append(dict)
        return ids

    def create_collection(self, name, fields):
        if "_id" in fields:
            del fields["_id"]
        if "_ID" in fields:
            del fields["_ID"]
        l = []
        for field, type in fields.items():
            l.append("%s %s" % (field, self.types.get(type, type)))
        l.append("_id string unique")
        l = ", ".join(l)
        self.c.execute("create table if not exists %s (%s)" % (name, l))

    def drop_collection(self, name):
        self.c.execute("drop table %s" % name)

    @staticmethod
    def _add_coma_and_quote(s, opt):
        n = ""
        for a in s:
            t = type(a)
            if a is None and opt == 1:
                n += "'null', "
            elif t == int or t == float or opt == 0:
                n += "%s, " % a
            else:
                n+= "'%s', " % a

        return n

    def insert_document(self, name, fields, values):
        id = uuid()
        fields.append("_id")
        values.append(str(id))
        s_fields = self._add_coma_and_quote(fields, 0)
        s_values = self._add_coma_and_quote(values, 1)
        self.c.execute("insert into %s (%s) values (%s)" %
                       (name, s_fields[:-2], s_values[:-2]))
        return id

    def update_by_ids(self, name, fields, values, ids):
        s = []
        for f, v in zip(fields, values):
	    t = type(v)
            if t != int and t != float:
                s.append("%s='%s'" % (f, v))
            else:
                s.append("%s=%s" % (f, v))
        ids = [(id, ) for id in ids]
        self.c.executemany("update %s set %s where _id=?" %
                           (name, ", ".join(s)), ids)

    def delete_documents(self, name, ids):
        ids = [(id, ) for id in ids]
        self.c.executemany("delete from %s where _id=?" % name, ids)

    def add_fields(self, name, fields):
        self.c.execute("select * from %s limit 1" % name)
        infos = self.c.description
        for info in infos:
            if info[0].lower() in fields:
                del fields[info[0].lower()]
        if len(fields) > 0:
            l = []
            for field, type in fields.items():
                l.append("%s %s" % (field, self.types.get(type, type)))
            s_fields = ", ".join(l)
            self.c.execute("alter table %s add column %s" % (name, s_fields))

    def create_index(self, name, key, option, unique=False):
        if unique is False:
            s = "create index %s_%s on %s (%s %s)"
        else:
            s = "create unique index %s_%s on %s (%s %s)"
        try:
	    key = key.replace(".", "__")
            self.c.execute(s % (key, self.indexes.get(option, option), name, key, self.indexes.get(option, option)))
        except:
	    pass

    def drop_index(self, name, index):
	self.c.execute("drop index %s" % index)

    def index_information(self, name):
	self.c.execute("show table %s" % name)
	l = filter(lambda s: "on field" in s, self.c.fetchall()[0][0].split('\n'))
	indexes = {}
	for index in l:
	    sp = index.split()
	    r = 1 if "ASC" in sp[2] else -1
	    d = {'key': [(sp[-1], r)]}
	    if sp[0] != "Secondary":
		d.update(unique=True)
	    indexes.update({sp[2]:d})	    
	return indexes

    def begin_transaction(self):
        try:
            self.c.execute("start transaction")
        except:
            self.commit_transaction()
            self.begin_transaction()

    def commit_transaction(self):
	self.c.execute("commit")

    def logout(self):
        pass
