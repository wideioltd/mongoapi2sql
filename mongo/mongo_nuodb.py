#!/usr/bin/python2
import pynuodb
from uuid import uuid4 as uuid

from mongo_basic import MongoBasic


class MongoNuodb(MongoBasic):
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
            dict = {}
            for i in range(l):
                 dict[self.c.description[i][0]] = item[i]
            ids.append(dict)
        return ids

    def create_collection(self, name, fields):
        if "_id" in fields:
            del fields["_id"]
        l = []
        for field, type in fields.items():
            l.append("%s %s" % (field, type))
        l.append("_id string unique")
        for field in l:
            try:
                self.c.execute("create domain %s" % field)
            except:
                pass
        l = ", ".join(l)
        self.c.execute("create table if not exists %s (%s)" % (name, l))

    def update_by_ids(self, name, fields, values, ids):
        s = []
        for f, v in zip(fields, values):
            if type(values) == int:
                s.append("%s=%s" % (f, v))
            else:
                s.append("%s='%s'" % (f, v))
        ids = [(id, ) for id in ids]
        self.c.executemany("update %s set %s where _id=?" %
                           (name, ", ".join(s)), ids)

    @staticmethod
    def _add_coma_and_quote(s, opt):
        n = ""
        for a in s:
            if type(a) == str and opt == 1:
                n+= "'%s', " % a
            else:
                n += "%s, " % a
        return n

    def update_by_ids(self, name, fields, values, ids):
        s = []
        for f, v in zip(fields, values):
            s.append("%s=%s" % (str(f), str(v)))
        ids = [(id, ) for id in ids]
        self.c.executemany("update %s set %s where _id=?" %
                           (name, ", ".join(s)), ids)

    def insert_document(self, name, fields, values):
        id = uuid()
        fields.append("_id")
        values.append(str(id))
        s_fields = self._add_coma_and_quote(fields, 0)
        s_values = self._add_coma_and_quote(values, 1)
        self.c.execute("insert into %s (%s) values (%s)" %
                       (name, s_fields[:-2], s_values[:-2]))
        return id

    def begin_transaction(self):
        try:
            self.c.execute("start transaction")
        except:
            self.commit_transaction()
            self.begin_transaction()

    def commit_transaction(self):
	self.c.execute("commit")

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
                l.append("%s %s" % (field, type))
            s_fields = ", ".join(l)
            self.c.execute("alter table %s add column %s" % (name, s_fields))

    def drop_collection(self, name):
        self.c.execute("drop table %s" % name)
