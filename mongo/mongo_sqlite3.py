#!/usr/bin/env python
import sqlite3
from mongo_basic import MongoBasic


class MongoSqlite3(MongoBasic):
    def __init__(self):
        self.db = None
        self.c = None

    def connect(self, path):
        db = sqlite3.connect(path)
        db.row_factory = sqlite3.Row
        self.db = db
        self.c = db.cursor()
        return self.c

    def close(self):
        self.db.close()

    def collection_names(self):
        self.c.execute("select name from sqlite_master where type = 'table'")
        return map(lambda row: row['name'], self.c.fetchall())

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
        except sqlite3.OperationalError:
            return None
        return self.c.fetchall()

    def create_collection(self, name, fields):
        if "_id" in fields:
            del fields["_id"]
        l = []
        for field, type in fields.items():
            l.append("%s %s" % (field, type))
        l.append("_id INTEGER PRIMARY KEY ASC")
        l = ", ".join(l)
        self.c.execute("create table if not exists %s (%s)" % (name, l))

    def drop_collection(self, name):
        self.c.execute("drop table if exists %s" % name)

    @staticmethod
    def _add_coma_and_quote(s):
        n = ""
        for a in s:
            if type(a) == str:
                n+= "'%s', " % a
            elif type(a) == bool:
                # boolean does not seem to exist unfortunately in sqlite
                if a == True:
                    n += "1, "
                else:
                    n += "0, "
            else:
                n += "%s, " % a
        return n

    def insert_document(self, name, fields, values):
        s_fields = self._add_coma_and_quote(fields)
        s_values = self._add_coma_and_quote(values)
        self.c.execute("insert into %s (%s) values (%s)" %
                       (name, s_fields[:-2], s_values[:-2]))
        return self.c.lastrowid

    def update_by_ids(self, name, fields, values, ids):
        s = []
        for f, v in zip(fields, values):
            if type(v) == str:
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
            if info[0] in fields:
                del fields[info[0]]
        if len(fields) > 0:
            l = []
            for field, type in fields.items():
                l.append("%s %s" % (field, type))
            s_fields = ", ".join(l)
            self.c.execute("alter table %s add column %s" % (name, s_fields))

    def create_index(self, name, keys, options):
        for k, v in keys.items():
            try:
                self.c.execute("create index %s on %s (%s %s)" % (v, name, k, options))
            except sqlite3.OperationalError:
                pass

    def begin_transaction(self):
        self.c.execute("BEGIN")

    def commit_transaction(self):
        self.db.commit()

    def logout(self):
        pass
