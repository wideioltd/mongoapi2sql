#!/usr/bin/env python
import sqlite3
from mongo_db import MongoDb


class MongoSqlite3(MongoDb):
    # types for type not existing in sqlite3
    types = {
        "NoneType": "str",
    }

    def __init__(self):
        """
        Init object
        """
        self.db = None
        self.c = None

    def connect(self, path):
        """
        Connect yourself to your db
        """
        db = sqlite3.connect(path)
        db.row_factory = sqlite3.Row
        self.db = db
        self.c = db.cursor()
        return self.c

    def close(self):
        """
        Close your db
        """
        self.db.close()

    def collection_names(self):
        """
        Return a list of all collections
        """
        self.c.execute("select name from sqlite_master where type = 'table'")
        return map(lambda row: row['name'], self.c.fetchall())

    def select(self, name, filters, limit):
        """
        Return n <limit> documents of the collection <name> matching <filter>
        """
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
        """
        Create the collection named <name> with a set of field <fields>
        """
        if "_id" in fields:
            del fields["_id"]
        l = []
        for field, type in fields.items():
            l.append("%s %s" % (field, self.types.get(type, type)))
        l.append("_id INTEGER PRIMARY KEY ASC")
        l = ", ".join(l)
        self.c.execute("create table if not exists %s (%s)" % (name, l))

    def drop_collection(self, name):
        """
        Drop a collection and delete its documents
        """
        self.c.execute("drop table if exists %s" % name)

    @staticmethod
    def _add_coma_and_quote(s):
        n = ""
        for a in s:
            if type(a) == str:
                n += "'%s', " % a
            elif a == None:
                n += "'null', "
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
        """
        Insert a single document with fields=values in collection <name>
        with <fields> and <values> being list
        """
        s_fields = self._add_coma_and_quote(fields)
        s_values = self._add_coma_and_quote(values)
        self.c.execute("insert into %s (%s) values (%s)" %
                       (name, s_fields[:-2], s_values[:-2]))
        return self.c.lastrowid

    def update_by_ids(self, name, fields, values, ids):
        """
        Update by ids with fields=values in table <name>
        with <fields> and <values> as tuples
        """
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
        """
        Delete all documents in the collection <name>
        with matching ids <ids>
        """
        ids = [(id, ) for id in ids]
        self.c.executemany("delete from %s where _id=?" % name, ids)

    def add_fields(self, name, fields):
        """
        Add fields <fields> to the collection <name>
        if fields <fields> does not already exist
        """
        self.c.execute("select * from %s limit 1" % name)
        infos = self.c.description
        for info in infos:
            if info[0] in fields:
                del fields[info[0]]
        if len(fields) > 0:
            l = []
            for field, type in fields.items():
                l.append("%s %s" % (field, self.types.get(type, type)))
            s_fields = " ".join(l)
            for field in l:
                self.c.execute("alter table %s add column %s" % (name, field))

    def create_index(self, name, key, option, unique=False):
        """
        Create indexes <keys.values()> on <key.keys()> of
        the collection <name> with <options>
        """
        if unique is False:
            s = "create index %s_%s on %s (%s %s)"
        else:
            s = "create unique index %s_%s on %s (%s %s)"
        try:
            self.c.execute(s % (key, option, name, key, option))
        except sqlite3.OperationalError as e:
            print e

    def begin_transaction(self):
        """
        Begin a transaction
        """
        try:
            self.c.execute("BEGIN")
        except:
            self.commit_transaction()
            self.begin_transaction()

    def commit_transaction(self):
        """
        Commit the transaction
        """
        self.db.commit()

    def logout(self):
        """
        Logout
        """
        pass
