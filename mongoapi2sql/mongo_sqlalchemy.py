#!/usr/bin/python
import sqlalchemy
from uuid import uuid4 as uuid
from mongo_db import MongoDb


class MongoSqlAlchemy(MongoDb):
    # types for type not existing in nuodb
    # IDFIELD="_id" #default
    IDFIELD = "id"  # default

    types = {
        "str": "string",
        "bool": "boolean",
        "NoneType": "string",
    }

    # corresponding options for indexes
    indexes = {
        1: "ASC",
        -1: "DESC",
    }

    def __init__(self):
        """
        Init object
        """
        self.db = None
        self.c = None

    def connect(self, url=None,engine=None, db=None, host=None, user=None, password=None, options={"schema": "mongo-syntax"}):
        """
        Connect yourself to your db
        """
        if url is not None:
            engine = sqlalchemy.create_engine(url)            
        else:
            if engine is None:
                raise ValueError
            if db is None:
                raise ValueError            
            if host is None:
                raise ValueError
            if user is None:
                raise ValueError            
            if password is None:
                raise ValueError
            engine = sqlalchemy.create_engine('{engine}://{user}:{password}@{host}/{db}'.format(engine=engine,user=user, password=password, format=host, db=db))

        self.db = engine
        self.c = engine.connect()
        #self.c.execute("SET AUTOCOMMIT ON;")
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
        self.c.execute("show tables")
        return self.c.fetchall()

    @staticmethod
    def _restruct_subobject(val, key, dict):
        """
        Find object with a particular pattern and restruct it
        a__b: 42 --> {a: {b: 42}}
        """
        index = key.find(MongoSqlAlchemy.SEP)
        k = key[index + 2:]
        d = dict.get(k, {})
        if MongoSqlAlchemy.SEP in k:
            val = MongoSqlAlchemy.restruct_subobject(val, k, d)
        else:
            d.update({k: val})
        r = dict.get(key[:index], {})
        if r == {}:
            dict[key[:index]] = d
        else:
            r.update(d)
        return dict

    @staticmethod
    def _restruct_object(object, keys, l):
        """
        Restructure the object to create a dictionary with lowercase keys
        """
        dict = {}
        for i in range(l):
            key = keys[i][0].lower()
            if MongoSqlAlchemy.SEP in key:
                MongoSqlAlchemy._restruct_subobject(object[i], key, dict)
            else:
                dict[key] = object[i]
        return dict

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
        self.c.execute(res)
        fetched = self.c.fetchall()
        items = []
        try:
            l = len(fetched[0])
        except IndexError:
            return None
        for item in fetched:
            dict = MongoSqlAlchemy._restruct_object(item, self.c.description, l)
            items.append(dict)
        return items

    def create_collection(self, name, fields):
        """
        Create the collection named <name> with a set of field <fields>
        """
        if MongoSqlAlchemy.IDFIELD in fields:
            del fields[MongoSqlAlchemy.IDFIELD]
        l = []
        for field, type in fields.items():
            l.append("%s %s" % (field, self.types.get(type, type)))
        l.append(MongoSqlAlchemy.IDFIELD + " string unique")
        l = ", ".join(l)
        self.c.execute("create table if not exists %s (%s)" % (name, l))

    def drop_collection(self, name):
        """
        Drop a collection and delete its documents
        """
        self.c.execute("drop table %s" % name)

    @staticmethod
    def _add_coma_and_quote(s, opt):
        """
        separate args and add quote
        """
        types = [int, float]
        n = ""
        for a in s:
            t = type(a)
            if a == None:
                n += "null, "
            elif t in types or opt == 0:
                n += "%s, " % a
            else:
                n += "'%s', " % a

        return n

    def insert_document(self, name, fields, values):
        """
        Insert a single document with fields=values in collection <name>
        with <fields> and <values> being list
        todo: ID issue
        """
        id = uuid()
        fields.append(MongoSqlAlchemy.IDFIELD)
        values.append(str(id))
        d = {}
        for f, v in zip(fields, values):
            if f == "$set" or f == "$push" or f == "$inc":
                d.update(v)
            else:
                d.update({f: v})
        s_fields = self._add_coma_and_quote(d.keys(), 0)
        s_values = self._add_coma_and_quote(d.values(), 1)
        # print("insert into %s (%s) values (%s)" %(name, s_fields[:-2], s_values[:-2]))
        self.c.execute("insert into %s (%s) values (%s)" %
                       (name, s_fields[:-2], s_values[:-2]))
        return id

    def update_by_ids(self, name, fields, values, ids):
        """
        Update by ids with fields=values in table <name>
        with <fields> and <values> as tuples
        """
        s = []
        for f, v in zip(fields, values):
            t = type(v)
            if "$set" == f:
                self.update_by_ids(name, v.keys(), v.values(), ids)
            elif "$" in f:
                raise Exception, "JSON operation " + f + " currently not uspported"
            elif v is None:
                s.append("%s=null" % (f,))
            elif t != int and t != float:
                s.append("%s='%s'" % (
                f, str(v).replace("'", "\"")))  # < FIXME: SECURITY THIS IS NOT A GOOD ESCAPE AT ALL !!!! SQLI DANGER!
            else:
                s.append("%s=%s" % (f, v))
        if len(ids) != 0:
            if len(ids) == 1:
                ss = ("update %s set %s where " + MongoSqlAlchemy.IDFIELD + "=?") % (name, ", ".join(s))
                self.c.execute(ss, (ids[0],))
            else:
                ids = [(id,) for id in ids]
                # was: "... where _id=?"
                ss = ("update %s set %s where " + MongoSqlAlchemy.IDFIELD + "=?") % (name, ", ".join(s))
                # print(ss)
                # self.c.executemany(ss, ids)
                for cid in ids:
                    self.c.execute(ss, cid)

    def delete_documents(self, name, ids):
        """
        Delete all documents in the collection <name>
        with matching ids <ids>
        """
        ids = [(id,) for id in ids]
        print ids
        ss = (("delete from %s where " + MongoSqlAlchemy.IDFIELD + "=?") % name)
        for cid in ids:
            print (ss, (cid,))
            self.c.execute(ss, cid)

    def add_fields(self, name, fields):
        """
        Add fields <fields> to the collection <name>
        if fields <fields> does not already exist
        """
        self.c.execute("select * from %s limit 1" % name)
        infos = self.c.description
        for info in infos:
            if info[0].lower() in fields:
                del fields[info[0].lower()]
        if len(fields) > 0:
            for field, type in fields.items():
                self.c.execute("alter table %s add column %s %s" % (name, field, self.types.get(type, type)))

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
            key = key.replace(".", self.SEP)
            self.c.execute(s % (key, self.indexes.get(option, option), name, key, self.indexes.get(option, option)))
        except:
            pass

    def drop_index(self, name, index):
        """
        Drop the index <index>
        """
        self.c.execute("drop index %s" % index)

    def index_information(self, name):
        """
        Display informations on indexes in collection <name>
        """
        self.c.execute("show table %s" % name)
        l = filter(lambda s: "on field" in s, self.c.fetchall()[0][0].split('\n'))
        indexes = {}
        for index in l:
            sp = index.split()
            r = 1 if "ASC" in sp[2] else -1
            d = {'key': [(sp[-1], r)]}
            if sp[0] != "Secondary":
                d.update(unique=True)
            indexes.update({sp[2]: d})
        return indexes

    def begin_transaction(self):
        """
        Begin a transaction
        """
        try:
            self.c.execute("start transaction")
        except:
            self.commit_transaction()
            self.begin_transaction()

    def commit_transaction(self):
        """
        Commit the transaction
        """
        self.c.execute("commit")

    def logout(self):
        """
        Logout
        """
        pass
