#!/usr/bin/env python


class MongoMatch(object):
    @staticmethod
    def match_object(rules):
        """
        Convert a dict of rules to its matching string
        """
        if rules == {}:
            return ""
        r = " and ".join(MongoMatch._convert_op(rules))
        return r

    @staticmethod
    def _convert_op(rules):
        """
        Convert operators, and return them in a list
        """
        import sys
        sys.stderr.write("rules:"+repr(rules)+"\n")
        res = []
        for k, v in rules.items():
            if k[0] == "$":
                res.append(MongoMatch._operators[k](v))
            elif type(v) == dict:
                res.append(MongoMatch._operators[v.keys()[0]](k, v.values()[0]))
            #if type(v) in [list, tuple] :
            #        res.append(MongoMatch._operators[v.keys()[0]](v))
            elif type(v) == str:
                res.append("(%s like '%s')" % (k, v))
            else:
                res.append("(%s = %s)" % (k, v))
        return res

    @staticmethod
    def _logical_op(rules, s):
        """
        Convert operators, and concat them with a logical operator
        """
        res = []
        for rule in rules:
            res += MongoMatch._convert_op(rule)
        return s.join(res)

    _operators = {
        '$and': lambda rules: "(%s)" %
        MongoMatch._logical_op(rules, " and "),

        '$not': lambda rules: "(not (%s))" %
        MongoMatch._logical_op(rules, " and "),

        '$or': lambda rules: "(%s)" %
        MongoMatch._logical_op(rules, " or "),

        '$nor': lambda rules: "(not (%s))" %
        MongoMatch._logical_op(rules, " or "),

        '$lt': lambda x, y: "(%s < %s)" % (x, y),
        '$lte': lambda x, y:  "(%s <= %s)" % (x, y),
        '$gt': lambda x, y: "(%s > %s)" % (x, y),
        '$gte': lambda x, y: "(%s >= %s)" % (x, y),
        '$ne': lambda x, y: "(%s != %s)" % (x, y) if type(y) != str else "(%s != '%s')" % (x, y),
        '$in': lambda x, y: "(%s in %s)" % (x, y),
        '$nin': lambda x, y: "(%s not in %s)" % (x, y),
        '$regex': lambda x, y: "(%s like %s)" % (x, y),
        '$mod': lambda x, y: "(MOD(%s, %s) == %s)" % (x, y[0], y[1]),
    }
