#!/usr/bin/env python


class MongoError(Exception):
    """
    MongoSyntax and Collection errors
    """
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)
