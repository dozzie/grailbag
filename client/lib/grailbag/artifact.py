#!/usr/bin/python

#-----------------------------------------------------------------------------

class Artifact:
    def __init__(self):
        self.id = None
        self.type = None
        self.size = None
        self.digest = None
        self.ctime = None
        self.mtime = None
        self.tags = {}
        self.tokens = []
        self.valid = False # conforming to artifact's schema

#-----------------------------------------------------------------------------
# vim:ft=python:foldmethod=marker
