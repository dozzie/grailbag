#!/usr/bin/python

#-----------------------------------------------------------------------------

class GrailBagException(Exception):
    pass

class AuthenticationError(GrailBagException):
    def __str__(self):
        return "authentication error: invalid user or password"

class PermissionError(GrailBagException):
    def __str__(self):
        return "authorization error: operation not permitted"

class NetworkError(GrailBagException):
    def __init__(self, exception):
        super(NetworkError, self).__init__(*exception.args)
        self.errno = exception.errno
        self.strerror = exception.strerror
        self.filename = exception.filename

    def __str__(self):
        if self.filename is not None:
            return "[Errno %s] %s: '%s'" % \
                   (self.errno, self.strerror, self.filename)
        else:
            return "[Errno %s] %s" % (self.errno, self.strerror)

class ProtocolError(NetworkError):
    def __init__(self, message, *args):
        self.args = (message,) + args
        self.message = message % args
        self.errno = errno.EBADMSG
        self.strerror = os.strerror(self.errno)
        self.filename = None

    def __str__(self):
        return self.message

class OperationError(GrailBagException):
    pass

class ServerError(OperationError):
    def __init__(self, event_id):
        self.event_id = str(event_id)
        self.args = (str(event_id),)
        self.message = ""

    def __str__(self):
        return "server error (event ID: %s), check logs for details" % \
               (self.event_id,)

class UnknownArtifact(OperationError):
    def __init__(self, artifact_id):
        self.artifact = str(artifact_id)
        self.args = (str(artifact_id),)
        self.message = ""

    def __str__(self):
        return "artifact %s does not exist" % (self.artifact,)

class UnknownType(OperationError):
    def __init__(self, artifact_type):
        self.type = artifact_type
        self.args = (artifact_type,)
        self.message = ""

    def __str__(self):
        return "artifact type %s does not exist" % (self.type,)

class UploadChecksumMismatch(OperationError):
    def __init__(self, artifact_id):
        self.artifact = str(artifact_id)
        self.args = (str(artifact_id),)
        self.message = ""

    def __str__(self):
        return "uploading artifact %s failed: bad checksum" % (self.artifact,)

class ArtifactHasTokens(OperationError):
    def __init__(self, artifact_id):
        self.artifact = str(artifact_id)
        self.args = (str(artifact_id),)
        self.message = ""

    def __str__(self):
        return "deleting artifact %s failed: artifact still has tokens" % \
               (self.artifact,)

class SchemaError(OperationError):
    def __init__(self, duplicate_tags = None, missing_tags = None,
                 unknown_tokens = None, bad_tags = None, bad_tokens = None,
                 bad_artifact_type = None):
        self.dups = duplicate_tags
        self.missing = missing_tags
        self.unknown = unknown_tokens
        self.bad_tags = bad_tags
        self.bad_tokens = bad_tokens
        self.bad_artifact_type = bad_artifact_type

    def __str__(self):
        message = "schema validation error:"
        if self.dups is not None:
            message += "\n  unique tags collisions: " + ", ".join(self.dups)
        if self.missing is not None:
            message += "\n  missing mandatory tags: " + ", ".join(self.missing)
        if self.unknown is not None:
            message += "\n  unrecognized tokens: " + ", ".join(self.unknown)
        if self.bad_tags is not None:
            message += "\n  invalid tag names: " + ", ".join(self.bad_tags)
        if self.bad_tokens is not None:
            message += "\n  invalid token names: " + ", ".join(self.bad_tokens)
        if self.bad_artifact_type is not None:
            message += "\n  invalid artifact type: " + self.bad_artifact_type
        return message

#-----------------------------------------------------------------------------
# vim:ft=python:foldmethod=marker
