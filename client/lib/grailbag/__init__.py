#!/usr/bin/python

import socket
import ssl
import platform
import errno
import os
import uuid
import hashlib

from artifact import Artifact

__all__ = [
    "GrailBagException",
    "AuthenticationError", "PermissionError",
    "NetworkError", "ProtocolError",
    "OperationError",
    "ServerError",
    "UnknownArtifact", "UnknownType", "UploadChecksumMismatch",
    "ArtifactHasTokens", "SchemaError",
    "Artifact",
    "Server",
]

#-----------------------------------------------------------------------------
# exceptions {{{

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

# }}}
#-----------------------------------------------------------------------------
# helper classes {{{

class _Constant:
    def __init__(self, name):
        self.name = name

    def __str__(self):
        return "<%s>" % (self.name,)

    def __repr__(self):
        return str(self)

class _ReqBuffer:
    def __init__(self, content = ""):
        self.buf = bytearray(content)

    def add(self, s):
        self.buf.extend(s)

    def add_int16(self, i):
        self.buf.extend("\x00\x00")
        self.buf[-2] = (i >> 8) & 0xff
        self.buf[-1] = (i     ) & 0xff

    def add_int32(self, i):
        self.buf.extend("\x00\x00\x00\x00")
        self.buf[-4] = (i >> 24) & 0xff
        self.buf[-3] = (i >> 16) & 0xff
        self.buf[-2] = (i >>  8) & 0xff
        self.buf[-1] = (i      ) & 0xff

    def add_str16(self, s):
        self.buf.extend("\x00\x00" + s)
        self.buf[-(len(s) + 2)] = (len(s) >> 8) & 0xff
        self.buf[-(len(s) + 1)] = (len(s)     ) & 0xff

    def add_str32(self, s):
        self.buf.extend("\x00\x00\x00\x00" + s)
        self.buf[-(len(s) + 4)] = (len(s) >> 24) & 0xff
        self.buf[-(len(s) + 3)] = (len(s) >> 16) & 0xff
        self.buf[-(len(s) + 2)] = (len(s) >>  8) & 0xff
        self.buf[-(len(s) + 1)] = (len(s)      ) & 0xff

    def add_pair(self, name, value):
        self.buf.extend("\x00\x00\x00\x00\x00\x00")
        self.buf[-6] = (len(name) >> 8) & 0xff
        self.buf[-5] = (len(name)     ) & 0xff
        self.buf[-4] = (len(value) >> 24) & 0xff
        self.buf[-3] = (len(value) >> 16) & 0xff
        self.buf[-2] = (len(value) >>  8) & 0xff
        self.buf[-1] = (len(value)      ) & 0xff
        self.buf.extend(name)
        self.buf.extend(value)

    def bytes(self):
        return bytes(self.buf)

    def __str__(self):
        return str(self.buf)

    def __len__(self):
        return len(self.buf)

_TAG_OK = _Constant("OK")
_TAG_OK_CHUNKSIZE = _Constant("OK+CHUNKSIZE")
_TAG_INFO = _Constant("INFO")
_TAG_LIST = _Constant("LIST")
_TAG_WHOAMI = _Constant("WHOAMI")
_TAG_TYPES = _Constant("TYPES")
_TAG_ERROR = _Constant("ERROR")
_TAG_SERVER_ERROR = _Constant("SERVER_ERROR")
_TAG_AUTH_ERROR = _Constant("AUTH_ERROR")
_TAG_PERMISSION_ERROR = _Constant("PERMISSION_ERROR")

_ERR_CODE_NXID = _Constant("UNKNOWN_ARTIFACT")
_ERR_CODE_NXTYPE = _Constant("UNKNOWN_ARTIFACT_TYPE")
_ERR_CODE_CHECKSUM = _Constant("BODY_CHECKSUM_MISMATCH")
_ERR_CODE_HAS_TOKENS = _Constant("ARTIFACT_HAS_TOKENS")
_ERR_CODE_SCHEMA = _Constant("SCHEMA_ERROR")

class _RespBuffer:
    def __init__(self, size):
        self.buf = bytearray(size)
        self.write_pos = 0
        self.pos = 0

    def full(self):
        return len(self.buf) <= self.write_pos

    def left(self):
        return len(self.buf) - self.write_pos

    def add(self, content):
        self.buf[self.write_pos : (self.write_pos + len(content))] = content
        self.write_pos += len(content)

    def start_reading(self):
        self.pos = 4
        tag = (self.buf[0] & 0xf0) >> 4
        size = (self.buf[0] & 0x0f) << 24 | self.buf[1] << 16 | \
               self.buf[2] << 8 | self.buf[3]

        if tag == 0 and size == 0:
            return (_TAG_OK, None)

        if tag == 0 and size > 0:
            return (_TAG_OK_CHUNKSIZE, size)

        if tag == 1:
            return (_TAG_INFO, None)

        if tag == 2:
            return (_TAG_LIST, size)

        if tag == 3:
            return (_TAG_TYPES, size)

        if tag == 4:
            return (_TAG_WHOAMI, size)

        if tag == 13:
            code = size >> 16
            if code == 0:
                return (_TAG_AUTH_ERROR, None)
            elif code == 1:
                return (_TAG_PERMISSION_ERROR, None)
            raise ProtocolError("unrecognized request error code: %d", code)

        if tag == 14: # server error
            event_id = self.uuid()
            raise ServerError(event_id)

        if tag == 15: # request error
            code = size >> 16
            if code == 0:
                return (_TAG_ERROR, _ERR_CODE_NXID)
            if code == 1:
                return (_TAG_ERROR, _ERR_CODE_NXTYPE)
            if code == 2:
                return (_TAG_ERROR, _ERR_CODE_CHECKSUM)
            if code == 3:
                return (_TAG_ERROR, _ERR_CODE_HAS_TOKENS)
            if code == 4:
                # number of errors is encoded at byte boundary, so let's just
                # go back a little
                self.pos = 2
                return (_TAG_ERROR, _ERR_CODE_SCHEMA)
            raise ProtocolError("unrecognized request error code: %d", code)

        raise ProtocolError("unrecognized reply tag: %s", tag)

    def uuid(self):
        result = uuid.UUID(bytes = str(self.buf[self.pos : (self.pos + 16)]))
        self.pos += 16 # 128 bits
        return result

    def int8(self):
        result = self.buf[self.pos]
        self.pos += 1
        return result

    def int16(self):
        result = self.buf[self.pos    ] << 8 | \
                 self.buf[self.pos + 1]
        self.pos += 2
        return result

    def int32(self):
        result = self.buf[self.pos    ] << 24 | \
                 self.buf[self.pos + 1] << 16 | \
                 self.buf[self.pos + 2] <<  8 | \
                 self.buf[self.pos + 3]
        self.pos += 4
        return result

    def int64(self):
        result = self.buf[self.pos    ] << 56 | \
                 self.buf[self.pos + 1] << 48 | \
                 self.buf[self.pos + 2] << 40 | \
                 self.buf[self.pos + 3] << 32 | \
                 self.buf[self.pos + 4] << 24 | \
                 self.buf[self.pos + 5] << 16 | \
                 self.buf[self.pos + 6] <<  8 | \
                 self.buf[self.pos + 7]
        self.pos += 8
        return result

    def str16(self):
        return self.str(self.int16())

    def str32(self):
        return self.str(self.int32())

    def str(self, length):
        result = self.buf[self.pos : (self.pos + length)]
        self.pos += len(result)
        return str(result)

    def empty(self):
        return self.pos >= len(self.buf)

# }}}
#-----------------------------------------------------------------------------

class Server:

    #------------------------------------------------------
    # Server.Download (context manager) {{{

    class Download:
        def __init__(self, info, conn, size, parent):
            self.info = info
            self._conn = conn
            self._left = size
            self._parent = parent
            self._parent.op_in_progress = True

        def __del__(self):
            if self._left > 0 and self._parent is not None:
                # abort the connection
                self._parent._close()
            self._parent = None

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc_val, exc_tb):
            if self._parent is not None:
                self._parent.op_in_progress = False
            if self._left > 0 and self._parent is not None:
                # abort the connection
                self._parent._close()
            self._parent = None

        def finished(self):
            return self._left == 0

        def read(self, size):
            data = self._conn.recv(min(size, self._left))
            self._left -= len(data)
            return data

        def chunks(self, size):
            while self._left > 0:
                data = self._conn.recv(min(size, self._left))
                if data == "":
                    self._parent._close()
                    raise ProtocolError("connection closed unexpectedly")
                self._left -= len(data)
                yield data

    # }}}
    #------------------------------------------------------
    # Server.Upload (context manager) {{{

    class Upload:
        def __init__(self, chunk_size, id, conn, parent):
            self.chunk_size = chunk_size
            self.id = id
            self._conn = conn
            self._parent = parent
            self._parent.op_in_progress = True
            self._ctx = hashlib.sha512()
            self._sizebuf = bytearray(4)

        def __del__(self):
            if self._ctx is not None and self._parent is not None:
                # abort the connection
                self._parent._close()
            self._parent = None
            self._conn = None

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc_val, exc_tb):
            if self._parent is not None:
                self._parent.op_in_progress = False
            if self._ctx is not None and self._parent is not None:
                # abort the connection
                self._parent._close()
            self._parent = None
            self._conn = None

        def write(self, data):
            self._sizebuf[0] = (len(data) >> 24) & 0xff
            self._sizebuf[1] = (len(data) >> 16) & 0xff
            self._sizebuf[2] = (len(data) >>  8) & 0xff
            self._sizebuf[3] = (len(data)      ) & 0xff
            self._conn.send(self._sizebuf)
            self._conn.send(data)
            self._ctx.update(data)

        def done(self):
            self._conn.send("\x00\x00\x00\x00") # send EOF marker
            digest = self._ctx.digest()
            self._ctx = None
            self._sizebuf[0] = (len(digest) >> 24) & 0xff
            self._sizebuf[1] = (len(digest) >> 16) & 0xff
            self._sizebuf[2] = (len(digest) >>  8) & 0xff
            self._sizebuf[3] = (len(digest)      ) & 0xff
            self._conn.send(self._sizebuf)
            self._conn.send(digest)

            reply = self._parent._recv()
            (tag, info) = reply.start_reading()

            if tag is _TAG_OK:
                artifact_id = reply.uuid()

                if not reply.empty():
                    raise ProtocolError("unexpected message payload")
                return

            if tag is _TAG_ERROR and info is _ERR_CODE_CHECKSUM:
                raise UploadChecksumMismatch(self.id)

            raise ProtocolError("unexpected reply (tag %s)", tag)

    # }}}
    #------------------------------------------------------

    def __init__(self, host, port = 3255, user = None, password = None,
                 ca_file = None, timeout = 5):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.timeout = timeout
        self.conn = None
        self.ca_file = ca_file
        self.op_in_progress = False

    #------------------------------------------------------
    # _connect(), _close() {{{

    def _connect(self):
        conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        conn.settimeout(self.timeout)
        if self.ca_file is not None:
            conn = ssl.wrap_socket(conn, ca_certs = self.ca_file,
                                   cert_reqs = ssl.CERT_REQUIRED)
        else:
            conn = ssl.wrap_socket(conn, cert_reqs = ssl.CERT_NONE)
        try:
            conn.connect((self.host, self.port))
        except Exception as e:
            conn.close()
            raise NetworkError(e)

        conn.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        if platform.system() == "Linux":
            # XXX: unportable, Linux-specific code
            # send first probe after 30s
            conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, 30)
            # keep sending probes every 30s
            conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, 30)
            # after this many probes the connection drops
            conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, 9)

        self.conn = conn

        # authenticate
        request = _ReqBuffer("l")
        request.add_str16(self.user)
        request.add_str16(self.password)

        reply = self._send(request)
        (tag, info) = reply.start_reading()
        if tag is _TAG_OK:
            return

        if tag is _TAG_AUTH_ERROR:
            raise AuthenticationError()

        raise ProtocolError("unexpected reply (tag %s)", tag)

    def _close(self):
        if self.conn is not None:
            self.conn.close()
        self.conn = None

    # }}}
    #------------------------------------------------------
    # _send(), _recv() {{{

    def _send(self, buf):
        if self.conn is None:
            self._connect()

        sizebuf = bytearray(4)
        sizebuf[0] = (len(buf) >> 24) & 0xff
        sizebuf[1] = (len(buf) >> 16) & 0xff
        sizebuf[2] = (len(buf) >>  8) & 0xff
        sizebuf[3] =  len(buf)        & 0xff
        try:
            self.conn.send(sizebuf)
            self.conn.send(str(buf))
        except Exception as e:
            self._close()
            raise NetworkError(e)
        return self._recv()

    def _recv(self):
        sizebuf = _RespBuffer(4)
        while not sizebuf.full():
            buf = self.conn.recv(sizebuf.left())
            if buf == "":
                self._close()
                raise ProtocolError("connection closed unexpectedly")
            sizebuf.add(buf)

        data = _RespBuffer(sizebuf.int32())
        while not data.full():
            buf = self.conn.recv(data.left())
            if buf == "":
                self._close()
                raise ProtocolError("connection closed unexpectedly")
            data.add(buf)

        return data

    # }}}
    #------------------------------------------------------
    # Server._decode_schema_errors() {{{

    @staticmethod
    def _decode_schema_errors(reply):
        dups = []
        missing = []
        unknown = []
        bad_tags = []
        bad_tokens = []
        bad_type = [] # this type of error should only be reported once
        errors = {
            1: dups,
            2: missing,
            3: unknown,
            4: bad_tags,
            5: bad_tokens,
            6: bad_type,
        }

        nerrors = reply.int16()
        for i in xrange(nerrors):
            err_type = reply.int8()
            name = reply.str16()
            if err_type in errors:
                errors[err_type].append(name)
            else:
                raise ProtocolError("unrecognized schema error tag: %d", err_type)

        if len(dups) == 0:
            dups = None
        if len(missing) == 0:
            missing = None
        if len(unknown) == 0:
            unknown = None
        if len(bad_tags) == 0:
            bad_tags = None
        if len(bad_tokens) == 0:
            bad_tokens = None
        if len(bad_type) > 0:
            # only one of this kind should be reported
            bad_type = bad_type[0]
        else:
            bad_type = None

        return SchemaError(
            duplicate_tags    = dups,
            missing_tags      = missing,
            unknown_tokens    = unknown,
            bad_tags          = bad_tags,
            bad_tokens        = bad_tokens,
            bad_artifact_type = bad_type,
        )

    # }}}
    #------------------------------------------------------

    def whoami(self):
        if self.op_in_progress:
            raise Exception("another operation in progress")

        request = _ReqBuffer("w")

        reply = self._send(request)
        (tag, info) = reply.start_reading()

        if tag is _TAG_WHOAMI:
            nperms = info
            user = reply.str16()
            if user == "":
                return (None, None)
            perms_map = {}
            for i in xrange(nperms):
                perms = reply.int8()
                name = reply.str16()
                perms_map[name] = [
                    perm_name
                    for (perm_name, num) in [("create", 0x10), ("read", 0x08),
                                             ("update", 0x04), ("delete", 0x02),
                                             ("tokens", 0x01)]
                    if (perms & num) != 0
                ]
            if not reply.empty():
                raise ProtocolError("unexpected message payload")
            return (user, perms_map)

        raise ProtocolError("unexpected reply (tag %s)", tag)

    def store(self, artifact_type, tags):
        if self.op_in_progress:
            raise Exception("another operation in progress")

        request = _ReqBuffer("S")
        request.add_str16(artifact_type)
        request.add_int32(len(tags))
        for (name, value) in tags.iteritems():
            request.add_pair(name, value)

        reply = self._send(request)
        (tag, info) = reply.start_reading()

        if tag is _TAG_OK_CHUNKSIZE:
            chunk_size = info
            artifact_id = reply.uuid()

            if not reply.empty():
                raise ProtocolError("unexpected message payload")
            return Server.Upload(chunk_size, str(artifact_id), self.conn, self)

        if tag is _TAG_ERROR and info is _ERR_CODE_NXTYPE:
            raise UnknownType(artifact_type)

        if tag is _TAG_ERROR and info is _ERR_CODE_SCHEMA:
            error = Server._decode_schema_errors(reply)
            if not reply.empty():
                raise ProtocolError("unexpected message payload")
            raise error

        if tag is _TAG_PERMISSION_ERROR:
            raise PermissionError()

        raise ProtocolError("unexpected reply (tag %s)", tag)

    def delete(self, artifact_id):
        if self.op_in_progress:
            raise Exception("another operation in progress")
        artifact_id = uuid.UUID(artifact_id)

        request = _ReqBuffer("D")
        request.add(artifact_id.bytes)

        reply = self._send(request)
        (tag, info) = reply.start_reading()

        if tag is _TAG_OK:
            artifact_id = reply.uuid()

            if not reply.empty():
                raise ProtocolError("unexpected message payload")
            return True

        if tag is _TAG_ERROR and info is _ERR_CODE_NXID:
            return False

        if tag is _TAG_ERROR and info is _ERR_CODE_HAS_TOKENS:
            raise ArtifactHasTokens(artifact_id)

        if tag is _TAG_PERMISSION_ERROR:
            raise PermissionError()

        raise ProtocolError("unexpected reply (tag %s)", tag)

    def update_tags(self, artifact_id, set_tags, unset_tags):
        if self.op_in_progress:
            raise Exception("another operation in progress")
        artifact_id = uuid.UUID(artifact_id)

        request = _ReqBuffer("A")
        request.add(artifact_id.bytes)
        request.add_int32(len(set_tags))
        request.add_int32(len(unset_tags))
        for (name, value) in set_tags.iteritems():
            request.add_pair(name, value)
        for name in unset_tags:
            request.add_str16(name)

        reply = self._send(request)
        (tag, info) = reply.start_reading()

        if tag is _TAG_OK:
            artifact_id = reply.uuid()

            if not reply.empty():
                raise ProtocolError("unexpected message payload")
            return

        if tag is _TAG_ERROR and info is _ERR_CODE_NXID:
            raise UnknownArtifact(artifact_id)

        if tag is _TAG_ERROR and info is _ERR_CODE_SCHEMA:
            error = Server._decode_schema_errors(reply)
            if not reply.empty():
                raise ProtocolError("unexpected message payload")
            raise error

        if tag is _TAG_PERMISSION_ERROR:
            raise PermissionError()

        raise ProtocolError("unexpected reply (tag %s)", tag)

    def update_tokens(self, artifact_id, set_tokens, unset_tokens):
        if self.op_in_progress:
            raise Exception("another operation in progress")
        artifact_id = uuid.UUID(artifact_id)

        request = _ReqBuffer("O")
        request.add(artifact_id.bytes)
        request.add_int32(len(set_tokens))
        request.add_int32(len(unset_tokens))
        for name in set_tokens:
            request.add_str16(name)
        for name in unset_tokens:
            request.add_str16(name)

        reply = self._send(request)
        (tag, info) = reply.start_reading()

        if tag is _TAG_OK:
            artifact_id = reply.uuid()

            if not reply.empty():
                raise ProtocolError("unexpected message payload")
            return

        if tag is _TAG_ERROR and info is _ERR_CODE_NXID:
            raise UnknownArtifact(artifact_id)

        if tag is _TAG_ERROR and info is _ERR_CODE_SCHEMA:
            error = Server._decode_schema_errors(reply)
            if not reply.empty():
                raise ProtocolError("unexpected message payload")
            raise error

        if tag is _TAG_PERMISSION_ERROR:
            raise PermissionError()

        raise ProtocolError("unexpected reply (tag %s)", tag)

    def types(self):
        if self.op_in_progress:
            raise Exception("another operation in progress")

        request = _ReqBuffer("T")
        reply = self._send(request)
        (tag, info) = reply.start_reading()

        if tag is _TAG_TYPES:
            ntypes = info
            result = [reply.str16() for i in xrange(ntypes)]
            if not reply.empty():
                raise ProtocolError("unexpected message payload")
            return result

        raise ProtocolError("unexpected reply (tag %s)", tag)

    #------------------------------------------------------
    # Server._decode_artifact_info() {{{

    @staticmethod
    def _decode_artifact_info(reply):
        result = Artifact()
        result.id = str(reply.uuid())
        result.type = reply.str16()
        flags = reply.int16()
        result.valid = ((flags & 0x8000) == 0)
        result.size = reply.int64()
        result.digest = reply.str16()
        result.ctime = reply.int64()
        result.mtime = reply.int64()
        ntags = reply.int32()
        ntokens = reply.int32()
        for i in xrange(ntags):
            tsize = reply.int16()
            vsize = reply.int32()
            tag = reply.str(tsize)
            result.tags[tag] = reply.str(vsize)
        for i in xrange(ntokens):
            result.tokens.append(reply.str16())
        return result

    # }}}
    #------------------------------------------------------

    def artifacts(self, artifact_type):
        if self.op_in_progress:
            raise Exception("another operation in progress")

        request = _ReqBuffer("L")
        request.add_str16(artifact_type)

        reply = self._send(request)
        (tag, info) = reply.start_reading()

        if tag is _TAG_LIST:
            nartifacts = info
            result = [
                Server._decode_artifact_info(reply)
                for i in xrange(nartifacts)
            ]
            if not reply.empty():
                raise ProtocolError("unexpected message payload")
            return result

        if tag is _TAG_ERROR and info is _ERR_CODE_NXTYPE:
            raise UnknownType(artifact_type)

        if tag is _TAG_ERROR and info is _ERR_CODE_SCHEMA:
            error = Server._decode_schema_errors(reply)
            if not reply.empty():
                raise ProtocolError("unexpected message payload")
            raise error

        if tag is _TAG_PERMISSION_ERROR:
            raise PermissionError()

        raise ProtocolError("unexpected reply (tag %s)", tag)

    def info(self, artifact_id):
        if self.op_in_progress:
            raise Exception("another operation in progress")
        artifact_id = uuid.UUID(artifact_id)

        request = _ReqBuffer("I")
        request.add(artifact_id.bytes)

        reply = self._send(request)
        (tag, info) = reply.start_reading()

        if tag is _TAG_INFO:
            result = Server._decode_artifact_info(reply)
            if not reply.empty():
                raise ProtocolError("unexpected message payload")
            return result

        if tag is _TAG_ERROR and info is _ERR_CODE_NXID:
            raise UnknownArtifact(artifact_id)

        if tag is _TAG_PERMISSION_ERROR:
            raise PermissionError()

        raise ProtocolError("unexpected reply (tag %s)", tag)

    def get(self, artifact_id):
        # returns a context manager
        if self.op_in_progress:
            raise Exception("another operation in progress")
        artifact_id = uuid.UUID(artifact_id)

        request = _ReqBuffer("G")
        request.add(artifact_id.bytes)

        reply = self._send(request)
        (tag, info) = reply.start_reading()

        if tag is _TAG_INFO:
            info = Server._decode_artifact_info(reply)
            if not reply.empty():
                self._close()
                raise ProtocolError("unexpected message payload")
            return Server.Download(info, self.conn, info.size, self)

        if tag is _TAG_ERROR and info is _ERR_CODE_NXID:
            raise UnknownArtifact(artifact_id)

        if tag is _TAG_PERMISSION_ERROR:
            raise PermissionError()

        raise ProtocolError("unexpected reply (tag %s)", tag)

#-----------------------------------------------------------------------------
# vim:ft=python:foldmethod=marker
