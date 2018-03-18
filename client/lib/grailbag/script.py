#!/usr/bin/python

import re
import hashlib
import os
import shutil
import stat
import errno
from artifact import Artifact
from exceptions import *

__all__ = [
    "Script",
]

#-----------------------------------------------------------------------------
# string helper functions {{{

_QUOTE = {
    chr(c): ("\\x%02x" % (c,))
    for c in range(0, ord(' ')) + range(0x80, 0xff + 1)
}
_QUOTE[" "]  =  " "
_QUOTE["\n"] = "\\n"
_QUOTE["\t"] = "\\t"
_QUOTE["\r"] = "\\r"
_QUOTE["\""] = "\\\""
_QUOTE["\\"] = "\\\\"

def _quote(string):
    if string == "":
        return '""'

    if len(set(string) & _QUOTE.viewkeys()) == 0:
        return string

    result = bytearray("\"")
    for char in string:
        result.extend(_QUOTE.get(char, char))
    result.extend("\"")
    return str(result)

# NOTE: compare the regexp with `grailbag:valid/2' in server's code
_FIELD_RE = re.compile(r'[$%]\{[a-zA-Z0-9_][a-zA-Z0-9_.@%+-]*\}')
def _format(string, artifact_or_tags, tags, env):
    fields = {}
    if isinstance(artifact_or_tags, Artifact):
        fields.update(artifact_or_tags.tags)
    elif isinstance(artifact_or_tags, dict):
        fields.update(artifact_or_tags)
    elif artifact_or_tags is None:
        pass
    fields.update(tags)

    def replace(match):
        key = match.group(0)
        if key[0] == "$":
            if key[2:-1] not in env:
                # TODO: different error type
                raise ValueError("%s variable not defined" % (key,))
            return env[key[2:-1]]
        return fields[key[2:-1]]
    try:
        return _FIELD_RE.sub(replace, string)
    except KeyError:
        return None

_ENV_NAME_CHARS = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-")
def _valid_env_name(name):
    return (len(set(name) - _ENV_NAME_CHARS) == 0)

# }}}
#-----------------------------------------------------------------------------
# execution plan {{{

class _Plan:
    def __init__(self):
        self._paths = set()
        self._plan = []
        self._has_errors = False

    def add_path(self, path):
        if path in self._paths:
            pass # TODO: raise an exception, except for duplicate mkdirs

        while path != "" and path not in self._paths:
            self._paths.add(path)
            path = os.path.dirname(path)

    def __contains__(self, path):
        return (path in self._paths)

    def entry(self, action = None, log = None, error = None):
        def format_msg(s):
            if not isinstance(s, (list, tuple)):
                return s
            if len(s) == 0:
                return s[0]
            return s[0] % tuple(s[1:])

        if error is not None:
            self._has_errors = True

        self._plan.append((action, format_msg(log), format_msg(error)))

    def has_errors(self):
        return self._has_errors

    def errors(self):
        for (action, log, error) in self._plan:
            yield (error, log)

    def actions(self):
        if self._has_errors:
            # TODO: change the error type
            raise Exception("the script has errors")

        for (action, log, error) in self._plan:
            yield (action[0], action[1:], log)

# }}}
#-----------------------------------------------------------------------------
# filesystem helper functions {{{

def _file_type(path):
    try:
        result = os.lstat(path)
    except OSError, e:
        if e.errno == errno.ENOENT:
            return None
        raise
    return stat.S_IFMT(result.st_mode)

def _digest(path):
    # TODO: react to errno.EPERM
    # TODO: react to errno.ENOTDIR
    ftype = _file_type(path)

    if ftype is None:
        return ""
    elif ftype != stat.S_IFREG:
        return None

    # TODO: react to errno.EPERM
    with open(path) as f:
        ctx = hashlib.sha512()
        chunk = f.read(16 * 1024)
        while chunk != "":
            ctx.update(chunk)
            chunk = f.read(16 * 1024)
        return ctx.digest()

def _delete(path):
    ftype = _file_type(path)
    if ftype == stat.S_IFDIR:
        shutil.rmtree(path)
    elif ftype is not None:
        os.unlink(path)

def _make_path(path):
    if path == "":
        raise ValueError("can't create a directory with an empty name")
    try:
        os.mkdir(path)
    except OSError, e:
        if e.errno == errno.ENOENT:
            _make_path(os.path.dirname(path))
            os.mkdir(path)
            return
        if e.errno == errno.EEXIST and os.path.isdir(path):
            return
        raise

def _make_link(target, link):
    os.symlink(target, link)

def _execute(command, args):
    print "TODO: implement running commands"
    pass # TODO: implement me

# }}}
#-----------------------------------------------------------------------------
# operations {{{

class _OpWatch:
    def __init__(self, wildcard):
        self.wildcard = wildcard

    def __str__(self):
        return "wildcard %s" % (_quote(self.wildcard),)

class _OpIgnore:
    def __init__(self, wildcard):
        self.wildcard = wildcard

    def __str__(self):
        return "ignore %s" % (_quote(self.wildcard),)

class _OpGetFile:
    def __init__(self, path, artifact_id):
        self.path = path
        self.id = artifact_id

    def __str__(self):
        return "file %s %s" % (
            _quote(self.path),
            _quote(self.id),
        )

    @staticmethod
    def get(server, path, artifact_id):
        fd = os.open(path, os.O_WRONLY | os.O_CREAT, 0666)
        with os.fdopen(fd, "w") as output:
            with server.get(artifact_id) as download:
                for chunk in download.chunks(16 * 1024):
                    output.write(chunk)
            output.truncate()

    def plan(self, server, rootdir, plan):
        filename = os.path.join(rootdir, self.path)
        destdir = os.path.dirname(filename)

        if destdir not in plan and not os.path.isdir(destdir):
            plan.entry(
                action = [_make_path, os.path.dirname(filename)],
                log = ["make_dir %s", _quote(os.path.dirname(self.path))],
            )

        file_hash = _digest(filename)
        # TODO: plan error on `UnknownArtifact' or `PermissionError'
        info = server.info(self.id)
        if file_hash is None:
            plan.entry(
                action = [_delete, filename],
                log = ["delete %s", _quote(self.path)],
            )

        plan.add_path(self.path)
        if file_hash != info.digest:
            plan.entry(
                action = [_OpGetFile.get, server, filename, self.id],
                log = ["get %s %s", _quote(self.path), _quote(self.id)],
            )

class _OpMakeDirectory:
    def __init__(self, path):
        self.path = path

    def __str__(self):
        return "dir %s" % (_quote(self.path),)

    def plan(self, server, rootdir, plan):
        dirname = os.path.join(rootdir, self.path)

        plan.add_path(self.path)
        if dirname not in plan and not os.path.isdir(dirname):
            plan.entry(
                action = [_make_path, dirname],
                log = ["make_dir %s", _quote(self.path)],
            )

class _OpSymlink:
    def __init__(self, link, target):
        self.link = link
        self.target = target

    def __str__(self):
        return "symlink %s %s" % (
            _quote(self.link),
            _quote(self.target),
        )

    def plan(self, server, rootdir, plan):
        link_path = os.path.join(rootdir, self.link)
        link_dir = os.path.dirname(link_path)
        target_path = os.path.join(rootdir, self.target)
        target_relative_path = os.path.relpath(target_path, link_dir)

        if not os.path.exists(target_path) and self.target not in plan:
            plan.entry(
                error = "can't find the symlink target",
                log = ["symlink %s -> %s (%s)", _quote(self.link),
                       _quote(self.target), _quote(target_relative_path)],
            )
            return

        if link_dir not in plan and not os.path.isdir(link_dir):
            plan.entry(
                action = [_make_path, link_dir],
                log = ["make_dir %s", _quote(os.path.dirname(self.link))],
            )

        link_ftype = _file_type(link_path)
        if link_ftype == stat.S_IFLNK:
            link_current_target = os.readlink(link_path)
        else:
            link_current_target = None
        if link_ftype is not None and \
           link_current_target != target_relative_path:
            # either not a link or link to wrong file
            plan.entry(
                action = [_delete, link_path],
                log = ["delete %s", _quote(self.link)],
            )

        plan.add_path(self.link)
        if link_current_target != target_relative_path:
            plan.entry(
                action = [_make_link, target_relative_path, link_path],
                log = ["symlink %s -> %s (%s)", _quote(self.link),
                       _quote(self.target), _quote(target_relative_path)],
            )

class _OpSetEnv:
    def __init__(self, name, value):
        self.name = name
        self.value = value

    def __str__(self):
        return "env %s %s" % (
            _quote(self.name),
            _quote(self.value),
        )

    def plan(self, server, rootdir, plan):
        pass

class _OpRunCommand:
    def __init__(self, command):
        self.command = command

    def __str__(self):
        return "run %s" % (" ".join(_quote(c) for c in self.command),)

    def plan(self, server, rootdir, plan):
        plan.entry(
            action = [_execute, self.command[0], self.command[1:]],
            log = ["run %s", " ".join(_quote(c) for c in self.command)],
        )

class _OpRunScript:
    def __init__(self, script, env):
        self.script = script
        self.env = env.copy()

    def __str__(self):
        # TODO: don't indent empty lines
        lines = self.script.strip("\n").split("\n")
        if lines[0].startswith(" "):
            return "script indent=2\n  %s\nend" % ("\n  ".join(lines),)
        else:
            return "script\n  %s\nend" % ("\n  ".join(lines),)

    def plan(self, server, rootdir, plan):
        plan.entry(
            action = [_execute, "/bin/sh", ["-c", self.script]],
            log = "script (TODO: dump the env vars and the script)",
        )

# }}}
#-----------------------------------------------------------------------------

class Script:
    def __init__(self, script = None):
        self._watch = []
        self._ops = []
        self._env = {}
        self._script = script
        if self._script is not None:
            self._parse()

    #-------------------------------------------------------
    # script builder {{{

    def watch(self, wildcard):
        self._watch.append(_OpWatch(_format(wildcard, None, {}, self._env)))
        return True

    def ignore(self, wildcard):
        self._watch.append(_OpIgnore(_format(wildcard, None, {}, self._env)))
        return True

    def file(self, format, artifact, **tags):
        path = _format(format, artifact, tags, self._env)
        if path is None:
            return False
        self._ops.append(_OpGetFile(path, artifact.id))
        return True

    def directory(self, format, artifact_or_tags = None, **tags):
        path = _format(format, artifact_or_tags, tags, self._env)
        if path is None:
            return False
        self._ops.append(_OpMakeDirectory(path))
        return True

    def symlink(self, link, target, artifact_or_tags = None, **tags):
        link_path = _format(link, artifact_or_tags, tags, self._env)
        if link_path is None:
            return False
        target_path = _format(target, artifact_or_tags, tags, self._env)
        if target_path is None:
            return False
        self._ops.append(_OpSymlink(link_path, target_path))
        return True

    def env(self, name, format, artifact_or_tags = None, **tags):
        if not _valid_env_name(name):
            raise ValueError("invalid environment variable name: %s" % (name,))

        if format is None:
            if name in self._env:
                del self._env[name]
            return True

        value = _format(format, artifact_or_tags, tags, self._env)
        if value is None:
            return False
        self._env[name] = value
        self._ops.append(_OpSetEnv(name, value))
        return True

    def command(self, *command, **tags):
        artifact_or_tags = None
        command = list(command)
        for i in xrange(len(command)):
            if isinstance(command[i], (dict, Artifact)):
                artifact_or_tags = command[i]
                del command[i]
                break
        cmd = []
        for c in command:
            val = _format(c, artifact_or_tags, tags, self._env)
            if val is None:
                return False
            cmd.append(val)
        self._ops.append(_OpRunCommand(cmd))
        return True

    def script(self, script):
        self._ops.append(_OpRunScript(script, self._env))
        return True

    # }}}
    #-------------------------------------------------------
    # string representation {{{

    def __str__(self):
        watch = "\n".join(str(p) for p in self._watch)
        ops = "\n".join(str(o) for o in self._ops)
        return "\n".join([watch, ops])

    def _parse(self):
        pass # TODO

    # }}}
    #-------------------------------------------------------
    # script execution {{{

    def run(self, server, rootdir, dry_run, handle):
        plan = _Plan()

        for op in self._ops:
            op.plan(server, rootdir, plan)

        if plan.has_errors():
            # FIXME: don't print to STDERR, collect the logs as an array and
            # raise an exception with those
            import sys
            for (error, log) in plan.errors():
                if log is not None:
                    sys.stderr.write(log)
                    sys.stderr.write("\n")
                if error is not None:
                    sys.stderr.write(error)
                    sys.stderr.write("\n")
                sys.stderr.flush()
            return

        for (fun, args, log) in plan.actions():
            if handle is not None and log is not None:
                handle.write(log)
                handle.write("\n")
                handle.flush()
            if not dry_run:
                fun(*args)

        # TODO: clean up the rootdir, according to self._watch

    # }}}
    #-------------------------------------------------------

#-----------------------------------------------------------------------------
# vim:ft=python:foldmethod=marker
