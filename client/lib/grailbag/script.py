#!/usr/bin/python

import re
import hashlib
import subprocess
import os
import shutil
import stat
import errno
import fnmatch
import collections
from artifact import Artifact
from exceptions import GrailBagException, UnknownArtifact

__all__ = [
    "ScriptError", "ScriptBuildError", "ScriptParseError", "ScriptRunError",
    "Script",
]

#-----------------------------------------------------------------------------
# exceptions {{{

class ScriptError(Exception):
    def __init__(self, message, *args):
        super(ScriptError, self).__init__(message % args)

class ScriptBuildError(ScriptError, ValueError):
    pass

class ScriptParseError(ScriptError):
    def __init__(self, message, line = None, column = None, *args):
        super(ScriptParseError, self).__init__(message % args)
        self.line = line
        self.column = column

class ScriptRunError(ScriptError):
    def __init__(self, message, logs = None, *args):
        super(ScriptRunError, self).__init__(message % args)
        # if set, it's a list of tuples (log,error), with `log' and `error'
        # being a (possibly multiline) string or `None'
        self.logs = logs

# }}}
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
                raise ScriptBuildError("variable %s not defined", key)
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
        self._env = {}

    def add_path(self, path):
        if path in self._paths:
            pass # TODO: raise an exception, except for duplicate mkdirs

        while path != "" and path not in self._paths:
            self._paths.add(path)
            path = os.path.dirname(path)

    def __contains__(self, path):
        return (path in self._paths)

    def set_env(self, name, value):
        if value is not None:
            self._env[name] = value
        elif name in self._env:
            del self._env[name]

    def get_env(self):
        return self._env.copy()

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

    def errors(self):
        for (action, log, error) in self._plan:
            yield (error, log)

    def actions(self):
        if self._has_errors:
            logs = [
                (log, error)
                for (action, log, error) in self._plan
                if log is not None or error is not None
            ]
            raise ScriptRunError("the script plan has errors", logs = logs)

        for (action, log, error) in self._plan:
            yield (action[0], action[1:], log)

# }}}
#-----------------------------------------------------------------------------
# glob mapping {{{

class GlobMap:
    def __init__(self):
        self._rules = collections.deque()

    @staticmethod
    def _split(path):
        return [f for f in path.split("/") if f != "" and f != "."]

    def add(self, pattern, include = True):
        fragments = GlobMap._split(pattern)
        self._rules.appendleft((include, fragments))

    @staticmethod
    def _match(path, rule):
        i = 0
        while i < len(path) and i < len(rule):
            if not fnmatch.fnmatch(path[i], rule[i]):
                return None
            i += 1
        return (path[i:], rule[i:])

    def _file_match(self, path):
        path_frags = GlobMap._split(path)

        for (include, rule_frags) in self._rules:
            match = GlobMap._match(path_frags, rule_frags)
            if match is not None and len(match[1]) == 0:
                # rule pattern exhausted
                return include

        # default decision
        return False

    def _dir_match(self, path):
        path_frags = GlobMap._split(path)

        path_include = None
        subtree_decisions = set()
        for (include, rule_frags) in self._rules:
            match = GlobMap._match(path_frags, rule_frags)
            if match is None:
                continue
            if path_include is None and len(match[1]) == 0:
                # rule pattern exhausted for the first time
                path_include = include
            elif len(match[0]) == 0 and len(match[1]) > 0:
                # path exhausted, so it's a pattern from the subtree
                subtree_decisions.add(include)

        if path_include is None:
            # default decision
            path_include = False

        # the decision for the whole subtree is definitive ("skip/recurse
        # whole subtree") if the other decision is present somewhere in the
        # subtree
        definitive = ((not path_include) not in subtree_decisions)
        return (path_include, definitive)

    def dirtree(self, rootdir):
        paths = []
        subtrees = []
        def walk(subdir):
            for name in sorted(os.listdir(os.path.join(rootdir, subdir))):
                path = os.path.join(subdir, name)
                ftype = _file_type(os.path.join(rootdir, path))

                if ftype != stat.S_IFDIR:
                    if self._file_match(path):
                        paths.append(path)
                    continue

                # XXX: subdirectory
                (include, whole_subtree) = self._dir_match(path)
                if whole_subtree:
                    if include:
                        subtrees.append(path)
                    continue

                if include:
                    paths.append(path)

                walk(path)

        for path in sorted(os.listdir(rootdir)):
            ftype = _file_type(os.path.join(rootdir, path))

            if ftype != stat.S_IFDIR:
                if self._file_match(path):
                    paths.append(path)
                continue

            # XXX: subdirectory
            (include, whole_subtree) = self._dir_match(path)
            if whole_subtree:
                if include:
                    subtrees.append(path)
                continue

            if include:
                paths.append(path)

            walk(path)
        return (paths, subtrees)

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

def _execute(command, shell, env, rootdir):
    def set_env():
        for (key,value) in env.iteritems():
            os.environ[key] = value

    process = subprocess.Popen(
        command,
        stdin = subprocess.PIPE,
        close_fds = True,
        preexec_fn = set_env,
        cwd = rootdir,
        shell = shell,
    )
    process.communicate()
    if process.returncode > 0:
        raise ScriptRunError("command exited with code %d", process.returncode)
    if process.returncode < 0:
        raise ScriptRunError("command died on signal %d", -process.returncode)
    # process exited with code 0
    return

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

        # TODO: allow no-network plan (file hash checking will be skipped)
        try:
            info = server.info(self.id)
        except UnknownArtifact:
            plan.entry(
                error = "artifact not found",
                log = ["get %s %s", _quote(self.id), _quote(self.path)],
            )
            return
        except GrailBagException, e:
            plan.entry(
                error = ["GrailBag error: %s", str(e)],
                log = ["get %s %s", _quote(self.id), _quote(self.path)],
            )
            return

        file_hash = _digest(filename)
        if file_hash is None:
            plan.entry(
                action = [_delete, filename],
                log = ["delete %s", _quote(self.path)],
            )

        plan.add_path(self.path)
        if file_hash != info.digest:
            plan.entry(
                action = [_OpGetFile.get, server, filename, self.id],
                log = ["get %s %s", _quote(self.id), _quote(self.path)],
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
        if self.value is None:
            return "env %s" % (_quote(self.name),)
        return "env %s %s" % (
            _quote(self.name),
            _quote(self.value),
        )

    @staticmethod
    def noop():
        pass

    def plan(self, server, rootdir, plan):
        if self.value is not None:
            plan.entry(
                action = [_OpSetEnv.noop],
                log = ["%s=%s", _quote(self.name), _quote(self.value)],
            )
        else:
            plan.entry(
                action = [_OpSetEnv.noop],
                log = ["unset %s", _quote(self.name)],
            )
        plan.set_env(self.name, self.value)

class _OpRunCommand:
    def __init__(self, *command):
        self.command = list(command)

    def __str__(self):
        return "run %s" % (" ".join(_quote(c) for c in self.command),)

    def plan(self, server, rootdir, plan):
        # XXX: `self.command' is a list
        plan.entry(
            action = [_execute, self.command, False, plan.get_env(), rootdir],
            log = ["run %s", " ".join(_quote(c) for c in self.command)],
        )

class _OpRunScript:
    def __init__(self, script):
        self.script = script

    def __str__(self):
        # TODO: don't indent empty lines
        lines = self.script.strip("\n").split("\n")
        if lines[0].startswith(" "):
            return "script indent=2\n  %s\nend" % ("\n  ".join(lines),)
        else:
            return "script\n  %s\nend" % ("\n  ".join(lines),)

    def plan(self, server, rootdir, plan):
        # XXX: `self.command' is a string

        script_log_lines = []
        for line in self.script.strip("\n").split("\n"):
            script_log_lines.append(("# " + line).rstrip())
        plan.entry(
            action = [_execute, self.script, True, plan.get_env(), rootdir],
            log = ["run script:\n%s", "\n".join(script_log_lines)],
        )

# }}}
#-----------------------------------------------------------------------------

class Script:
    #-------------------------------------------------------
    # static fields for script parsing {{{

    _SPACES = {" ", "\t", "\n", "\r"}
    _OPS = {
        "wildcard": { "nargs": lambda(n): n == 1, "class": _OpWatch         },
        "ignore":   { "nargs": lambda(n): n == 1, "class": _OpIgnore        },
        "file":     { "nargs": lambda(n): n == 2, "class": _OpGetFile       },
        "dir":      { "nargs": lambda(n): n == 1, "class": _OpMakeDirectory },
        "symlink":  { "nargs": lambda(n): n == 2, "class": _OpSymlink       },
        "env":      { "nargs": lambda(n): n in (1, 2), "class": _OpSetEnv   },
        "run":      { "nargs": lambda(n): n >= 1, "class": _OpRunCommand    },
        "script":   { "nargs": lambda(n): True,   "class": _OpRunScript     },
    }

    # }}}
    #-------------------------------------------------------

    def __init__(self, script = None):
        self._globs = GlobMap()
        self._watch = []
        self._ops = []
        self._env = {}
        self._script = script
        if self._script is not None:
            self._parse()

    #-------------------------------------------------------
    # script builder {{{

    def watch(self, wildcard):
        # XXX: the last entry has the precedence
        pattern = _format(wildcard, None, {}, self._env)
        self._watch.append(_OpWatch(pattern))
        self._globs.add(pattern, include = True)
        return True

    def ignore(self, wildcard):
        # XXX: the last entry has the precedence
        pattern = _format(wildcard, None, {}, self._env)
        self._watch.append(_OpIgnore(pattern))
        self._globs.add(pattern, include = False)
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
            raise ScriptBuildError("invalid environment variable: %s", name)

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
        self._ops.append(_OpRunCommand(*cmd))
        return True

    def script(self, script):
        self._ops.append(_OpRunScript(script))
        return True

    # }}}
    #-------------------------------------------------------
    # string representation {{{

    def __str__(self):
        watch = "\n".join(str(p) for p in self._watch)
        ops = "\n".join(str(o) for o in self._ops)
        return "\n".join([watch, ops])

    def _parse(self):
        # TODO: change exception types
        # TODO: report exact error position

        lines = self._script.split("\n")
        l = 0
        while l < len(lines):
            fields = Script._parse_fields(lines[l])
            if fields is None:
                l += 1
                continue

            operation = fields[0]
            args = fields[1:]

            if operation not in Script._OPS:
                # TODO: different exception type
                raise ScriptParseError("unrecognized operation %s", operation)
            if not Script._OPS[operation]["nargs"](len(args)):
                raise ScriptParseError(
                    "wrong number of arguments for %s: (got %d)",
                    operation, len(args)
                )

            if operation == "script":
                # special parsing
                if len(args) == 0:
                    indent = 2
                elif len(args) == 1 and args[0].startswith("indent="):
                    try:
                        indent = int(args[0][len("indent="):])
                    except:
                        raise ScriptParseError("invalid script indent value")
                    if indent < 1:
                        raise ScriptParseError("invalid script indent value")
                indent = " " * indent
                l += 1
                script_start = l
                while l < len(lines) and \
                      (lines[l] == "" or lines[l].startswith(indent)):
                    # strip the leading spaces while we're passing here
                    lines[l] = lines[l][len(indent):]
                    l += 1
                if l == len(lines) or lines[l].strip() != "end":
                    raise ScriptParseError("unterminated script")
                # XXX: now lines[l] is at the "end" keyword
                args = [ "\n".join(lines[script_start:l]) ]

            node = Script._OPS[operation]["class"](*args)
            if isinstance(node, _OpWatch):
                self._watch.append(node)
                self._globs.add(node.wildcard, include = True)
            elif isinstance(node, _OpIgnore):
                self._watch.append(node)
                self._globs.add(node.wildcard, include = False)
            else:
                self._ops.append(node)
            l += 1

    @staticmethod
    def _count_spaces(line, pos):
        count = 0
        while pos < len(line) and line[pos] in Script._SPACES:
            count += 1
            pos += 1
        return count

    @staticmethod
    def _parse_quoted_field(line, pos):
        pos = pos + 1 # skip the initial double quote
        result = bytearray()
        while pos < len(line):
            if line[pos] == '"':
                return (pos, str(result))

            if ord(line[pos]) < 32:
                # control character (which includes TAB)
                raise ScriptParseError("unexpected control character")

            if line[pos] != "\\":
                result.append(line[pos])
            else: # line[pos] == "\\"
                pos += 1
                if pos >= len(line):
                    raise ScriptParseError("unterminated double quote")

                if line[pos] == "n":
                    result.append("\n")
                elif line[pos] == "t":
                    result.append("\t")
                elif line[pos] == "r":
                    result.append("\r")
                elif line[pos] == '"':
                    result.append('"')
                elif line[pos] == "\\":
                    result.append("\\")
                elif line[pos] == "x":
                    if pos + 2 >= len(line):
                        raise ScriptParseError("unterminated double quote")
                    try:
                        result.append(int(line[(pos + 1):(pos + 3)], 16))
                        pos += 2
                    except:
                        raise ScriptParseError("invalid \\x## sequence")
                else:
                    raise ScriptParseError("invalid \\X sequence")

            pos += 1

        raise ScriptParseError("unterminated double quote")

    @staticmethod
    def _parse_unquoted_field(line, pos):
        start = pos
        while pos < len(line) and line[pos] not in Script._SPACES:
            if line[pos] in _QUOTE:
                raise ScriptParseError("unquoted special character")
            pos += 1
        # return the last processed character (and the field, of course)
        return (pos - 1, line[start:pos])

    @staticmethod
    def _parse_fields(line):
        # skip initial whitespaces
        pos = Script._count_spaces(line, 0)
        if pos == len(line) or line[pos] == "#":
            # an empty line or a comment
            return None

        result = []
        while pos < len(line):
            # XXX: line[pos] is a non-space

            if line[pos] == '"':
                # quoted string
                # start with a quote character, end with a quote character
                (pos, field) = Script._parse_quoted_field(line, pos)
                result.append(field)
            elif line[pos] in _QUOTE:
                # special or control character, which is an error
                # FIXME: UTF-8 characters?
                raise ScriptParseError("unquoted special character")
            else:
                # unquoted string
                (pos, field) = Script._parse_unquoted_field(line, pos)
                result.append(field)

            # line[pos] was the last processed character
            pos += 1

            # skip whitespaces, so the next iteration starts at a field
            # boundary
            spaces = Script._count_spaces(line, pos)
            if spaces == 0 and pos < len(line):
                raise ScriptParseError("no space separator found")
            pos += spaces
        return result

    # }}}
    #-------------------------------------------------------
    # script execution {{{

    def run(self, server, rootdir, dry_run, handle):
        plan = _Plan()

        for op in self._ops:
            op.plan(server, rootdir, plan)

        for (fun, args, log) in plan.actions():
            if handle is not None and log is not None:
                handle.write(log)
                handle.write("\n")
                handle.flush()
            if not dry_run:
                fun(*args)

        if handle is not None:
            handle.write("cleanup: removing excessive files\n")
            handle.flush()

        # things to check if they were created by this run
        (paths, subtrees) = self._globs.dirtree(rootdir)
        for path in sorted(paths, reverse = True):
            if path in plan:
                continue
            if handle is not None:
                handle.write("delete %s\n" % (_quote(path),))
            if not dry_run:
                full_path = os.path.join(rootdir, path)
                ftype = _file_type(full_path)
                if ftype == stat.S_IFDIR:
                    try:
                        os.rmdir(full_path)
                    except OSError, e:
                        if e.errno == errno.ENOTEMPTY:
                            continue
                        raise
                elif ftype is not None:
                    os.unlink(full_path)
        if handle is not None:
            handle.flush()

        def strip_rootdir(path):
            if len(path) > len(rootdir) and path.startswith(rootdir) and \
               path[len(rootdir)] == "/":
                return path[(len(rootdir) + 1):]
            return path

        for tree in subtrees:
            tree_path = os.path.join(rootdir, tree)
            for (d, dirs, files) in os.walk(tree_path, topdown = False):
                drel = strip_rootdir(d)

                for name in dirs:
                    path = os.path.join(drel, name)
                    if path in plan:
                        continue
                    if handle is not None:
                        handle.write("delete %s\n" % (_quote(path),))
                    if not dry_run:
                        # the directory should be empty (its children deleted
                        # because of depth-first walk); if it contained any
                        # files from the plan, it should be in the plan
                        # itself, so we wouldn't get here
                        os.rmdir(os.path.join(rootdir, path))

                for name in files:
                    path = os.path.join(drel, name)
                    if path in plan:
                        continue
                    if handle is not None:
                        handle.write("delete %s\n" % (_quote(path),))
                    if not dry_run:
                        os.unlink(os.path.join(rootdir, path))

            if tree not in plan:
                if handle is not None:
                    handle.write("delete %s\n" % (_quote(tree),))
                    handle.flush()
                if not dry_run:
                    os.rmdir(os.path.join(rootdir, tree_path))

    # }}}
    #-------------------------------------------------------

#-----------------------------------------------------------------------------
# vim:ft=python:foldmethod=marker
