# -*- coding: utf-8 -*-
#
# Copyright (C) 2012-2013 Edgewall Software
# Copyright (C) 2006-2011, Herbert Valerio Riedel <hvr@gnu.org>
# All rights reserved.
#
# This software is licensed as described in the file COPYING, which
# you should have received as part of this distribution. The terms
# are also available at http://trac.edgewall.org/wiki/TracLicense.
#
# This software consists of voluntary contributions made by many
# individuals. For the exact contribution history, see the revision
# history and logs, available at http://trac.edgewall.org/log/.

from __future__ import with_statement

import os
import posixpath
from cStringIO import StringIO
from datetime import datetime

try:
    import pygit2
except ImportError:
    pygit2 = None
if pygit2:
    from pygit2 import (
        GIT_OBJ_COMMIT, GIT_OBJ_TREE, GIT_OBJ_BLOB, GIT_OBJ_TAG,
        GIT_DELTA_ADDED, GIT_DELTA_DELETED, GIT_DELTA_MODIFIED,
        GIT_DELTA_RENAMED, GIT_DELTA_COPIED,
        GIT_SORT_TIME, GIT_SORT_REVERSE,
    )

from genshi.builder import tag

from trac.config import BoolOption, IntOption, PathOption, Option
from trac.core import Component, implements, TracError
from trac.env import ISystemInfoProvider
from trac.util import shorten_line
from trac.util.datefmt import (
    utc, FixedOffset, to_timestamp, format_datetime, user_time,
)
from trac.util.text import to_unicode
from trac.util.translation import _, N_, tag_, gettext
from trac.versioncontrol.api import (
    Changeset, Node, Repository, IRepositoryConnector, NoSuchChangeset,
    NoSuchNode, IRepositoryProvider,
)
from trac.versioncontrol.cache import CachedRepository, CachedChangeset
from trac.versioncontrol.web_ui import IPropertyRenderer, RenderedProperty
from trac.web.chrome import Chrome
from trac.wiki import IWikiSyntaxProvider
from trac.wiki.formatter import wiki_to_oneliner


__all__ = ['GitCachedRepository', 'GitCachedChangeset', 'GitConnector',
           'GitRepository', 'GitChangeset', 'GitNode',
           'GitwebProjectsRepositoryProvider']


if pygit2:
    _DELTA_STATUS_MAP = {
        GIT_DELTA_ADDED:    Changeset.ADD,
        GIT_DELTA_DELETED:  Changeset.DELETE,
        GIT_DELTA_MODIFIED: Changeset.EDIT,
        GIT_DELTA_RENAMED:  Changeset.MOVE,
        GIT_DELTA_COPIED:   Changeset.COPY,
    }
else:
    _DELTA_STATUS_MAP = {}


class GitCachedRepository(CachedRepository):
    """Git-specific cached repository.

    Passes through {display,short,normalize}_rev
    """

    def short_rev(self, rev):
        return self.repos.short_rev(rev)

    display_rev = short_rev

    def normalize_rev(self, rev):
        return self.repos.normalize_rev(rev)

    def get_changeset(self, rev):
        return GitCachedChangeset(self, self.normalize_rev(rev), self.env)


class GitCachedChangeset(CachedChangeset):
    """Git-specific cached changeset.

    Handles get_branches() and get_tags()
    """
    def get_branches(self):
        return self.repos.repos.get_changeset(self.rev).get_branches()

    def get_tags(self):
        return self.repos.repos.get_changeset(self.rev).get_tags()


def intersperse(sep, iterable):
    """The 'intersperse' generator takes an element and an iterable and
    intersperses that element between the elements of the iterable.

    inspired by Haskell's ``Data.List.intersperse``
    """

    for i, item in enumerate(iterable):
        if i: yield sep
        yield item


def _git_timestamp(ts, offset):
    if offset == 0:
        tz = utc
    else:
        hours, rem = divmod(abs(offset), 60)
        tzname = 'UTC%+03d:%02d' % (-hours if offset < 0 else hours, rem)
        tz = FixedOffset(offset, tzname)
    return datetime.fromtimestamp(ts, tz)


def _format_signature(signature):
    name = signature.name.strip()
    email = signature.email.strip()
    return ('%s <%s>' % (name, email)).strip()


def _walk_tree(tree, path=None):
    for entry in tree:
        name = posixpath.join(path, entry.name) \
               if path is not None else entry.name
        git_object = entry.to_object()
        if git_object.type == GIT_OBJ_TREE:
            for val in _walk_tree(git_object, name):
                yield val
        else:
            yield git_object, name


class _cached_walker(object):

    __slots__ = ['walker', 'revs', 'commits']

    def __init__(self, walker):
        self.walker = walker
        self.revs = set()
        self.commits = []

    def __contains__(self, rev):
        if rev in self.revs:
            return True
        add_rev = self.revs.add
        add_commit = self.commits.append
        for commit in self.walker:
            old_rev = commit.hex
            add_commit(commit)
            add_rev(old_rev)
            if old_rev == rev:
                return True
        return False

    def reverse(self, start_rev):
        if start_rev not in self:
            return
        match = False
        for commit in reversed(self.commits):
            if not match and commit.hex == start_rev:
                match = True
            if match:
                yield commit


class GitConnector(Component):

    implements(ISystemInfoProvider, IRepositoryConnector, IWikiSyntaxProvider)

    # ISystemInfoProvider methods

    def get_system_info(self):
        if pygit2:
            pygit2_version = pygit2.__version__
            if hasattr(pygit2, 'LIBGIT2_VERSION'):
                pygit2_version = '%s (compiled with libgit2 %s)' % \
                                 (pygit2_version, pygit2.LIBGIT2_VERSION)
            yield 'pygit2', pygit2_version
            libgit2_version = self._get_libgit2_version()
            if libgit2_version:
                yield 'libgit2', libgit2_version

    def _get_libgit2_version(self):
        from ctypes import CDLL, CFUNCTYPE, POINTER, c_int, pointer
        from ctypes.util import find_library
        try:
            sharedlib = CDLL('git2.dll' if os.name == 'nt' else
                             find_library('git2'))
            prototype = CFUNCTYPE(None, POINTER(c_int), POINTER(c_int),
                                  POINTER(c_int))
            func = prototype(('git_libgit2_version', sharedlib))
            args = [pointer(c_int(0)) for i in xrange(3)]
            func(*args)
            return '.'.join([str(arg[0]) for arg in args])
        except:
            self.log.warn('Exception caught while retrieving libgit2 version',
                          exc_info=True)

    # IWikiSyntaxProvider methods

    def _format_sha_link(self, formatter, sha, label):
        # FIXME: this function needs serious rethinking...

        reponame = ''

        context = formatter.context
        while context:
            if context.resource.realm in ('source', 'changeset'):
                reponame = context.resource.parent.id
                break
            context = context.parent

        try:
            repos = self.env.get_repository(reponame)

            if not repos:
                raise Exception("Repository '%s' not found" % reponame)

            sha = repos.normalize_rev(sha) # in case it was abbreviated
            changeset = repos.get_changeset(sha)
            return tag.a(label, class_='changeset',
                         title=shorten_line(changeset.message),
                         href=formatter.href.changeset(sha, repos.reponame))
        except Exception, e:
            return tag.a(label, class_='missing changeset',
                         title=to_unicode(e), rel='nofollow')

    def get_wiki_syntax(self):
        yield (r'(?:\b|!)r?[0-9a-fA-F]{%d,40}\b' % self.wiki_shortrev_len,
               lambda fmt, sha, match:
                    self._format_sha_link(fmt, sha.startswith('r')
                                          and sha[1:] or sha, sha))

    def get_link_resolvers(self):
        yield ('sha', lambda fmt, _, sha, label, match=None:
                        self._format_sha_link(fmt, sha, label))

    # IRepositoryConnector methods

    cached_repository = BoolOption('git', 'cached_repository', 'false',
        """Wrap `GitRepository` in `CachedRepository`.""")

    shortrev_len = IntOption('git', 'shortrev_len', 7,
        """The length at which a sha1 should be abbreviated to (must
        be >= 4 and <= 40).
        """)

    wiki_shortrev_len = IntOption('git', 'wikishortrev_len', 40,
        """The minimum length of an hex-string for which
        auto-detection as sha1 is performed (must be >= 4 and <= 40).
        """)

    trac_user_rlookup = BoolOption('git', 'trac_user_rlookup', 'false',
        """Enable reverse mapping of git email addresses to trac user ids
        (costly if you have many users).""")

    use_committer_id = BoolOption('git', 'use_committer_id', 'true',
        """Use git-committer id instead of git-author id for the
        changeset ''Author'' field.
        """)

    use_committer_time = BoolOption('git', 'use_committer_time', 'true',
        """Use git-committer timestamp instead of git-author timestamp
        for the changeset ''Timestamp'' field.
        """)

    git_fs_encoding = Option('git', 'git_fs_encoding', 'utf-8',
        """Define charset encoding of paths within git repositories.""")


    def get_supported_types(self):
        if pygit2:
            yield ('git', 8)

    def get_repository(self, type, dir, params):
        """GitRepository factory method"""
        assert type == 'git'

        if not (4 <= self.shortrev_len <= 40):
            raise TracError(_("[git] shortrev_len setting must be within "
                              "[4..40]"))

        if not (4 <= self.wiki_shortrev_len <= 40):
            raise TracError(_("[git] wikishortrev_len must be within [4..40]"))

        format_signature = self._format_signature_by_email \
                           if self.trac_user_rlookup else None

        repos = GitRepository(dir, params, self.log,
                              git_fs_encoding=self.git_fs_encoding,
                              shortrev_len=self.shortrev_len,
                              format_signature=format_signature,
                              use_committer_id=self.use_committer_id,
                              use_committer_time=self.use_committer_time)

        if self.cached_repository:
            repos = GitCachedRepository(self.env, repos, self.log)
            self.log.debug("enabled CachedRepository for '%s'", dir)
        else:
            self.log.debug("disabled CachedRepository for '%s'", dir)

        return repos

    def _format_signature_by_email(self, signature):
        """Reverse map 'real name <user@domain.tld>' addresses to trac
        user ids.
        """
        email = (signature.email or '').strip()
        if email:
            email = email.lower()
            for username, name, _email in self.env.get_known_users():
                if email == _email.lower():
                    return username
        return _format_signature(signature)


class CsetPropertyRenderer(Component):

    implements(IPropertyRenderer)

    git_properties = (
        N_("Parents:"), N_("Children:"), N_("Branches:"), N_("Tags:"),
    )

    # relied upon by GitChangeset
    def match_property(self, name, mode):
        if (mode == 'revprop' and
            name.startswith('git-') and
            name[4:] in ('Parents', 'Children', 'Branches', 'Tags',
                         'committer', 'author')):
            return 4
        return 0

    def render_property(self, name, mode, context, props):
        if name.startswith('git-'):
            label = name[4:] + ':'
            if label in self.git_properties:
                label = gettext(label)
        else:
            label = name
        return RenderedProperty(
                name=label, name_attributes=[('class', 'property')],
                content=self._render_property(name, props[name], context))

    def _render_property(self, name, value, context):
        if name == 'git-Branches':
            return self._render_branches(context, value)

        if name == 'git-Tags':
            return self._render_tags(context, value)

        if name == 'git-Parents' and len(value) > 1:
            return self._render_merge_commit(context, value)

        if name in ('git-Parents', 'git-Children'):
            return self._render_revs(context, value)

        if name in ('git-committer', 'git-author'):
            return self._render_signature(context, value)

        raise TracError("Internal error")

    def _render_branches(self, context, branches):
        links = [self._changeset_link(context, rev, name)
                 for name, rev in branches]
        return tag(*intersperse(', ', links))

    def _render_tags(self, context, names):
        rev = context.resource.id
        links = [self._changeset_link(context, rev, name) for name in names]
        return tag(*intersperse(', ', links))

    def _render_revs(self, context, revs):
        links = [self._changeset_link(context, rev) for rev in revs]
        return tag(*intersperse(', ', links))

    def _render_merge_commit(self, context, revs):
        # we got a merge...
        curr_rev = context.resource.id
        reponame = context.resource.parent.id
        href = context.href

        def parent_diff(rev):
            rev = self._changeset_link(context, rev)
            diff = tag.a(_("diff"),
                         href=href.changeset(curr_rev, reponame, old=rev),
                         title=_("Diff against this parent (show the changes "
                                 "merged from the other parents)"))
            return tag_("%(rev)s (%(diff)s)", rev=rev, diff=diff)

        links = intersperse(', ', map(parent_diff, revs))
        hint = wiki_to_oneliner(
            _("'''Note''': this is a '''merge''' changeset, the changes "
              "displayed below correspond to the merge itself. Use the "
              "`(diff)` links above to see all the changes relative to each "
              "parent."),
            self.env)
        return tag(list(links), tag.br(), tag.span(hint, class_='hint'))

    def _render_signature(self, context, signature):
        req = context.req
        dt = _git_timestamp(signature.time, signature.offset)
        chrome = Chrome(self.env)
        return u'%s (%s)' % (chrome.format_author(req, signature.name),
                             user_time(req, format_datetime, dt))

    def _changeset_link(self, context, rev, label=None):
        # `rev` is assumed to be a non-abbreviated 40-chars sha id
        reponame = context.resource.parent.id
        repos = self.env.get_repository(reponame)
        try:
            cset = repos.get_changeset(rev)
        except (NoSuchChangeset, NoSuchNode), e:
            return tag.a(rev, class_='missing changeset', title=to_unicode(e),
                         rel='nofollow')
        if label is None:
            label = repos.display_rev(rev)
        return tag.a(label, class_='changeset',
                     title=shorten_line(cset.message),
                     href=context.href.changeset(rev, repos.reponame))



class GitRepository(Repository):
    """Git repository"""

    def __init__(self, path, params, log, git_fs_encoding='utf-8',
                 shortrev_len=7, format_signature=None, use_committer_id=False,
                 use_committer_time=False):

        try:
            self.git_repos = pygit2.Repository(path)
        except:
            log.warn('Not git repository: %r', path, exc_info=True)
            raise TracError(_("%(path)s does not appear to be a Git "
                              "repository.", path=path))

        self.path = path
        self.params = params
        self.git_fs_encoding = git_fs_encoding
        self.shortrev_len = max(4, min(shortrev_len, 40))
        self.format_signature = format_signature or _format_signature
        self.use_committer_id = use_committer_id
        self.use_committer_time = use_committer_time
        self._ref_walkers = None
        Repository.__init__(self, 'git:' + path, self.params, log)

    def _from_fspath(self, name):
        return name.decode(self.git_fs_encoding)

    def _to_fspath(self, name):
        return name.encode(self.git_fs_encoding)

    def _get_commit_username(self, commit):
        signature = commit.committer or commit.author \
                    if self.use_committer_id \
                    else commit.author or commit.committer
        return self.format_signature(signature)

    def _get_commit_time(self, commit):
        signature = commit.committer or commit.author \
                    if self.use_committer_time \
                    else commit.author or commit.committer
        return _git_timestamp(signature.time, signature.offset)

    def _get_tree_entry(self, tree, path):
        if not path:
            return None
        if isinstance(path, unicode):
            path = self._to_fspath(path)
        entry = tree
        for name in path.split('/'):
            if name not in tree:
                return None
            entry = tree[name]
            tree = entry.to_object()
        return entry

    def _get_tree(self, tree, path):
        if not path:
            return tree
        entry = self._get_tree_entry(tree, path)
        return entry.to_object() if entry else None

    def _get_commit(self, oid):
        git_repos = self.git_repos
        try:
            git_object = git_repos[oid]
        except (KeyError, ValueError):
            return None
        if git_object.type == GIT_OBJ_TAG:
            git_object = git_repos[git_object.target]
        if git_object.type == GIT_OBJ_COMMIT:
            return git_object
        return None

    def _get_ref_walkers(self):
        walkers = self._ref_walkers
        if walkers is not None:
            return walkers

        git_repos = self.git_repos
        walkers = {}
        for name in git_repos.listall_references():
            if not name.startswith('refs/heads/'):
                continue
            ref = git_repos.lookup_reference(name)
            commit = self._get_commit(ref.oid)
            if not commit:
                continue
            walkers[name] = _cached_walker(git_repos.walk(commit.oid,
                                                          GIT_SORT_TIME))
        self._ref_walkers = walkers
        return walkers

    def _iter_ref_walkers(self, rev):
        git_repos = self.git_repos
        target = git_repos[rev]

        walkers = self._get_ref_walkers()
        for name in git_repos.listall_references():
            if not name.startswith('refs/heads/'):
                continue
            walker = walkers.get(name)
            if walker is None:
                continue
            ref = git_repos.lookup_reference(name)
            commit = git_repos[ref.oid]
            if commit.commit_time < target.commit_time:
                continue
            yield self._from_fspath(name), ref, walker

    def _get_changes(self, parent_tree, commit_tree):
        _get_tree_entry = self._get_tree_entry
        files = []
        added_oids = {}
        deleted_oids = {}
        parent_oids = {}
        diff = parent_tree.diff(commit_tree)

        for change in diff.changes.get('files', ()):
            old = change[0]
            new = change[1]
            status = change[2]
            if status == GIT_DELTA_ADDED:
                entry = _get_tree_entry(commit_tree, new)
                added_oids.setdefault(entry.oid, []).append(new)
            elif status == GIT_DELTA_DELETED:
                entry = _get_tree_entry(parent_tree, old)
                deleted_oids.setdefault(entry.oid, []).append(old)
            else:
                files.append((old, new, status))

        if added_oids:
            for git_object, name in _walk_tree(parent_tree):
                parent_oids.setdefault(git_object.oid, []).append(name)

        for oids in (added_oids, deleted_oids, parent_oids):
            for paths in oids.itervalues():
                paths.sort(reverse=True)

        # Handle copying and renaming files
        for oid, added_paths in added_oids.iteritems():
            deleted_paths = deleted_oids.get(oid, ())
            parent_paths = parent_oids.get(oid, ())
            while added_paths:
                added_path = added_paths[-1]
                if deleted_paths:
                    files.append((deleted_paths.pop(), added_path,
                                  GIT_DELTA_RENAMED))
                elif parent_paths:
                    files.append((parent_paths.pop(), added_path,
                                  GIT_DELTA_COPIED))
                else:
                    break
                added_paths.pop()

        files.extend((path, path, GIT_DELTA_ADDED)
                     for added_paths in added_oids.itervalues()
                     for path in added_paths)
        files.extend((path, path, GIT_DELTA_DELETED)
                     for deleted_paths in deleted_oids.itervalues()
                     for path in deleted_paths)

        _from_fspath = self._from_fspath
        return sorted(((_from_fspath(old), _from_fspath(new), status)
                       for old, new, status in files),
                      key=lambda change: change[1])

    def _get_branches(self, rev):
        return sorted((name[11:], ref.hex)
                      for name, ref, walker in self._iter_ref_walkers(rev)
                      if rev in walker)

    def _resolve_rev(self, rev, raises=True):
        if rev is not None and not isinstance(rev, unicode):
            normrev = unicode(rev, 'latin1') if isinstance(rev, str) \
                      else to_unicode(rev)
        else:
            normrev = rev
        if not normrev:
            try:
                return self.git_repos.head
            except pygit2.GitError:
                if raises:
                    raise NoSuchChangeset(rev)
                return None

        commit = self._get_commit(normrev)
        if commit:
            return commit

        for name in self.git_repos.listall_references():
            ref = self.git_repos.lookup_reference(name)
            name = self._from_fspath(name)
            if name.startswith('refs/heads/'):
                match = name[11:] == normrev
            elif name.startswith('refs/tags/'):
                match = name[10:] == normrev
            else:
                continue
            if not match:
                continue
            commit = self._get_commit(ref.oid)
            if commit:
                return commit

        if raises:
            raise NoSuchChangeset(rev)

    def close(self):
        self.clear()
        self.git_repos = None

    def get_youngest_rev(self):
        try:
            return self.git_repos.head.hex
        except pygit2.GitError:
            return None

    def get_oldest_rev(self):
        try:
            self.git_repos.head
        except pygit2.GitError:
            return None
        sort = GIT_SORT_TIME | GIT_SORT_REVERSE
        for commit in self.git_repos.walk(self.git_repos.head.oid, sort):
            return commit.hex

    def normalize_path(self, path):
        if isinstance(path, str):
            path = self._from_fspath(path)
        return path.strip('/') if path else ''

    def normalize_rev(self, rev):
        return self._resolve_rev(rev).hex

    def short_rev(self, rev):
        rev = self.normalize_rev(rev)
        git_repos = self.git_repos
        for size in xrange(self.shortrev_len, 41):
            short_rev = rev[:size]
            try:
                git_object = git_repos[short_rev]
                if git_object.type == GIT_OBJ_COMMIT:
                    return short_rev
            except ValueError:
                pass
        raise TracError("was not able to find rev %s"%rev)

    display_rev = short_rev

    def get_node(self, path, rev=None):
        commit = self._resolve_rev(rev, raises=False)
        if not commit:
            raise NoSuchNode(path, rev)
        return GitNode(self, self.normalize_path(path), commit)

    def get_quickjump_entries(self, rev):
        git_repos = self.git_repos
        refs = sorted(
            (self._from_fspath(name), git_repos.lookup_reference(name))
            for name in git_repos.listall_references()
            if name.startswith(('refs/heads/', 'refs/tags/')))

        for name, ref in refs:
            if name.startswith('refs/heads/'):
                commit = self._get_commit(ref.oid)
                if commit:
                    yield 'branches', name[11:], '/', commit.hex

        for name, ref in refs:
            if name.startswith('refs/tags/'):
                commit = self._get_commit(ref.oid)
                yield 'tags', name[10:], '/', commit.hex

    def get_path_url(self, path, rev):
        return self.params.get('url')

    def get_changesets(self, start, stop):
        seen_oids = set()
        def iter_commits():
            ts_start = to_timestamp(start)
            ts_stop = to_timestamp(stop)
            git_repos = self.git_repos
            for name in git_repos.listall_references():
                if not name.startswith('refs/heads/'):
                    continue
                ref = git_repos.lookup_reference(name)
                for commit in git_repos.walk(ref.oid, GIT_SORT_TIME):
                    ts = commit.commit_time
                    if ts < ts_start:
                        break
                    if ts_start <= ts <= ts_stop:
                        oid = commit.oid
                        if oid not in seen_oids:
                            seen_oids.add(oid)
                            yield ts, commit

        for ts, commit in sorted(iter_commits(), key=lambda v: v[0],
                                 reverse=True):
            yield GitChangeset(self, commit)

    def get_changeset(self, rev):
        return GitChangeset(self, self._resolve_rev(rev))

    def get_changeset_uid(self, rev):
        return rev

    def get_changeset_uid(self, rev):
        return self.normalize_rev(rev)

    def get_changes(self, old_path, old_rev, new_path, new_rev,
                    ignore_ancestry=0):
        # TODO: handle ignore_ancestry

        def iter_changes(old_commit, old_path, new_commit, new_path):
            old_tree = self._get_tree(old_commit.tree, old_path)
            old_rev = old_commit.hex
            new_tree = self._get_tree(new_commit.tree, new_path)
            new_rev = new_commit.hex

            for old_file, new_file, status in \
                    self._get_changes(old_tree, new_tree):
                action = _DELTA_STATUS_MAP.get(status)
                if not action:
                    continue
                old_node = new_node = None
                if status != GIT_DELTA_ADDED:
                    old_node = self.get_node(
                                posixpath.join(old_path, old_file), old_rev)
                if status != GIT_DELTA_DELETED:
                    new_node = self.get_node(
                                posixpath.join(new_path, new_file), new_rev)
                yield old_node, new_node, Node.FILE, action

        old_commit = self._resolve_rev(old_rev)
        new_commit = self._resolve_rev(new_rev)
        return iter_changes(old_commit, self.normalize_path(old_path),
                            new_commit, self.normalize_path(new_path))

    def previous_rev(self, rev, path=''):
        commit = self._resolve_rev(rev)
        if not path or path == '/':
            for parent in commit.parents:
                return parent.hex
        else:
            node = GitNode(self, self.normalize_path(path), commit)
            for commit, action in node._walk_commits(node.rev):
                for parent in commit.parents:
                    return parent.hex

    def next_rev(self, rev, path=''):
        rev = self.normalize_rev(rev)
        path = self.normalize_path(path)

        for name, ref, walker in self._iter_ref_walkers(rev):
            if rev not in walker:
                continue
            for commit in walker.reverse(rev):
                if not any(p.hex == rev for p in commit.parents):
                    continue
                tree = commit.tree
                entry = self._get_tree(tree, path)
                if entry is None:
                    return None
                for parent in commit.parents:
                    parent_tree = parent.tree
                    if entry.oid == parent_tree.oid:
                        continue
                    parent_entry = self._get_tree(parent_tree, path)
                    if entry is parent_entry is None:
                        continue
                    if (entry is None or parent_entry is None or
                        entry.oid != parent_entry.oid):
                        return commit.hex
                rev = commit.hex

    def parent_revs(self, rev):
        commit = self._resolve_rev(rev)
        return [c.hex for c in commit.parents]

    def child_revs(self, rev):
        def iter_children(rev):
            seen = set()
            for name, ref, walker in self._iter_ref_walkers(rev):
                if rev not in walker:
                    continue
                for commit in walker.reverse(rev):
                    if commit.oid in seen:
                        break
                    seen.add(commit.oid)
                    if any(p.hex == rev for p in commit.parents):
                        yield commit
        return [c.hex for c in iter_children(self.normalize_rev(rev))]

    def rev_older_than(self, rev1, rev2):
        oid1 = self._resolve_rev(rev1).oid
        oid2 = self._resolve_rev(rev2).oid
        return any(oid2 == c.oid
                   for c in self.git_repos.walk(oid1, GIT_SORT_TIME))

    def get_path_history(self, path, rev=None, limit=None):
        raise TracError(_("GitRepository does not support path_history"))

    def clear(self, youngest_rev=None):
        self._ref_walkers = None


class GitNode(Node):

    def __init__(self, repos, path, rev, created_commit=None):
        self.log = repos.log

        if type(rev) is pygit2.Commit:
            commit = rev
            rev = commit.hex
        else:
            commit = repos._resolve_rev(rev, raises=False)
            if not commit:
                raise NoSuchNode(path, rev)
        normrev = commit.hex

        tree_entry = None
        git_object = commit.tree
        if path:
            tree_entry = repos._get_tree_entry(git_object, path)
            if tree_entry is None:
                raise NoSuchNode(path, rev)
            git_object = tree_entry.to_object()

        if git_object.type == GIT_OBJ_TREE:
            kind = Node.DIRECTORY
            tree = git_object
            blob = None
        elif git_object.type == GIT_OBJ_BLOB:
            kind = Node.FILE
            tree = None
            blob = git_object
        else:
            raise NoSuchNode(path, rev)

        self.commit = commit
        self.tree_entry = tree_entry
        self.tree = tree
        self.blob = blob
        self.created_path = path  # XXX how to use?
        self._created_commit = created_commit
        Node.__init__(self, repos, path, normrev, kind)

    def _get_created_commit(self):
        commit = self._created_commit
        if commit is None:
            for commit, action in self._walk_commits(self.rev):
                self._created_commit = commit
                break
        return commit

    @property
    def created_rev(self):
        return self._get_created_commit().hex

    def _walk_commits(self, rev, path=None):
        if path is None:
            path = self.path

        _get_tree = self.repos._get_tree
        path = self.repos._to_fspath(path)
        commit = None
        for commit in self.repos.git_repos.walk(rev, GIT_SORT_TIME):
            tree = _get_tree(commit.tree, path)
            for parent in commit.parents:
                parent_tree = _get_tree(parent.tree, path)
                action = None
                if tree is parent_tree is None:
                    return
                if tree is None:
                    action = Changeset.DELETE
                elif parent_tree is None:
                    action = Changeset.ADD
                elif parent_tree.oid != tree.oid:
                    action = Changeset.EDIT
                else:
                    continue
                yield commit, action
                break
        if commit and not commit.parents:
            yield commit, Changeset.ADD

    def get_content(self):
        if not self.isfile:
            return None
        return StringIO(self.blob.data)

    def get_properties(self):
        props = {}
        if self.tree_entry:
            mode = getattr(self.tree_entry, 'filemode', None)
            if mode is None:
                mode = self.tree_entry.attributes # 0.17.3
            props['mode'] = '%06o' % mode
        return props

    def get_annotations(self):
        if not self.isfile:
            return
        # TODO: libgit2 v0.17 and pygit2 v0.17.3 have no blame feature
        # ('blame' branch - https://github.com/libgit2/libgit2/pull/1317)
        raise NotImplementedError()

    def get_entries(self):
        if not self.isdir:
            return

        repos = self.repos
        _from_fspath = repos._from_fspath
        path = repos._to_fspath(self.path)
        names = sorted(entry.name for entry in self.tree)

        def get_entries(tree):
            if not tree:
                tree = ()
            return dict((name, tree[name] if name in tree else None)
                        for name in names)

        def get_commits():
            commits = {}
            _get_tree = repos._get_tree
            for commit in repos.git_repos.walk(self.created_rev,
                                               GIT_SORT_TIME):
                tree = _get_tree(commit.tree, path)
                entries = get_entries(tree)
                for parent in commit.parents:
                    parent_tree = _get_tree(parent.tree, path)
                    parent_entries = get_entries(parent_tree)
                    for name in names:
                        if name in commits:
                            continue
                        entry = entries[name]
                        parent_entry = parent_entries[name]
                        if entry is parent_entry is None:
                            commits[name] = None
                            continue
                        if (entry is None or parent_entry is None or
                            entry.oid != parent_entry.oid):
                            commits[name] = commit
                            continue
                    if len(commits) == len(names):
                        return commits
            return commits

        commits = get_commits()
        for name in names:
            yield GitNode(repos, posixpath.join(self.path, _from_fspath(name)),
                          self.rev, created_commit=commits.get(name))

    def get_content_type(self):
        if self.isdir:
            return None
        return ''

    def get_content_length(self):
        if not self.isfile:
            return None
        return self.blob.size

    def get_history(self, limit=None):
        path = self.path
        count = 0
        for commit, action in self._walk_commits(self.rev):
            yield path, commit.hex, action
            count += 1
            if limit == count:
                return

    def get_last_modified(self):
        if not self.isfile:
            return None
        commit = self._get_created_commit()
        return self.repos._get_commit_time(commit)


class GitChangeset(Changeset):
    """A Git changeset in the Git repository.

    Corresponds to a Git commit blob.
    """

    def __init__(self, repos, rev):
        self.log = repos.log

        if type(rev) is pygit2.Commit:
            commit = rev
            rev = commit.hex
        else:
            commit = repos._resolve_rev(rev)
            rev = commit.hex

        author = repos._get_commit_username(commit)
        date = repos._get_commit_time(commit)

        self.commit = commit
        Changeset.__init__(self, repos, rev, commit.message, author, date)

    def get_branches(self):
        branches = self.repos._get_branches(self.rev)
        return [(name, rev == self.rev) for name, rev in branches]

    def get_tags(self):
        repos = self.repos
        git_repos = repos.git_repos

        def iter_tags():
            for name in git_repos.listall_references():
                if not name.startswith('refs/tags/'):
                    continue
                ref = git_repos.lookup_reference(name)
                git_object = git_repos[ref.oid]
                if git_object.type == GIT_OBJ_TAG:
                    git_object = git_repos[git_object.target]
                if self.rev == git_object.hex:
                    yield repos._from_fspath(name[10:])

        return sorted(iter_tags())

    def get_properties(self):
        properties = {}
        commit = self.commit

        if commit.parents:
            properties['git-Parents'] = [c.hex for c in commit.parents]

        if (commit.author.name != commit.committer.name or
            commit.author.email != commit.committer.email):
            properties['git-committer'] = commit.committer
            properties['git-author'] = commit.author

        branches = self.repos._get_branches(self.rev)
        if branches:
            properties['git-Branches'] = branches
        tags = self.get_tags()
        if tags:
            properties['git-Tags'] = tags

        children = self.repos.child_revs(self.rev)
        if children:
            properties['git-Children'] = children

        return properties

    def get_changes(self):
        commit = self.commit
        if commit.parents:
            # diff for the first parent if even merge-commit
            parent = commit.parents[0]
            parent_rev = parent.hex
            files = self.repos._get_changes(parent.tree, commit.tree)
        else:
            _from_fspath = self.repos._from_fspath
            files = sorted(((None, _from_fspath(name), GIT_DELTA_ADDED)
                            for git_object, name in _walk_tree(commit.tree)),
                           key=lambda change: change[1])
            parent_rev = None

        for old_path, new_path, status in files:
            action = _DELTA_STATUS_MAP.get(status)
            if not action:
                continue
            if status == GIT_DELTA_ADDED:
                yield new_path, Node.FILE, action, None, None
            else:
                yield new_path, Node.FILE, action, old_path, parent_rev


class GitwebProjectsRepositoryProvider(Component):

    implements(IRepositoryProvider)

    projects_list = PathOption('git', 'projects_list', doc=
        """Path to a gitweb-formatted projects.list""")

    projects_base = PathOption('git', 'projects_base', doc=
        """Path to the base of your git projects""")

    projects_url = Option('git', 'projects_url', doc=
        """Template for project URLs. %s will be replaced with the repo
        name""")

    def get_repositories(self):
        if not self.projects_list:
            return

        for line in open(self.projects_list):
            line = line.strip()
            name = line
            if name.endswith('.git'):
                name = name[:-4]
            repo = {
                'dir': os.path.join(self.projects_base, line),
                'type': 'git',
            }
            description_path = os.path.join(repo['dir'], 'description')
            if os.path.exists(description_path):
                repo['description'] = open(description_path).read().strip()
            if self.projects_url:
                repo['url'] = self.projects_url % name
            yield name, repo
