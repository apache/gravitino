# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import importlib.util
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest import mock

SCRIPT = Path(__file__).parent.parent.parent / "scripts" / "generate_version.py"


def _load_script():
    spec = importlib.util.spec_from_file_location("generate_version", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _git(cwd, *args):
    result = subprocess.run(
        ["git", "-c", "user.name=t", "-c", "user.email=t@t", *args],
        cwd=cwd,
        capture_output=True,
        text=True,
        check=True,
    )
    return result.stdout.strip()


@unittest.skipIf(shutil.which("git") is None, "git is not installed")
class TestGenerateVersionCommitId(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.tmp = Path(self._tmp.name).resolve()
        self.repo = self.tmp / "repo"
        self.repo.mkdir()
        _git(self.repo, "init", "-q", "-b", "main")
        _git(self.repo, "commit", "-q", "--allow-empty", "-m", "init")
        self.commit = _git(self.repo, "rev-parse", "HEAD")
        self.script = _load_script()

    def _commit_id(self, root, use_git=True):
        self.script.PROJECT_ROOT = root
        if use_git:
            return self.script.get_git_commit_id()
        # Force the file-parsing fallback by making the git call fail.
        with mock.patch.object(self.script.subprocess, "run", side_effect=OSError):
            return self.script.get_git_commit_id()

    def test_loose_refs(self):
        for use_git in (True, False):
            self.assertEqual(self.commit, self._commit_id(self.repo, use_git))

    def test_packed_refs(self):
        _git(self.repo, "pack-refs", "--all")
        self.assertFalse(any((self.repo / ".git/refs/heads").iterdir()))
        for use_git in (True, False):
            self.assertEqual(self.commit, self._commit_id(self.repo, use_git))

    def test_detached_head(self):
        _git(self.repo, "checkout", "-q", "--detach")
        for use_git in (True, False):
            self.assertEqual(self.commit, self._commit_id(self.repo, use_git))

    def test_linked_worktrees(self):
        named = self.tmp / "named"
        detached = self.tmp / "detached"
        _git(self.repo, "worktree", "add", "-q", "-b", "wt", str(named))
        _git(self.repo, "worktree", "add", "-q", "--detach", str(detached))
        _git(self.repo, "pack-refs", "--all")
        for root in (named, detached):
            for use_git in (True, False):
                self.assertEqual(self.commit, self._commit_id(root, use_git))

    def test_no_git_directory_gives_empty_id(self):
        # A source tarball extracted inside another repository must not pick up
        # the outer repository's commit id.
        nested = self.repo / "nested"
        nested.mkdir()
        for use_git in (True, False):
            self.assertEqual("", self._commit_id(nested, use_git))

    def test_invalid_head_gives_empty_id(self):
        _git(self.repo, "checkout", "-q", "--detach")
        (self.repo / ".git" / "HEAD").write_text("not-a-sha\n", encoding="utf-8")
        self.assertEqual("", self._commit_id(self.repo, use_git=False))
