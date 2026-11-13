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

# coding=utf-8

import re
import configparser
import subprocess
from datetime import datetime
from pathlib import Path
from gravitino.constants.root import PROJECT_ROOT

from gravitino.constants.version import Version, VERSION_INI, SETUP_FILE
from gravitino.exceptions.base import GravitinoRuntimeException

VERSION_PATTERN = r"version\s*=\s*['\"]([^'\"]+)['\"]"


COMMIT_ID_PATTERN = re.compile(r"[0-9a-fA-F]{40}")


def _read_commit_id_from_git():
    """Ask git itself, which handles worktrees, packed refs and detached HEAD.

    Only trusted when PROJECT_ROOT has its own .git, so a source tarball extracted
    inside another repository does not pick up that repository's commit id.
    """
    if not (Path(PROJECT_ROOT) / ".git").exists():
        return ""
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=PROJECT_ROOT,
            capture_output=True,
            text=True,
            check=False,
            timeout=30,
        )
    except (OSError, subprocess.SubprocessError):
        return ""
    output = result.stdout.strip()
    if result.returncode == 0 and COMMIT_ID_PATTERN.fullmatch(output):
        return output
    return ""


def _read_commit_id_from_files():
    """Fallback that parses the .git directory by hand when git is unavailable."""
    try:
        git_path = Path(PROJECT_ROOT) / ".git"
        if git_path.is_file():
            # Linked worktrees have a ".git" file holding a "gitdir:" path
            # (absolute, or relative to the root).
            content = git_path.read_text(encoding="utf-8").strip()
            gitdir = content.split("gitdir:", 1)[1].strip()
            git_dir = (Path(PROJECT_ROOT) / gitdir).resolve()
        else:
            git_dir = git_path

        head = (git_dir / "HEAD").read_text(encoding="utf-8").strip()
        if not head.startswith("ref:"):
            return head

        ref_name = head.split(":", 1)[1].strip()
        # Branch refs of linked worktrees live in the common git dir.
        search_dirs = [git_dir]
        common_dir = git_dir / "commondir"
        if common_dir.is_file():
            common = common_dir.read_text(encoding="utf-8").strip()
            search_dirs.append((git_dir / common).resolve())

        for folder in search_dirs:
            ref_file = folder / ref_name
            if ref_file.is_file():
                return ref_file.read_text(encoding="utf-8").strip()
            packed = folder / "packed-refs"
            if packed.is_file():
                for line in packed.read_text(encoding="utf-8").splitlines():
                    if line.endswith(" " + ref_name):
                        return line.split(" ", 1)[0].strip()
        return ""
    except (OSError, IndexError):
        return ""


def get_git_commit_id():
    commit_id = _read_commit_id_from_git() or _read_commit_id_from_files()
    return commit_id if COMMIT_ID_PATTERN.fullmatch(commit_id) else ""


def main():
    with open(SETUP_FILE, "r", encoding="utf-8") as f:
        setup_content = f.read()
        m = re.search(VERSION_PATTERN, setup_content)
        if m is not None:
            version = m.group(1)
        else:
            raise GravitinoRuntimeException("Can't find valid version info in setup.py")

    git_commit = get_git_commit_id()

    compile_date = datetime.now().strftime("%d/%m/%Y %H:%M:%S")

    config = configparser.ConfigParser()
    config.optionxform = str
    config["metadata"] = {
        Version.VERSION.value: version,
        Version.GIT_COMMIT.value: git_commit,
        Version.COMPILE_DATE.value: compile_date,
    }

    license_header = [
        "# Licensed to the Apache Software Foundation (ASF) under one\n",
        "# or more contributor license agreements.  See the NOTICE file\n",
        "# distributed with this work for additional information\n",
        "# regarding copyright ownership.  The ASF licenses this file\n",
        "# to you under the Apache License, Version 2.0 (the\n",
        '# "License"); you may not use this file except in compliance\n',
        "# with the License.  You may obtain a copy of the License at\n",
        "#\n",
        "#   http://www.apache.org/licenses/LICENSE-2.0\n",
        "#\n",
        "# Unless required by applicable law or agreed to in writing,\n",
        "# software distributed under the License is distributed on an\n",
        '# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY\n',
        "# KIND, either express or implied.  See the License for the\n",
        "# specific language governing permissions and limitations\n",
        "# under the License.\n",
    ]

    with open(VERSION_INI, "w", encoding="utf-8") as f:
        f.writelines(license_header)
        config.write(f)


if __name__ == "__main__":
    main()
