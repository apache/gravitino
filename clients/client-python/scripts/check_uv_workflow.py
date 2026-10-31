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

"""Check the uv lock, package metadata and cache behavior in isolated copies."""

import json
import os
import shutil
import subprocess
import sys
import tempfile
from email.parser import Parser
from pathlib import Path


def _require(condition, message):
    if not condition:
        raise RuntimeError(message)


def _run(command, cwd, environment):
    return subprocess.run(
        command,
        cwd=cwd,
        env=environment,
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    ).stdout


def _copy_source(root, destination):
    return shutil.copytree(
        root,
        destination,
        ignore=shutil.ignore_patterns(
            ".venv*",
            "build",
            "dist",
            "*.egg-info",
            "__pycache__",
            "*.pyc",
            "htmlcov",
            ".coverage",
            "gravitino-uv-check-*",
        ),
    )


def _metadata(source, environment):
    egg_base = source.parent / (source.name + "-metadata")
    egg_base.mkdir()
    _run(
        [sys.executable, "setup.py", "egg_info", "--egg-base", str(egg_base)],
        source,
        environment,
    )
    files = list(egg_base.glob("*.egg-info/PKG-INFO"))
    _require(len(files) == 1, "Expected one package metadata file")
    metadata = Parser().parsestr(files[0].read_text(encoding="utf-8"))
    return sorted(metadata.items()), metadata.get_payload()


def _installed_metadata(python, workspace, environment):
    # Query installed dist-info outside the source tree, not its egg-info.
    probe = (
        "import importlib.metadata as m, json; "
        "d = m.metadata('apache-gravitino'); "
        "print(json.dumps({'description': d.get_payload().strip(), "
        "'requirements': sorted(d.get_all('Requires-Dist', []))}))"
    )
    return json.loads(_run([str(python), "-c", probe], workspace, environment))


def _check_cache(source, workspace, environment):
    environment = dict(environment, UV_PROJECT_ENVIRONMENT=str(workspace / "venv"))
    python = (
        workspace / "venv" / ("Scripts/python.exe" if os.name == "nt" else "bin/python")
    )

    def sync():
        _run(
            [
                "uv",
                "sync",
                "--project",
                str(source),
                "--locked",
                "--no-dev",
                "--python",
                sys.executable,
            ],
            source,
            environment,
        )
        return _installed_metadata(python, workspace, environment)

    readme = source / "README.md"
    readme.write_text("uv cache check: README A\n", encoding="utf-8")
    before = sync()
    _require(
        before["description"] == "uv cache check: README A", "README A not installed"
    )
    readme.write_text("uv cache check: README B\n", encoding="utf-8")
    after = sync()
    _require(
        after["description"] == "uv cache check: README B", "Stale README metadata"
    )
    _require(
        after["requirements"] == before["requirements"], "README changed requirements"
    )
    print("PASS: README metadata cache invalidation", flush=True)

    requirements = source / "requirements.txt"
    original = requirements.read_text(encoding="utf-8")
    _require(
        "packaging" not in before["requirements"], "Cache probe is already declared"
    )
    for content, expected in (
        (original + "\npackaging\n", sorted(before["requirements"] + ["packaging"])),
        (original, before["requirements"]),
    ):
        requirements.write_text(content, encoding="utf-8")
        _run(["uv", "lock", "--project", str(source)], source, environment)
        _require(sync()["requirements"] == expected, "Stale requirements metadata")
    _run(["uv", "pip", "check", "--python", str(python)], workspace, environment)
    print("PASS: requirement metadata cache invalidation", flush=True)


def main():
    """Run the checks without changing checkout inputs or clearing caches."""
    root = Path(__file__).resolve().parents[1]
    environment = os.environ.copy()
    # These checks must exercise cache reuse even if the caller disables it.
    environment["UV_NO_CACHE"] = "false"
    for name in (
        "PYTHONPATH",
        "UV_PROJECT_ENVIRONMENT",
        "UV_LOCKED",
        "UV_FROZEN",
        "UV_NO_SYNC",
    ):
        environment.pop(name, None)
    _require(shutil.which("uv") is not None, "Install uv before running this check")
    lock = (root / "uv.lock").read_bytes()
    _run(["uv", "lock", "--project", str(root), "--check"], root, environment)
    print("PASS: uv lock consistency", flush=True)
    with tempfile.TemporaryDirectory(prefix="gravitino-uv-check-") as directory:
        workspace = Path(directory)
        legacy = _copy_source(root, workspace / "legacy")
        bridged = _copy_source(root, workspace / "bridged")
        (legacy / "pyproject.toml").rename(legacy / "pyproject.reference.toml")
        _require(
            _metadata(legacy, environment) == _metadata(bridged, environment),
            "setup.py metadata differs from the pyproject bridge",
        )
        print("PASS: setup.py and pyproject metadata equivalence", flush=True)
        _check_cache(bridged, workspace, environment)
    _require((root / "uv.lock").read_bytes() == lock, "Checkout lockfile was modified")


if __name__ == "__main__":
    try:
        main()
    except subprocess.CalledProcessError as error:
        print(error.output, file=sys.stderr, end="")
        sys.exit(f"uv workflow check failed: {error}")
    except RuntimeError as error:
        sys.exit(f"uv workflow check failed: {error}")
