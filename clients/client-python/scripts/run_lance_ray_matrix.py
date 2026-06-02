#!/usr/bin/env python3
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

"""Run ``tests/integration/test_lance_ray.py`` against multiple lance-ray
versions to validate the supported range advertised in the Compatibility
Matrix (``docs/lance-rest-integration.md``).

For each version we provision a dedicated venv under
``clients/client-python/build/lance-ray-matrix/.venv-<version>/`` and install
``ray``, ``lance-ray==<version>``, ``lance-namespace``, ``requests``, plus the
in-tree ``apache-gravitino`` distribution (editable). The unittest itself is
launched per-version with ``python -m unittest -v
tests.integration.test_lance_ray``; results are collected into a pass/fail
table at the end.

The caller is responsible for starting the Gravitino server (with the
auxiliary lance-rest service enabled). The Gradle wrapper task
``:clients:client-python:lanceRayMatrixTest`` handles that. For ad-hoc local
use::

    distribution/package/bin/gravitino.sh start
    python3 clients/client-python/scripts/run_lance_ray_matrix.py \
        --versions 0.4.2,0.3.0 \
        --gravitino-home distribution/package
    distribution/package/bin/gravitino.sh stop

Each test class will append its own metalake binding to ``gravitino.conf`` and
restart the server itself. The matrix runner opts into keeping that binding
between versions, so back-to-back runs avoid unnecessary Gravitino restarts.
"""

from __future__ import annotations

import argparse
import hashlib
import os
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import List

# Default version set tracks what the Compatibility Matrix in
# docs/lance-rest-integration.md claims to support. Keep these in sync.
DEFAULT_VERSIONS = ["0.4.2", "0.3.0"]


REPO_ROOT = Path(__file__).resolve().parents[3]
PYTHON_CLIENT_DIR = REPO_ROOT / "clients" / "client-python"
DEFAULT_MATRIX_DIR = PYTHON_CLIENT_DIR / "build" / "lance-ray-matrix"
DEFAULT_GRAVITINO_HOME = REPO_ROOT / "distribution" / "package"
LANCE_REST_CONF = Path("conf") / "gravitino.conf"
LANCE_REST_METALAKE_BINDING = (
    "gravitino.lance-rest.gravitino-metalake = lance_ray_test_metalake"
)


@dataclass
class VersionResult:
    version: str
    status: str  # "ok", "fail", "setup-error"
    details: str


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument(
        "--versions",
        default=",".join(DEFAULT_VERSIONS),
        help="Comma-separated list of lance-ray versions to test. "
        f"Default: {','.join(DEFAULT_VERSIONS)}",
    )
    p.add_argument(
        "--python",
        default=sys.executable,
        help="Path to the host python interpreter used to bootstrap each "
        "version's venv. Default: %(default)s",
    )
    p.add_argument(
        "--matrix-dir",
        default=str(DEFAULT_MATRIX_DIR),
        help="Directory under which per-version venvs are created and "
        "cached across runs. Default: %(default)s",
    )
    p.add_argument(
        "--gravitino-home",
        default=str(DEFAULT_GRAVITINO_HOME),
        help="Path to the built Gravitino distribution package. The "
        "lance-rest aux service must be enabled there. Default: %(default)s",
    )
    p.add_argument(
        "--ray-spec",
        default="ray==2.55.1",
        help="Pip spec for ray. Pinned by default so the matrix is "
        "reproducible across runs and does not drift with PyPI. Override "
        "(e.g. 'ray') to let pip pick a compatible version per lance-ray. "
        "Default: %(default)s",
    )
    p.add_argument(
        "--lance-namespace-spec",
        default="lance-namespace==0.7.5",
        help="Pip spec for lance-namespace. Pinned by default so the "
        "matrix is reproducible and does not drift with PyPI. Override "
        "(e.g. 'lance-namespace') for latest. Default: %(default)s",
    )
    p.add_argument(
        "--keep-going",
        action="store_true",
        help="Continue to the next version after a failure instead of "
        "stopping on the first failed run.",
    )
    return p.parse_args()


def run(cmd: List[str], **kwargs) -> subprocess.CompletedProcess:
    print(f"[matrix] $ {' '.join(cmd)}")
    return subprocess.run(cmd, check=False, **kwargs)


def ensure_venv(python: str, venv_dir: Path) -> Path:
    """Create the venv if it doesn't already exist. Returns the venv python path."""
    venv_python = venv_dir / "bin" / "python"
    if not venv_python.exists():
        venv_dir.parent.mkdir(parents=True, exist_ok=True)
        rc = run([python, "-m", "venv", str(venv_dir)]).returncode
        if rc != 0:
            raise RuntimeError(f"Failed to create venv at {venv_dir}")
    return venv_python


def _deps_sentinel(venv_dir: Path, version: str, ray_spec: str, lance_namespace_spec: str) -> Path:
    """Return the path to the sentinel file that marks a fully-installed venv.

    The sentinel encodes all dep specs so any change in pinned versions forces
    a reinstall, while an identical set of specs skips all pip invocations.
    """
    key = f"{version}|{ray_spec}|{lance_namespace_spec}"
    digest = hashlib.sha1(key.encode(), usedforsecurity=False).hexdigest()[:12]
    return venv_dir / f".deps-installed-{digest}"


def install_deps(
    venv_python: Path,
    venv_dir: Path,
    version: str,
    ray_spec: str,
    lance_namespace_spec: str,
) -> None:
    """Install all deps into the venv.  Skips every pip call when a sentinel
    file proves the identical set of packages was already installed — this
    makes repeated matrix runs fast when the venv directory is cached (e.g.
    in CI artifact caches or local re-runs).
    """
    sentinel = _deps_sentinel(venv_dir, version, ray_spec, lance_namespace_spec)
    if sentinel.exists():
        print(f"[matrix] venv for lance-ray=={version} already populated, skipping pip install")
        return

    rc = run(
        [
            str(venv_python),
            "-m",
            "pip",
            "install",
            "--upgrade",
            "pip",
            "wheel",
        ]
    ).returncode
    if rc != 0:
        raise RuntimeError("pip upgrade failed in venv")

    rc = run(
        [
            str(venv_python),
            "-m",
            "pip",
            "install",
            ray_spec,
            f"lance-ray=={version}",
            lance_namespace_spec,
            "requests",
        ]
    ).returncode
    if rc != 0:
        raise RuntimeError(f"Failed to install lance-ray=={version} deps")

    rc = run(
        [
            str(venv_python),
            "-m",
            "pip",
            "install",
            "-e",
            str(PYTHON_CLIENT_DIR),
        ]
    ).returncode
    if rc != 0:
        raise RuntimeError("Failed to install apache-gravitino in editable mode")

    sentinel.touch()


def generate_version_ini(venv_python: Path) -> None:
    # The python client reads gravitino/version.ini at runtime. It is
    # produced by scripts/generate_version.py and is gitignored, so we
    # regenerate it here to make the matrix runnable on fresh checkouts.
    script = PYTHON_CLIENT_DIR / "scripts" / "generate_version.py"
    rc = run(
        [str(venv_python), str(script)],
        cwd=str(PYTHON_CLIENT_DIR),
    ).returncode
    if rc != 0:
        raise RuntimeError("Failed to generate version.ini for python client")


def run_unittest(venv_python: Path, gravitino_home: Path) -> int:
    env = os.environ.copy()
    env["PYTHONPATH"] = str(PYTHON_CLIENT_DIR)
    env["GRAVITINO_HOME"] = str(gravitino_home)
    env["START_EXTERNAL_GRAVITINO"] = "true"
    env["LANCE_RAY_KEEP_GRAVITINO_CONF"] = "true"
    cmd = [
        str(venv_python),
        "-m",
        "unittest",
        "-v",
        "tests.integration.test_lance_ray",
    ]
    print(f"[matrix] $ PYTHONPATH=... GRAVITINO_HOME=... {' '.join(cmd)}")
    return subprocess.run(
        cmd, cwd=str(PYTHON_CLIENT_DIR), env=env, check=False
    ).returncode


def count_lance_rest_binding(conf_path: Path) -> int:
    if not conf_path.exists():
        return 0
    with conf_path.open(encoding="utf-8") as file:
        return sum(1 for line in file if line.strip() == LANCE_REST_METALAKE_BINDING)


def restore_lance_rest_binding_count(conf_path: Path, original_count: int) -> None:
    if not conf_path.exists():
        return

    lines = conf_path.read_text(encoding="utf-8").splitlines(keepends=True)
    current_count = sum(
        1 for line in lines if line.strip() == LANCE_REST_METALAKE_BINDING
    )
    surplus_count = current_count - original_count
    if surplus_count <= 0:
        return

    filtered_lines = []
    removed_count = 0
    for line in reversed(lines):
        if (
            removed_count < surplus_count
            and line.strip() == LANCE_REST_METALAKE_BINDING
        ):
            removed_count += 1
            continue
        filtered_lines.append(line)

    conf_path.write_text("".join(reversed(filtered_lines)), encoding="utf-8")
    print(
        "[matrix] removed "
        f"{removed_count} lance-rest binding line(s) from {conf_path}"
    )


def main() -> int:
    args = parse_args()
    versions = [v.strip() for v in args.versions.split(",") if v.strip()]
    if not versions:
        print("--versions must contain at least one entry", file=sys.stderr)
        return 2

    matrix_dir = Path(args.matrix_dir).resolve()
    gravitino_home = Path(args.gravitino_home).resolve()
    if not (gravitino_home / "bin" / "gravitino.sh").exists():
        print(
            f"GRAVITINO_HOME={gravitino_home} does not look like a "
            "Gravitino distribution package (missing bin/gravitino.sh). "
            "Run `./gradlew compileDistribution -PskipWeb=true -x test` first.",
            file=sys.stderr,
        )
        return 2

    conf_path = gravitino_home / LANCE_REST_CONF
    original_binding_count = count_lance_rest_binding(conf_path)
    results: List[VersionResult] = []
    try:
        for version in versions:
            print(f"\n========== lance-ray=={version} ==========")
            venv_dir = matrix_dir / f".venv-{version}"
            try:
                venv_python = ensure_venv(args.python, venv_dir)
                install_deps(
                    venv_python,
                    venv_dir,
                    version,
                    args.ray_spec,
                    args.lance_namespace_spec,
                )
                generate_version_ini(venv_python)
            except Exception as e:  # pylint: disable=broad-exception-caught
                print(f"[matrix] {version}: setup failed: {e}", file=sys.stderr)
                results.append(VersionResult(version, "setup-error", str(e)))
                if not args.keep_going:
                    break
                continue

            rc = run_unittest(venv_python, gravitino_home)
            if rc == 0:
                print(f"[matrix] {version}: PASS")
                results.append(VersionResult(version, "ok", "tests passed"))
            else:
                print(f"[matrix] {version}: FAIL (exit={rc})")
                results.append(VersionResult(version, "fail", f"unittest exit {rc}"))
                if not args.keep_going:
                    break
    finally:
        restore_lance_rest_binding_count(conf_path, original_binding_count)

    print("\n========== summary ==========")
    width = max(len(r.version) for r in results) if results else 0
    for r in results:
        print(f"  lance-ray=={r.version.ljust(width)}  {r.status:11s}  {r.details}")

    any_fail = any(r.status != "ok" for r in results)
    return 1 if any_fail else 0


if __name__ == "__main__":
    sys.exit(main())
