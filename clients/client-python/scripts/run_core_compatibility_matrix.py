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

"""Verify installed core SDK artifacts in isolated Ray dependency environments."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import subprocess
import sys
import socket
import time
from contextlib import contextmanager, nullcontext
from dataclasses import dataclass
from email.parser import Parser
from pathlib import Path
from urllib.parse import urlsplit
from urllib.request import ProxyHandler, build_opener
from zipfile import ZipFile

CLIENT_ROOT = Path(__file__).resolve().parents[1]
CORE_TESTS = (
    "tests.unittests.test_gravitino_client",
    "tests.unittests.test_metalake",
    "tests.unittests.test_base_schema_catalog",
    "tests.unittests.test_relational_catalog",
    "tests.unittests.test_relational_table",
    "tests.unittests.dto.semantic.test_semantic_wire_compatibility",
    "tests.unittests.api.semantic.test_semantic_model_value_semantics",
    "tests.unittests.api.semantic.test_semantic_model_supporting_types",
    "tests.compatibility.test_core_metadata",
)
PROBE = """
import importlib.metadata as metadata
import json
import sys
from pathlib import Path
import gravitino
names = json.loads(sys.argv[1])
print(json.dumps({
    "python": list(sys.version_info[:2]),
    "prefix": str(Path(sys.prefix).resolve()),
    "sdk_path": str(Path(gravitino.__file__).resolve()),
    "versions": {name: metadata.version(name) for name in names},
}))
"""


@dataclass(frozen=True)
class CompatibilityCase:
    """One declared consumer environment and its exact dependency assertions."""

    name: str
    python_minor: tuple[int, int]
    simplejson: str
    arrow: str | None = None
    pandas: str | None = None

    def dependencies(self) -> dict[str, str]:
        """Return only the versions whose compatibility this case claims."""
        dependencies = {"dataclasses-json": "0.6.7", "simplejson": self.simplejson}
        if self.arrow is not None:
            if self.pandas is None:
                raise ValueError("Ray cases require an explicit pandas version")
            dependencies.update(
                {
                    "ray": "2.59.0",
                    "pyarrow": self.arrow,
                    "pandas": self.pandas,
                }
            )
        return dependencies

    def requirements(self) -> list[str]:
        """Return normal pip requirements, with Ray's Data extra when needed."""
        return [
            f"{'ray[data]' if name == 'ray' else name}=={version}"
            for name, version in self.dependencies().items()
        ]


CASES = (
    CompatibilityCase("core-min", (3, 10), "3.20.1"),
    CompatibilityCase("ray-arrow17", (3, 12), "4.2.0", "17.0.0", "2.2.3"),
    CompatibilityCase("ray-arrow23", (3, 12), "4.2.0", "23.0.1", "2.3.3"),
    CompatibilityCase("ray-arrow26", (3, 12), "4.2.0", "26.0.0", "3.0.6"),
)


def clean_environment() -> dict[str, str]:
    """Remove inherited source imports and version-check bypasses."""
    environment = os.environ.copy()
    for name in ("PYTHONPATH", "PYTHONHOME", "GRAVITINO_VERSION_CHECK_DISABLED"):
        environment.pop(name, None)
    environment["PYTHONNOUSERSITE"] = "1"
    no_proxy = environment.get("no_proxy", environment.get("NO_PROXY", ""))
    environment["NO_PROXY"] = ",".join(
        value for value in (no_proxy, "localhost", "127.0.0.1", "::1") if value
    )
    environment["no_proxy"] = environment["NO_PROXY"]
    return environment


def run_command(
    command: list[str], working_dir: Path, log: Path, environment: dict[str, str]
) -> str:
    """Run a command, preserve its output, and reject any nonzero result."""
    result = subprocess.run(
        command,
        cwd=working_dir,
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )
    with log.open("a", encoding="utf-8") as output:
        output.write(f"$ {json.dumps(command)}\n")
        output.write(result.stdout)
        output.write(result.stderr)
        output.write(f"\nexit_code={result.returncode}\n")
    if result.returncode:
        raise RuntimeError(f"Command failed with exit {result.returncode}; see {log}")
    return result.stdout


def assert_probe(
    probe: dict, case: CompatibilityCase, sdk_version: str
) -> dict[str, str]:
    """Require the declared versions and a SDK imported from this environment."""
    if tuple(probe["python"]) != case.python_minor:
        raise ValueError(
            f"Unexpected Python version for {case.name}: {probe['python']}"
        )
    sdk_path = Path(probe["sdk_path"]).resolve()
    prefix = Path(probe["prefix"]).resolve()
    if not sdk_path.is_relative_to(prefix):
        raise ValueError(f"SDK was imported outside the case environment: {sdk_path}")
    expected = {"apache-gravitino": sdk_version, **case.dependencies()}
    for name, version in expected.items():
        if probe["versions"].get(name) != version:
            raise ValueError(f"Unexpected {name} version for {case.name}")
    return probe["versions"]


def prepare_environment(interpreter: str, directory: Path) -> Path:
    """Create a dedicated venv without inheriting global site packages."""
    result = subprocess.run(
        [interpreter, "-m", "venv", str(directory)],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode:
        raise RuntimeError(f"Cannot create case environment: {result.stderr}")
    return directory / ("Scripts/python.exe" if os.name == "nt" else "bin/python")


def prepare_tests(directory: Path) -> None:
    """Copy test code only, so unittest cannot import the SDK checkout."""
    shutil.copytree(
        CLIENT_ROOT / "tests",
        directory / "tests",
        dirs_exist_ok=True,
        ignore=shutil.ignore_patterns("__pycache__", "htmlcov", ".coverage"),
    )


def install_artifact(
    python: Path,
    artifact: Path,
    requirements: list[str],
    workspace: Path,
    log: Path,
    environment: dict[str, str],
) -> None:
    """Replace even a same-version SDK while preserving installed dependencies."""
    run_command(
        [str(python), "-m", "pip", "uninstall", "-y", "apache-gravitino"],
        workspace,
        log,
        environment,
    )
    run_command(
        [str(python), "-m", "pip", "install", str(artifact), *requirements],
        workspace,
        log,
        environment,
    )


def wait_for_server(process: subprocess.Popen, uri: str, log: Path) -> None:
    """Wait for the owned foreground server, rejecting early exits and timeouts."""
    deadline = time.monotonic() + 30
    opener = build_opener(ProxyHandler({}))
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError(f"Gravitino exited before readiness; see {log}")
        try:
            with opener.open(f"{uri.rstrip('/')}/metrics", timeout=1) as response:
                if '"version"' in response.read().decode("utf-8"):
                    return
        except OSError:
            pass
        time.sleep(0.2)
    raise RuntimeError(f"Gravitino readiness timed out; see {log}")


@contextmanager
def owned_server(home: Path, uri: str, directory: Path):
    """Start and stop only this task's server, including failed startup paths."""
    address = urlsplit(uri)
    try:
        connection = socket.create_connection(
            (address.hostname, address.port or 80), timeout=1
        )
    except ConnectionRefusedError:
        pass
    else:
        connection.close()
        raise RuntimeError(f"Service endpoint is already in use: {uri}")
    log = directory / "server-console.log"
    environment = clean_environment()
    environment.update(
        {
            "GRAVITINO_HOME": str(home),
            "GRAVITINO_CONF_DIR": str(home / "conf"),
            "GRAVITINO_LOG_DIR": str(home / "logs"),
        }
    )
    with log.open("w", encoding="utf-8") as output:
        process = subprocess.Popen(
            [str(home / "bin/gravitino.sh"), "run"],
            cwd=home,
            env=environment,
            stdout=output,
            stderr=subprocess.STDOUT,
        )
        try:
            wait_for_server(process, uri, log)
            yield
        finally:
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()


def build_wheel(artifact: Path, directory: Path, interpreter: str) -> Path:
    """Build a wheel from the sdist; runtime installations still resolve normally."""
    if artifact.suffix == ".whl":
        return artifact
    builder = prepare_environment(interpreter, directory / "builder")
    wheels = directory / "artifacts"
    wheels.mkdir(exist_ok=True)
    run_command(
        [
            str(builder),
            "-m",
            "pip",
            "wheel",
            "--no-deps",
            "--wheel-dir",
            str(wheels),
            str(artifact),
        ],
        directory,
        directory / "wheel-build.log",
        clean_environment(),
    )
    candidates = list(wheels.glob("apache_gravitino-*.whl"))
    if len(candidates) != 1:
        raise ValueError("Expected exactly one built SDK wheel")
    return candidates[0]


def wheel_version(wheel: Path) -> str:
    """Read the version and verify the required version file from the wheel."""
    with ZipFile(wheel) as archive:
        metadata_paths = [
            name for name in archive.namelist() if name.endswith(".dist-info/METADATA")
        ]
        if len(metadata_paths) != 1:
            raise ValueError("Expected exactly one wheel metadata file")
        if "gravitino/version.ini" not in archive.namelist():
            raise ValueError("SDK wheel is missing gravitino/version.ini")
        metadata = Parser().parsestr(archive.read(metadata_paths[0]).decode("utf-8"))
    return metadata["Version"]


def run_case(case: CompatibilityCase, options: argparse.Namespace, wheel: Path) -> dict:
    """Verify one normally installed artifact against the real metadata service."""
    directory = options.matrix_dir / case.name
    directory.mkdir(exist_ok=True)
    log = directory / "execution.log"
    interpreter = (
        options.minimum_python if case.python_minor == (3, 10) else options.python
    )
    python = prepare_environment(interpreter, directory / "venv")
    workspace = directory / "test-workspace"
    workspace.mkdir(exist_ok=True)
    prepare_tests(workspace)
    environment = clean_environment()
    environment["GRAVITINO_CORE_COMPATIBILITY_URI"] = options.gravitino_uri
    environment["GRAVITINO_CORE_COMPATIBILITY_SOURCE"] = str(CLIENT_ROOT)
    # The minimum case installs the original sdist; other cases install its wheel.
    artifact = options.artifact if case.name == "core-min" else wheel
    install_artifact(python, artifact, case.requirements(), workspace, log, environment)
    run_command([str(python), "-m", "pip", "check"], workspace, log, environment)
    sdk_version = wheel_version(wheel)
    package_names = ["apache-gravitino", *case.dependencies()]
    output = run_command(
        [str(python), "-c", PROBE, json.dumps(package_names)],
        workspace,
        log,
        environment,
    )
    versions = assert_probe(json.loads(output), case, sdk_version)
    if case.arrow is not None:
        run_command(
            [str(python), "-c", "import ray.data; import gravitino"],
            workspace,
            log,
            environment,
        )
    else:
        run_command(
            [
                str(python),
                "-c",
                "import importlib.util; import gravitino; "
                "assert importlib.util.find_spec('fsspec') is None; "
                "assert importlib.util.find_spec('pyarrow') is None",
            ],
            workspace,
            log,
            environment,
        )
    run_command(
        [str(python), "-m", "unittest", "-v", *CORE_TESTS],
        workspace,
        log,
        environment,
    )
    resolved = run_command(
        [str(python), "-m", "pip", "freeze"], workspace, log, environment
    )
    (directory / "resolved-requirements.txt").write_text(resolved, encoding="utf-8")
    return {
        "case": case.name,
        "status": "passed",
        "versions": versions,
        "log": str(log),
    }


def parse_args() -> argparse.Namespace:
    """Parse an explicit artifact, matching service, and bounded case selection."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--artifact", required=True, type=Path)
    parser.add_argument("--gravitino-uri", required=True)
    parser.add_argument("--gravitino-home", type=Path)
    parser.add_argument("--python", default=sys.executable)
    parser.add_argument("--minimum-python", default="python3.10")
    parser.add_argument(
        "--matrix-dir", type=Path, default=CLIENT_ROOT / "build/core-compatibility"
    )
    parser.add_argument("--cases", default=",".join(case.name for case in CASES))
    options = parser.parse_args()
    options.artifact = options.artifact.resolve()
    options.matrix_dir = options.matrix_dir.resolve()
    if not options.artifact.is_file():
        parser.error("The SDK artifact must exist")
    uri = urlsplit(options.gravitino_uri)
    if uri.scheme not in {"http", "https"} or not uri.hostname:
        parser.error("Use an HTTP or HTTPS service URI")
    if any((uri.username, uri.password, uri.query, uri.fragment)):
        parser.error(
            "Use a service URI without embedded credentials or query parameters"
        )
    if options.gravitino_home is not None:
        options.gravitino_home = options.gravitino_home.resolve()
        if uri.scheme != "http" or uri.hostname not in {
            "localhost",
            "127.0.0.1",
            "::1",
        }:
            parser.error("An owned server requires a local HTTP endpoint")
        if not (options.gravitino_home / "bin/gravitino.sh").is_file():
            parser.error("The server distribution must contain bin/gravitino.sh")
    selected = options.cases.split(",")
    known = {case.name for case in CASES}
    if len(selected) != len(set(selected)) or not set(selected).issubset(known):
        parser.error("Cases must be distinct declared case names")
    options.selected = selected
    return options


def main() -> int:
    """Run each selected case once and retain a machine-readable result."""
    options = parse_args()
    options.matrix_dir.mkdir(parents=True, exist_ok=True)
    report = {
        "artifact": str(options.artifact),
        "artifact_sha256": hashlib.sha256(options.artifact.read_bytes()).hexdigest(),
        "results": [],
    }
    (options.matrix_dir / "results.json").write_text(
        json.dumps(report, indent=2), encoding="utf-8"
    )
    wheel = build_wheel(options.artifact, options.matrix_dir, options.python)
    report["wheel_sha256"] = hashlib.sha256(wheel.read_bytes()).hexdigest()
    (options.matrix_dir / "results.json").write_text(
        json.dumps(report, indent=2), encoding="utf-8"
    )
    service = (
        owned_server(options.gravitino_home, options.gravitino_uri, options.matrix_dir)
        if options.gravitino_home is not None
        else nullcontext()
    )
    with service:
        for case in CASES:
            if case.name not in options.selected:
                continue
            try:
                result = run_case(case, options, wheel)
            except (OSError, RuntimeError, ValueError) as error:
                result = {"case": case.name, "status": "failed", "error": str(error)}
            report["results"].append(result)
            (options.matrix_dir / "results.json").write_text(
                json.dumps(report, indent=2), encoding="utf-8"
            )
            print(json.dumps(result), flush=True)
            if result["status"] != "passed":
                return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
