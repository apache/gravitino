"""
Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2.
"""

import re
import configparser
import subprocess
from datetime import datetime

from gravitino.constants.version import Version, VERSION_INI, SETUP_FILE
from gravitino.exceptions.gravitino_runtime_exception import GravitinoRuntimeException

VERSION_PATTERN = r"version\s*=\s*['\"]([^'\"]+)['\"]"


def main():
    with open(SETUP_FILE, "r", encoding="utf-8") as f:
        setup_content = f.read()
        m = re.search(VERSION_PATTERN, setup_content)
        if m is not None:
            version = m.group(1)
        else:
            raise GravitinoRuntimeException("Can't find valid version info in setup.py")

    git_commit = (
        subprocess.check_output(["git", "rev-parse", "HEAD"]).decode("ascii").strip()
    )

    compile_date = datetime.now().strftime("%d/%m/%Y %H:%M:%S")

    config = configparser.ConfigParser()
    config.optionxform = str
    config["metadata"] = {
        Version.VERSION.value: version,
        Version.GIT_COMMIT.value: git_commit,
        Version.COMPILE_DATE.value: compile_date,
    }

    with open(VERSION_INI, "w", encoding="utf-8") as f:
        config.write(f)


if __name__ == "__main__":
    main()
