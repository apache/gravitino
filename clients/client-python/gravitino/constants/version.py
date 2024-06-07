"""
Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2.
"""

from enum import Enum
from pathlib import Path

PROJECT_HOME = Path(__file__).parent.parent.parent
VERSION_INI = PROJECT_HOME / "version.ini"
SETUP_FILE = PROJECT_HOME / "setup.py"


class Version(Enum):
    VERSION = "version"
    GIT_COMMIT = "gitCommit"
    COMPILE_DATE = "compileDate"
