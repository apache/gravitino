"""
Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2.
"""

from enum import Enum

from gravitino.constants.root import PROJECT_HOME
from gravitino.constants.root import MODULE_NAME

VERSION_INI = PROJECT_HOME / MODULE_NAME / "version.ini"
SETUP_FILE = PROJECT_HOME / "setup.py"


class Version(Enum):
    VERSION = "version"
    GIT_COMMIT = "gitCommit"
    COMPILE_DATE = "compileDate"
