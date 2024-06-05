from enum import Enum
from pathlib import Path

VERSION_INI = Path(__file__).parent.parent.parent / "version.ini"


class Version(Enum):
    VERSION = "version"
    GIT_COMMIT = "gitCommit"
    COMPILE_DATE = "compileDate"
