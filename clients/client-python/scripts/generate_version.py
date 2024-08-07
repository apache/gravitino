"""
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
"""

import re
import configparser
import subprocess
from datetime import datetime

from gravitino.constants.version import Version, VERSION_INI, SETUP_FILE
from gravitino.exceptions.base import GravitinoRuntimeException

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
