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

import pydoc
import os
import shutil

from gravitino.constants.doc import DOC_DIR
from gravitino.constants.root import GRAVITINO_DIR, MODULE_NAME, PROJECT_ROOT

if __name__ == "__main__":

    if os.path.exists(DOC_DIR):
        # If doc folder exists, delete it
        shutil.rmtree(DOC_DIR)

    # Create a new doc folder
    os.makedirs(DOC_DIR)
    # Change work directory to doc folder
    os.chdir(DOC_DIR)

    # Write doc for top module
    pydoc.writedoc(MODULE_NAME)

    # Write doc for submodules
    pydoc.writedocs(GRAVITINO_DIR.as_posix(), MODULE_NAME + ".")

    # Remove compile path in the python client docs
    for root, _, files in os.walk(DOC_DIR):
        for file in files:
            file_path = os.path.join(root, file)
            with open(file_path, "r", encoding="utf-8") as f:
                content = f.read()
            content = content.replace(
                f"PosixPath('{PROJECT_ROOT.parent}", "PosixPath('"
            )
            with open(file_path, "w", encoding="utf-8") as f:
                f.write(content)
