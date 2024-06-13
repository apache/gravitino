"""
Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2.
"""

import pydoc
import os
import shutil

from gravitino.constants.doc import DOC_DIR
from gravitino.constants.root import GRAVITNO_DIR, MODULE_NAME

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
    pydoc.writedocs(GRAVITNO_DIR.as_posix(), MODULE_NAME + ".")
