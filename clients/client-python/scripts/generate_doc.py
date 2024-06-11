import pydoc
import os

from gravitino.constants.doc import DOC_DIR
from gravitino.constants.root import GRAVITNO_DIR, MODULE_NAME

if __name__ == "__main__":

    if not os.path.exists(DOC_DIR):
        # If doc folder does not exist, create a new one
        os.makedirs(DOC_DIR)

    # Change work directory to doc folder
    os.chdir(DOC_DIR)

    # Write doc for top module
    pydoc.writedoc(MODULE_NAME)

    # Write doc for submodules
    pydoc.writedocs(GRAVITNO_DIR.as_posix(), MODULE_NAME + ".")
