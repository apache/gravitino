import os
import pydoc

from gravitino.constants.doc import DOC_DIR, EXCLUDE_FILES, EXCLUDE_DIRS
from gravitino.constants.root import GRAVITNO_DIR, PROJECT_HOME

if __name__ == "__main__":

    if not os.path.exists(DOC_DIR):
        # If doc folder does not exist, create a new one
        os.makedirs(DOC_DIR)

    # Change work directory to doc folder
    os.chdir(DOC_DIR)

    # Traverse the project and generate docs
    for dirpath, _, filenames in os.walk(GRAVITNO_DIR):

        # Exclude some unwanted directories
        if dirpath.split(os.sep)[-1] not in EXCLUDE_DIRS:

            # Trim the root path
            dirpath = dirpath.replace(PROJECT_HOME.as_posix() + os.sep, "")

            # Convert directory path to module path
            top_module_name = dirpath.replace(os.sep, ".")
            pydoc.writedoc(top_module_name)

            for filename in filenames:

                # Exclude some unwanted files
                if filename.endswith(".py") and filename not in EXCLUDE_FILES:

                    # Convert file path to module path
                    module_name = top_module_name + "." + filename[:-3]
                    pydoc.writedoc(module_name)
