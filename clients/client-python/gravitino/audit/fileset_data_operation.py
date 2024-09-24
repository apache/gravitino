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
from enum import Enum


class FilesetDataOperation(Enum):
    """An enum class containing fileset data operations that supported."""

    CREATE = "CREATE"
    """ Create a new file.
    """

    OPEN = "OPEN"
    """Open a file.
    """

    APPEND = "APPEND"
    """Append some content into a file.
    """

    RENAME = "RENAME"
    """Rename a file or a directory.
    """

    DELETE = "DELETE"
    """Delete a file or a directory.
    """

    GET_FILE_STATUS = "GET_FILE_STATUS"
    """Get a file status from a file or a directory.
    """

    LIST_STATUS = "LIST_STATUS"
    """List file statuses under a directory.
    """

    MKDIRS = "MKDIRS"
    """Create a directory.
    """

    EXISTS = "EXISTS"
    """Check if a file or a directory exists.
    """

    CREATED_TIME = "CREATED_TIME"
    """Get the created time of a file.
    """

    MODIFIED_TIME = "MODIFIED_TIME"
    """Get the modified time of a file.
    """

    COPY_FILE = "COPY_FILE"
    """Copy a file.
    """

    CAT_FILE = "CAT_FILE"
    """Get the content of a file.
    """

    GET_FILE = "GET_FILE"
    """Copy a remote file to local.
    """

    UNKNOWN = "UNKNOWN"
    """Unknown data operation.
    """
