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
    """Creates a new file.
    """

    OPEN = "OPEN"
    """Opens a file.
    """

    OPEN_AND_WRITE = "OPEN_AND_WRITE"
    """Opens a file and writes to it.
    """

    OPEN_AND_APPEND = "OPEN_AND_APPEND"
    """Opens a file and appends to it.
    """

    APPEND = "APPEND"
    """Appends some content into a file.
    """

    RENAME = "RENAME"
    """Renames a file or a directory.
    """

    DELETE = "DELETE"
    """Deletes a file or a directory.
    """

    GET_FILE_STATUS = "GET_FILE_STATUS"
    """Gets a file status from a file or a directory.
    """

    LIST_STATUS = "LIST_STATUS"
    """Lists file statuses under a directory.
    """

    MKDIRS = "MKDIRS"
    """Creates a directory.
    """

    EXISTS = "EXISTS"
    """Checks if a file or a directory exists.
    """

    CREATED_TIME = "CREATED_TIME"
    """Gets the created time of a file.
    """

    MODIFIED_TIME = "MODIFIED_TIME"
    """Gets the modified time of a file.
    """

    COPY_FILE = "COPY_FILE"
    """Copies a file.
    """

    CAT_FILE = "CAT_FILE"
    """Gets the content of a file.
    """

    GET_FILE = "GET_FILE"
    """Copies a remote file to local.
    """

    UNKNOWN = "UNKNOWN"
    """Unknown data operation.
    """
