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
    """ This data operation means that create a new file.
    """

    OPEN = "OPEN"
    """This data operation means that open a file.
    """

    APPEND = "APPEND"
    """This data operation means that append some content into a file.
    """

    RENAME = "RENAME"
    """This data operation means that rename a file or a directory.
    """

    DELETE = "DELETE"
    """This data operation means that delete a file or a directory.
    """

    GET_FILE_STATUS = "GET_FILE_STATUS"
    """This data operation means that get a file status from a file or a directory.
    """

    LIST_STATUS = "LIST_STATUS"
    """This data operation means that list file statuses under a directory.
    """

    MKDIRS = "MKDIRS"
    """This data operation means that create a directory.
    """

    EXISTS = "EXISTS"
    """This data operation means that check if a file or a directory exists.
    """

    CREATED_TIME = "CREATED_TIME"
    """This data operation means that get the created time of a file.
    """

    MODIFIED_TIME = "MODIFIED_TIME"
    """This data operation means that get the modified time of a file.
    """

    COPY_FILE = "COPY_FILE"
    """This data operation means that copy a file.
    """

    CAT_FILE = "CAT_FILE"
    """This data operation means that get the content of a file.
    """

    GET_FILE = "GET_FILE"
    """This data operation means that copy a remote file to local.
    """

    UNKNOWN = "UNKNOWN"
    """This data operation means that it is an unknown data operation.
    """
