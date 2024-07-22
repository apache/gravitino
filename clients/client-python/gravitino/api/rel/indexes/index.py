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

from abc import ABC, abstractmethod
from enum import Enum, unique

@unique
class IndexType(Enum):
    """
    Defines the type of the index, supporting PRIMARY_KEY and UNIQUE_KEY.
    """

    PRIMARY_KEY = "PRIMARY_KEY"
    """
    PRIMARY KEY index in a relational database is a field or a combination of fields that
    uniquely identifies each record in a table. It serves as a unique identifier for each row,
    ensuring that no two rows have the same key. The PRIMARY KEY is used to establish
    relationships between tables and enforce the entity integrity of a database. Additionally, it
    helps in indexing and organizing the data for efficient retrieval and maintenance.
    """

    UNIQUE_KEY = "UNIQUE_KEY"
    """
    UNIQUE KEY in a relational database is a field or a combination of fields that ensures each
    record in a table has a distinct value or combination of values. Unlike a primary key, a
    UNIQUE KEY allows for the presence of null values, but it still enforces the constraint that
    no two records can have the same unique key value(s). UNIQUE KEYs are used to maintain data
    integrity by preventing duplicate entries in specific columns, and they can be applied to
    columns that are not designated as the primary key. The uniqueness constraint imposed by
    UNIQUE KEY helps in avoiding redundancy and ensuring data accuracy in the database.
    """

class Index(ABC):
    """
    The Index class defines methods for implementing table index columns. Currently, settings for
    PRIMARY_KEY and UNIQUE_KEY are provided.
    """

    @abstractmethod
    def type(self):
        """
        Returns the type of the index, e.g., PRIMARY_KEY and UNIQUE_KEY.
        """
        pass

    @abstractmethod
    def name(self):
        """
        Returns the name of the index.
        """
        pass

    @abstractmethod
    def field_names(self):
        """
        Returns the field names under the table contained in the index.
        This could be "a.b.c" for nested columns, but normally it would just be "a".
        """
        pass

