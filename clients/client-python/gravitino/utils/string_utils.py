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


class StringUtils:
    @classmethod
    def is_blank(cls, s: str) -> bool:
        """Checks if a string is blank (null, empty, or only whitespace).

        Args:
            s: The string to check.

        Returns:
            True if the string is blank, False otherwise.
        """
        return s is None or s.strip() == ""

    @classmethod
    def is_not_blank(cls, s: str) -> bool:
        """
        Checks if a string is not blank (not null, not empty, and not only whitespace).

        Args:
            s (str): The string to check.

        Returns:
            bool: True if the string is not blank, False otherwise.
        """
        return not cls.is_blank(s)
