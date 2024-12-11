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

from gravitino.exceptions.base import IllegalArgumentException


class Precondition:
    @staticmethod
    def check_argument(expression_result: bool, error_message: str):
        """Ensures the truth of an expression involving one or more parameters
        to the calling method.

        Args:
          expression_result: A boolean expression.
          error_message: The error message to use if the check fails.
        Raises:
          IllegalArgumentException – if expression is false
        """
        if not expression_result:
            raise IllegalArgumentException(error_message)

    @staticmethod
    def check_string_not_empty(check_string: str, error_message: str):
        """Ensures the string is not empty.

        Args:
          check_string: The string to check.
          error_message: The error message to use if the check fails.
        Raises:
          IllegalArgumentException – if the check fails.
        """
        Precondition.check_argument(
            check_string is not None and check_string.strip() != "", error_message
        )
