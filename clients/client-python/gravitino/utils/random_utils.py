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


from __future__ import annotations

import random
import string


class RandomStringUtils:
    @staticmethod
    def random_string(length: int = 8) -> str:
        """
        Generate a random string of specified length.

        Args:
            length (int, optional): The length of string. Defaults to 8.

        Returns:
            str: The random string.
        """
        chars = string.ascii_letters + string.digits
        return "".join(random.choices(chars, k=length))
