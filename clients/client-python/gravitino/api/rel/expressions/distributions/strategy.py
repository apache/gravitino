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


class Strategy(Enum):
    """
    An enum that defines the distribution strategy.

    The following strategies are supported:

    - NONE: No distribution strategy, depends on the underlying system's allocation.
    - HASH: Uses the hash value of the expression to distribute data.
    - RANGE: Uses the specified range of the expression to distribute data.
    - EVEN: Distributes data evenly across partitions.
    """

    NONE = "NONE"
    HASH = "HASH"
    RANGE = "RANGE"
    EVEN = "EVEN"

    @staticmethod
    def get_by_name(name: str) -> "Strategy":
        upper_name = name.upper()
        if upper_name == "NONE":
            return Strategy.NONE
        elif upper_name == "HASH":
            return Strategy.HASH
        elif upper_name == "RANGE":
            return Strategy.RANGE
        elif upper_name in {"EVEN", "RANDOM"}:
            return Strategy.EVEN
        else:
            raise ValueError(
                f"Invalid distribution strategy: {name}. Valid values are: {[s.value for s in Strategy]}"
            )
