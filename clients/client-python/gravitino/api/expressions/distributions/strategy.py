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


# Enum equivalent in Python for Strategy
class Strategy:
    NONE = "NONE"
    HASH = "HASH"
    RANGE = "RANGE"
    EVEN = "EVEN"

    @staticmethod
    def get_by_name(name: str):
        name = name.upper()
        if name == "NONE":
            return Strategy.NONE
        if name == "HASH":
            return Strategy.HASH
        if name == "RANGE":
            return Strategy.RANGE
        if name in ("EVEN", "RANDOM"):
            return Strategy.EVEN
        raise ValueError(
            f"Invalid distribution strategy: {name}. "
            f"Valid values are: {', '.join([Strategy.NONE, Strategy.HASH, Strategy.RANGE, Strategy.EVEN])}"
        )
