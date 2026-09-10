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

from gravitino.utils.precondition import Precondition


class CustomExtension:
    """A vendor-specific extension carried by a Semantic Model member.

    Custom extensions are preserved losslessly for Ossie interchange, Gravitino
    does not interpret the extension data.
    """

    def __init__(self, vendor_name: str, data: str):
        Precondition.check_argument(
            vendor_name is not None, "vendorName must not be null"
        )
        Precondition.check_argument(data is not None, "data must not be null")
        self._vendor_name = vendor_name
        self._data = data

    def vendor_name(self) -> str:
        """Returns the vendor name that owns this extension."""
        return self._vendor_name

    def data(self) -> str:
        """Returns the opaque extension data."""
        return self._data

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, CustomExtension):
            return False
        return self._vendor_name == other.vendor_name() and self._data == other.data()

    def __hash__(self) -> int:
        return hash((self._vendor_name, self._data))

    def __repr__(self) -> str:
        return f"CustomExtension(vendorName={self._vendor_name!r}, data={self._data!r})"
