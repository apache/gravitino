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

import unicodedata

from gravitino.exceptions.base import IllegalArgumentException


def normalize_article_name(name: str) -> str:
    """Return the NFC article name used by both metadata-object and article APIs."""
    if not isinstance(name, str):
        raise IllegalArgumentException("Article name must be a string")
    normalized = unicodedata.normalize("NFC", name)
    categories = [unicodedata.category(ch) for ch in normalized]
    if (
        not 1 <= len(normalized) <= 128
        or not any(
            category.startswith("L") or category == "Nd" for category in categories
        )
        or not all(
            category.startswith(("L", "M")) or category == "Nd" or ch in "_-"
            for ch, category in zip(normalized, categories)
        )
    ):
        raise IllegalArgumentException(f"Invalid article name: {name}")
    return normalized
