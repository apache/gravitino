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

from datetime import datetime
from enum import Enum
from typing import Optional

from gravitino.api.auditable import Auditable


class SemanticModelStatus(str, Enum):
    """The public publication status of a revision."""

    DRAFT = "DRAFT"
    PUBLISHED = "PUBLISHED"


class SemanticModelRevision(Auditable):
    """Revision metadata, with audit information describing the writer."""

    def revision(self) -> int:
        """Returns the revision number, or zero if the server omitted it."""
        return 0

    def status(self) -> Optional[SemanticModelStatus]:
        """Returns the publication status when supplied by the server."""
        return None

    def published_by(self) -> Optional[str]:
        """Returns the publisher, or None for a draft."""
        return None

    def published_at(self) -> Optional[datetime]:
        """Returns the publication time, or None for a draft."""
        return None
