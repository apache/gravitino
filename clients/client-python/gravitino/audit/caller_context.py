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
import threading
from typing import Dict

caller_context_holder = threading.local()


class CallerContext:
    """A class defining the caller context for auditing coarse-grained operations."""

    _context: Dict[str, str]

    def __init__(self, context: Dict[str, str]):
        """Initialize the CallerContext.

        Args:
            context: The context dict.
        """
        self._context = context

    def context(self) -> Dict[str, str]:
        """Returns the context dict in the caller context.

        Returns:
             The context dict.
        """
        return self._context


class CallerContextHolder:
    """A thread local holder for the CallerContext."""

    @staticmethod
    def set(context: CallerContext):
        """Set the CallerContext in the thread local.

        Args:
             context: The CallerContext to set.
        """
        caller_context_holder.caller_context = context

    @staticmethod
    def get():
        """Get the CallerContext from the thread local.

        Returns:
             The CallerContext.
        """
        if not hasattr(caller_context_holder, "caller_context"):
            return None
        return caller_context_holder.caller_context

    @staticmethod
    def remove():
        """Remove the CallerContext from the thread local."""
        if hasattr(caller_context_holder, "caller_context"):
            del caller_context_holder.caller_context
