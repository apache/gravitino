/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.gravitino.maintenance.optimizer.common;

import java.util.ArrayDeque;
import java.util.Deque;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility to close a collection of resources while preserving the first failure and suppressing the
 * rest. Resources are closed in reverse registration order to mimic typical stack lifecycles.
 */
public final class CloseableGroup implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(CloseableGroup.class);
  private final Deque<CloseableEntry> entries = new ArrayDeque<>(4);

  public void register(AutoCloseable closeable, String name) {
    entries.addFirst(new CloseableEntry(closeable, name));
  }

  @Override
  public void close() throws Exception {
    Exception rootCause = null;
    while (!entries.isEmpty()) {
      CloseableEntry entry = entries.removeFirst();
      try {
        entry.closeable.close();
      } catch (Exception e) {
        LOG.warn("Failed to close {}", entry.name, e);
        if (rootCause == null) {
          rootCause = e;
        } else if (rootCause != e) {
          rootCause.addSuppressed(e);
        }
      }
    }
    if (rootCause != null) {
      throw rootCause;
    }
  }

  private static final class CloseableEntry {
    private final AutoCloseable closeable;
    private final String name;

    private CloseableEntry(AutoCloseable closeable, String name) {
      this.closeable = closeable;
      this.name = name;
    }
  }
}
