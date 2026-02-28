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

package org.apache.gravitino.maintenance.optimizer.api.common;

import com.google.common.base.Preconditions;
import java.util.List;

/**
 * Immutable, ordered path of partition entries from outer to inner level. Often used as the map key
 * when returning partition-level statistics.
 */
public final class PartitionPath {
  private final List<PartitionEntry> entries;

  private PartitionPath(List<PartitionEntry> entries) {
    this.entries = entries;
  }

  /**
   * Create a partition path from a list of entries (outer to inner).
   *
   * @param entries partition entries ordered from outer to inner level
   * @return a partition path
   */
  public static PartitionPath of(List<PartitionEntry> entries) {
    Preconditions.checkArgument(
        entries != null && !entries.isEmpty(), "partition entries must not be empty");
    return new PartitionPath(List.copyOf(entries));
  }

  /**
   * Partition entries ordered from outer to inner level.
   *
   * @return immutable list of partition entries
   */
  public List<PartitionEntry> entries() {
    return entries;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof PartitionPath)) {
      return false;
    }
    PartitionPath that = (PartitionPath) o;
    return entries.equals(that.entries);
  }

  @Override
  public int hashCode() {
    return entries.hashCode();
  }

  @Override
  public String toString() {
    return "PartitionPath " + entries;
  }
}
