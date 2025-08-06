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
package org.apache.gravitino.stats;

import org.apache.gravitino.rel.partitions.Partition;

/** PartitionNameComparator is a comparator that compares two partitions based on their names. */
class PartitionNameComparator implements PartitionComparator {

  private static final PartitionComparator instance = new PartitionNameComparator();

  /**
   * Returns a singleton instance of PartitionNameComparator.
   *
   * @return the singleton instance of PartitionNameComparator
   */
  public static PartitionComparator instance() {
    return instance;
  }

  @Override
  public int compareTo(Partition firstPartition, Partition secondPartition) {
    if (firstPartition == null && secondPartition == null) {
      return 0;
    } else if (firstPartition == null) {
      return -1;
    } else if (secondPartition == null) {
      return 1;
    }
    return firstPartition.name().compareTo(secondPartition.name());
  }

  @Override
  public Type type() {
    return Type.NAME;
  }
}
