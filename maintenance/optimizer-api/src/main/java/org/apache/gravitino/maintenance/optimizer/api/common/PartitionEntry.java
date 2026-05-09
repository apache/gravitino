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

import org.apache.gravitino.annotation.DeveloperApi;

/**
 * A single partition key/value pair.
 *
 * <p>For multi-level partitions, each level is represented by a separate entry (for example, {@code
 * p1=a} and {@code p2=b} for a {@code p1=a/p2=b} partition path). Combine entries with {@link
 * PartitionPath#of(java.util.List)} when returning the whole partition path.
 */
@DeveloperApi
public interface PartitionEntry {

  /**
   * Partition name.
   *
   * @return name of the partition field (for example, {@code ds} or {@code bucket_id})
   */
  String partitionName();

  /**
   * Partition value as a string.
   *
   * @return string representation of the partition value (e.g., {@code YYYY-MM-DD})
   */
  String partitionValue();
}
