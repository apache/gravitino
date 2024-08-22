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

package org.apache.gravitino.listener.api.info.partitions;

import java.util.Map;
import org.apache.gravitino.annotation.DeveloperApi;
import org.apache.gravitino.rel.expressions.literals.Literal;

/**
 * A range partition represents a result of range partitioning. For example, for range partition
 *
 * <pre>`PARTITION p20200321 VALUES LESS THAN ("2020-03-22")`</pre>
 *
 * its upper bound is "2020-03-22" and its lower bound is null.
 */
@DeveloperApi
public final class RangePartitionInfo extends PartitionInfo {
  private final Literal<?> upper;
  private final Literal<?> lower;

  /**
   * Constructs RangePartitionInfo with specified details.
   *
   * @param name The name of the Partition.
   * @param upper The upper bound of the partition.
   * @param lower The lower bound of the partition.
   * @param properties A map of properties associated with the Partition.
   */
  public RangePartitionInfo(
      String name, Literal<?> upper, Literal<?> lower, Map<String, String> properties) {
    super(name, properties);
    this.upper = upper;
    this.lower = lower;
  }

  /**
   * The upper bound of the partition.
   *
   * @return The upper bound of the partition.
   */
  public Literal<?> upper() {
    return upper;
  }

  /**
   * The lower bound of the partition.
   *
   * @return The lower bound of the partition.
   */
  public Literal<?> lower() {
    return lower;
  }
}
