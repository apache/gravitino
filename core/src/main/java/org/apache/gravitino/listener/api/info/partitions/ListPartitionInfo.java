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
 * A list partition represents a result of list partitioning. For example, for list partition
 *
 * <pre>
 *     `PARTITION p202204_California VALUES IN (
 *       ("2022-04-01", "Los Angeles"),
 *       ("2022-04-01", "San Francisco")
 *     )`
 *     </pre>
 *
 * its name is "p202204_California" and lists are [["2022-04-01","Los Angeles"], ["2022-04-01", "San
 * Francisco"]].
 */
@DeveloperApi
public final class ListPartitionInfo extends PartitionInfo {
  private final Literal<?>[][] lists;

  /**
   * Constructs ListPartitionInfo with specified details.
   *
   * @param name The name of the Partition.
   * @param properties A map of properties associated with the Partition.
   * @param lists The values of the list partition.
   */
  public ListPartitionInfo(String name, Map<String, String> properties, Literal<?>[][] lists) {
    super(name, properties);
    this.lists = lists;
  }

  /**
   * Returns the values of the list partition.
   *
   * @return the values of the list partition.
   */
  public Literal<?>[][] lists() {
    return lists;
  }
}
