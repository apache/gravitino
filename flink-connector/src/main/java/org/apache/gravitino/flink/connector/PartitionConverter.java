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
package org.apache.gravitino.flink.connector;

import java.util.List;
import org.apache.gravitino.rel.expressions.transforms.Transform;

/**
 * The PartitionConverter is used to convert the partition between Flink and Gravitino. The Flink
 * only support identity transform. Some of the table like Apache Paimon will use the table
 * properties to store the partition transform, so we can implement this interface to achieve more
 * partition transform.
 */
public interface PartitionConverter {
  /**
   * Convert the partition keys to Flink partition keys.
   *
   * @param partitions The partition keys in Gravitino.
   * @return The partition keys in Flink.
   */
  public abstract List<String> toFlinkPartitionKeys(Transform[] partitions);

  /**
   * Convert the partition keys to Gravitino partition keys.
   *
   * @param partitionsKey The partition keys in Flink.
   * @return The partition keys in Gravitino.
   */
  public abstract Transform[] toGravitinoPartitions(List<String> partitionsKey);
}
