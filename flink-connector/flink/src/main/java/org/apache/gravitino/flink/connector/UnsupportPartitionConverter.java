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

import com.google.common.base.Preconditions;
import java.util.Collections;
import java.util.List;
import org.apache.gravitino.rel.expressions.transforms.Transform;

/** Suitable for Catalog types that do not support partition keys. */
public class UnsupportPartitionConverter implements PartitionConverter {

  private UnsupportPartitionConverter() {}

  public static final UnsupportPartitionConverter INSTANCE = new UnsupportPartitionConverter();

  @Override
  public List<String> toFlinkPartitionKeys(Transform[] partitions) {
    Preconditions.checkArgument(
        partitions == null || partitions.length == 0, "Partition key conversion is not supported.");
    return Collections.emptyList();
  }

  @Override
  public Transform[] toGravitinoPartitions(List<String> partitionsKey) {
    Preconditions.checkArgument(
        partitionsKey == null || partitionsKey.isEmpty(),
        "Partition key conversion is not supported.");
    return new Transform[0];
  }
}
