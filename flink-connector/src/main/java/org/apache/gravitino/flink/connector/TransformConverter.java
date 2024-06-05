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
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.expressions.transforms.Transforms;

/**
 * The TransformConverter is used to convert the partition between Flink and Gravitino. The Flink
 * only support identity transform. Some of the table like Apache Paimon will use the table
 * properties to store the partition transform, so we can implement this interface to achieve more
 * partition transform.
 */
public interface TransformConverter {
  default List<String> toFlinkPartitionKeys(Transform[] transforms) {
    List<String> partitionKeys =
        Arrays.stream(transforms)
            .filter(t -> t instanceof Transforms.IdentityTransform)
            .map(Transform::name)
            .collect(Collectors.toList());
    Preconditions.checkArgument(
        partitionKeys.size() == transforms.length,
        "Flink only support identity transform for now.");
    return partitionKeys;
  }

  default Transform[] toGravitinoTransforms(List<String> partitionsKey) {
    return partitionsKey.stream().map(Transforms::identity).toArray(Transform[]::new);
  }
}
