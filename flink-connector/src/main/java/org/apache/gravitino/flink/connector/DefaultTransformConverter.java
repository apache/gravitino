/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package org.apache.gravitino.flink.connector;

import com.google.common.base.Preconditions;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.expressions.transforms.Transforms;

public class DefaultTransformConverter implements TransformConverter {

  private DefaultTransformConverter() {}

  public static final DefaultTransformConverter INSTANCE = new DefaultTransformConverter();

  @Override
  public List<String> toFlinkPartitionKeys(Transform[] transforms) {
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

  @Override
  public Transform[] toGravitinoPartitions(List<String> partitionsKey) {
    return partitionsKey.stream().map(Transforms::identity).toArray(Transform[]::new);
  }
}
