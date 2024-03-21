/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.rel.partitions;

import com.datastrato.gravitino.annotation.Evolving;
import com.datastrato.gravitino.rel.Table;
import java.util.Map;

/**
 * A partition represents a result of partitioning a table. The partition can be either a {@link
 * IdentityPartition}, {@link ListPartition} or {@link RangePartition}. It depends on the {@link
 * Table#partitioning()}.
 */
@Evolving
public interface Partition {

  /** @return The name of the partition. */
  String name();

  /** @return The properties of the partition, such as statistics, location, etc. */
  Map<String, String> properties();
}
