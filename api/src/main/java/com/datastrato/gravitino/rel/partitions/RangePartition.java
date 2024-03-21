/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.rel.partitions;

import com.datastrato.gravitino.annotation.Evolving;
import com.datastrato.gravitino.rel.expressions.literals.Literal;

/**
 * A range partition represents a result of range partitioning. For example, for range partition
 *
 * <pre>`PARTITION p20200321 VALUES LESS THAN ("2020-03-22")`</pre>
 *
 * its upper bound is "2020-03-22" and its lower bound is null.
 */
@Evolving
public interface RangePartition extends Partition {

  /** @return The upper bound of the partition. */
  Literal<?> upper();

  /** @return The lower bound of the partition. */
  Literal<?> lower();
}
