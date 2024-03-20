/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.rel.partitions;

import com.datastrato.gravitino.annotation.Evolving;
import com.datastrato.gravitino.rel.expressions.literals.Literal;

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
@Evolving
public interface ListPartition extends Partition {

  /** @return The values of the list partition. */
  Literal<?>[][] lists();
}
