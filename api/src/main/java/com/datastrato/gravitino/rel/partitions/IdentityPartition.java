/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.rel.partitions;

import com.datastrato.gravitino.annotation.Evolving;
import com.datastrato.gravitino.rel.expressions.literals.Literal;

/**
 * An identity partition represents a result of identity partitioning. For example, for Hive
 * partition
 *
 * <pre>`PARTITION (dt='2008-08-08',country='us')`</pre>
 *
 * its partition name is "dt=2008-08-08/country=us", field names are [["dt"], ["country"]] and
 * values are ["2008-08-08", "us"].
 */
@Evolving
public interface IdentityPartition extends Partition {

  /** @return The field names of the identity partition. */
  String[][] fieldNames();

  /**
   * @return The values of the identity partition. The values are in the same order as the field
   *     names.
   */
  Literal<?>[] values();
}
