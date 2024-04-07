/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.spark.connector;

import com.datastrato.gravitino.rel.expressions.sorts.SortDirection;

public class ConnectorConstants {
  public static final String COMMENT = "comment";

  public static final SortDirection SPARK_DEFAULT_SORT_DIRECTION = SortDirection.ASCENDING;
  public static final String LOCATION = "location";

  public static final String DOT = ".";

  private ConnectorConstants() {}
}
