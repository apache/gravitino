/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.catalogs.hive;

public class HiveTablePropertyKeys {
  public static final String COMMENT = "comment";
  public static final String NUM_FILES = "numFiles";
  public static final String TOTAL_SIZE = "totalSize";
  public static final String LOCATION = "location";
  public static final String FORMAT = "format";
  public static final String TABLE_TYPE = "table-type";
  public static final String INPUT_FORMAT = "input-format";
  public static final String OUTPUT_FORMAT = "output-format";
  public static final String SERDE_NAME = "serde-name";
  public static final String SERDE_LIB = "serde-lib";
  public static final String EXTERNAL = "EXTERNAL";

  public static final String TRANSIENT_LAST_DDL_TIME = "transient_lastDdlTime";
}
