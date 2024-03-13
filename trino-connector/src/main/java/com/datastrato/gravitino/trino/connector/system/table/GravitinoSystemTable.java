/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.trino.connector.system.table;

import io.trino.spi.Page;
import io.trino.spi.connector.ConnectorTableMetadata;

/** Gravitino System table interfaces * */
public abstract class GravitinoSystemTable {

  public static final String SYSTEM_TABLE_SCHEMA_NAME = "system";

  /** Return the definition of the table * */
  public abstract ConnectorTableMetadata getTableMetaData();

  /** Return all the data of the table * */
  public abstract Page loadPageData();
}
