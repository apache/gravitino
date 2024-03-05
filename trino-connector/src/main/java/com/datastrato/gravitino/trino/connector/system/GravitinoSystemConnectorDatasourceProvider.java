/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.trino.connector.system;

import com.datastrato.gravitino.trino.connector.GravitinoTableHandle;
import com.datastrato.gravitino.trino.connector.GravitinoTransactionHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorRecordSetProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.RecordSet;
import io.trino.spi.connector.SchemaTableName;

import java.util.List;

import static com.datastrato.gravitino.trino.connector.system.GravitinoSystemTable.createSystemTablePageSource;

public class GravitinoSystemConnectorDatasourceProvider implements ConnectorPageSourceProvider {

  @Override
  public ConnectorPageSource createPageSource(
      ConnectorTransactionHandle transaction,
      ConnectorSession session,
      ConnectorSplit split,
      ConnectorTableHandle table,
      List<ColumnHandle> columns,
      DynamicFilter dynamicFilter) {

    SchemaTableName tableName = ((GravitinoSystemTableHandle) table).name;
    return createSystemTablePageSource(tableName);
  }
}
