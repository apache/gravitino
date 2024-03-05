/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.trino.connector.system;

import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.FixedSplitSource;
import io.trino.spi.connector.SchemaTableName;

public class GravitinoSystemConnectorSplitManger implements ConnectorSplitManager {

  @Override
  public ConnectorSplitSource getSplits(
      ConnectorTransactionHandle transaction,
      ConnectorSession session,
      ConnectorTableHandle connectorTableHandle,
      DynamicFilter dynamicFilter,
      Constraint constraint) {
    SchemaTableName tableName = ((GravitinoSystemTableHandle) connectorTableHandle).name;
    return new FixedSplitSource(new GravitinoSystemConnectorSplit(tableName));
  }
}
