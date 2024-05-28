/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.trino.connector;

import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;
import java.util.List;

/** This class provides a ConnectorPageSource for trino read data from internal connector. */
public class GravitinoDataSourceProvider implements ConnectorPageSourceProvider {

  ConnectorPageSourceProvider internalPageSourceProvider;

  public GravitinoDataSourceProvider(ConnectorPageSourceProvider pageSourceProvider) {
    this.internalPageSourceProvider = pageSourceProvider;
  }

  @Override
  public ConnectorPageSource createPageSource(
      ConnectorTransactionHandle transaction,
      ConnectorSession session,
      ConnectorSplit split,
      ConnectorTableHandle table,
      List<ColumnHandle> columns,
      DynamicFilter dynamicFilter) {
    return internalPageSourceProvider.createPageSource(
        GravitinoHandle.unWrap(transaction),
        session,
        GravitinoHandle.unWrap(split),
        GravitinoHandle.unWrap(table),
        GravitinoHandle.unWrap(columns),
        new GravitinoDynamicFilter(dynamicFilter));
  }
}
