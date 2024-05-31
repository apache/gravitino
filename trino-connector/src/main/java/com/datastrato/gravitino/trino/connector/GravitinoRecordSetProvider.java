/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.trino.connector;

import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorRecordSetProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.RecordSet;
import java.util.List;

/** This class provides a RecordSet for trino read data from internal connector. */
public class GravitinoRecordSetProvider implements ConnectorRecordSetProvider {

  ConnectorRecordSetProvider internalRecordSetProvider;

  public GravitinoRecordSetProvider(ConnectorRecordSetProvider recordSetProvider) {
    this.internalRecordSetProvider = recordSetProvider;
  }

  @Override
  public RecordSet getRecordSet(
      ConnectorTransactionHandle transaction,
      ConnectorSession session,
      ConnectorSplit split,
      ConnectorTableHandle table,
      List<? extends ColumnHandle> columns) {
    return internalRecordSetProvider.getRecordSet(
        GravitinoHandle.unWrap(transaction),
        session,
        GravitinoHandle.unWrap(split),
        GravitinoHandle.unWrap(table),
        GravitinoHandle.unWrap(columns));
  }
}
