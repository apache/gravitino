/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.trino.connector;

import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorOutputTableHandle;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.connector.ConnectorPageSinkId;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTransactionHandle;

/** This class provides a ConnectorPageSink for trino to write data to internal connector. */
public class GravitonPageSinkProvider implements ConnectorPageSinkProvider {

  ConnectorPageSinkProvider pageSinkProvider;

  public GravitonPageSinkProvider(ConnectorPageSinkProvider pageSinkProvider) {
    this.pageSinkProvider = pageSinkProvider;
  }

  @Override
  public ConnectorPageSink createPageSink(
      ConnectorTransactionHandle transactionHandle,
      ConnectorSession session,
      ConnectorOutputTableHandle outputTableHandle,
      ConnectorPageSinkId pageSinkId) {
    return null;
  }

  @Override
  public ConnectorPageSink createPageSink(
      ConnectorTransactionHandle transactionHandle,
      ConnectorSession session,
      ConnectorInsertTableHandle insertTableHandle,
      ConnectorPageSinkId pageSinkId) {
    GravitonTransactionHandle gravitonTransactionHandle =
        (GravitonTransactionHandle) transactionHandle;
    GravitonInsertTableHandle gravitonInsertTableHandle =
        (GravitonInsertTableHandle) insertTableHandle;

    return pageSinkProvider.createPageSink(
        gravitonTransactionHandle.getInternalTransactionHandle(),
        session,
        gravitonInsertTableHandle.innerHandler(),
        pageSinkId);
  }
}
