/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.trino.connector;

import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorOutputTableHandle;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.connector.ConnectorPageSinkId;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTransactionHandle;
import org.apache.commons.lang3.NotImplementedException;

/** This class provides a ConnectorPageSink for trino to write data to internal connector. */
public class GravitinoPageSinkProvider implements ConnectorPageSinkProvider {

  ConnectorPageSinkProvider pageSinkProvider;

  public GravitinoPageSinkProvider(ConnectorPageSinkProvider pageSinkProvider) {
    this.pageSinkProvider = pageSinkProvider;
  }

  @Override
  public ConnectorPageSink createPageSink(
      ConnectorTransactionHandle transactionHandle,
      ConnectorSession session,
      ConnectorOutputTableHandle outputTableHandle,
      ConnectorPageSinkId pageSinkId) {
    throw new NotImplementedException();
  }

  @Override
  public ConnectorPageSink createPageSink(
      ConnectorTransactionHandle transactionHandle,
      ConnectorSession session,
      ConnectorInsertTableHandle insertTableHandle,
      ConnectorPageSinkId pageSinkId) {
    return pageSinkProvider.createPageSink(
        GravitinoHandle.unWrap(transactionHandle),
        session,
        GravitinoHandle.unWrap(insertTableHandle),
        pageSinkId);
  }
}
