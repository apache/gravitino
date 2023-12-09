/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.trino.connector;

import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.transaction.IsolationLevel;
import java.util.Collections;
import java.util.List;

/**
 * DummyGravitinoConnector is primarily used to drive the GravitinoCatalogManager to load catalog
 * connectors managed in the Gravitino server. After users configure the Gravitino connector through
 * Trino catalog configuration, a DummyGravitinoFConnector is initially created. It is just a
 * placeholder.
 */
public class DummyGravitinoConnector implements Connector {

  public DummyGravitinoConnector() {
    super();
  }

  @Override
  public ConnectorTransactionHandle beginTransaction(
      IsolationLevel isolationLevel, boolean readOnly, boolean autoCommit) {
    return new ConnectorTransactionHandle() {};
  }

  @Override
  public ConnectorMetadata getMetadata(
      ConnectorSession session, ConnectorTransactionHandle transactionHandle) {
    return new ConnectorMetadata() {
      @Override
      public List<String> listSchemaNames(ConnectorSession session) {
        return Collections.emptyList();
      }
    };
  }
}
