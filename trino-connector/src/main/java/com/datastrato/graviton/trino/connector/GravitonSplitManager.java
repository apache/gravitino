/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.trino.connector;

import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;

/** This class delegates the retrieval of split data sources to optimize query performance. */
public class GravitonSplitManager implements ConnectorSplitManager {
  private final ConnectorSplitManager internalSplitManager;

  public GravitonSplitManager(ConnectorSplitManager internalSplitManager) {
    this.internalSplitManager = internalSplitManager;
  }

  public ConnectorSplitSource getSplits(
      ConnectorTransactionHandle transaction,
      ConnectorSession session,
      ConnectorTableHandle connectorTableHandle,
      DynamicFilter dynamicFilter,
      Constraint constraint) {
    GravitonTableHandle gravitonTableHandle = (GravitonTableHandle) connectorTableHandle;
    GravitonTransactionHandle gravitonTransactionHandle = (GravitonTransactionHandle) transaction;

    return internalSplitManager.getSplits(
        gravitonTransactionHandle.getInternalTransactionHandle(),
        session,
        gravitonTableHandle.getInternalTableHandle(),
        dynamicFilter,
        constraint);
  }
}
