/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.trino.connector;

import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;

/** This class delegates the retrieval of split data sources to optimize query performance. */
public class GravitinoSplitManager implements ConnectorSplitManager {
  private final ConnectorSplitManager internalSplitManager;

  public GravitinoSplitManager(ConnectorSplitManager internalSplitManager) {
    this.internalSplitManager = internalSplitManager;
  }

  @Override
  public ConnectorSplitSource getSplits(
      ConnectorTransactionHandle transaction,
      ConnectorSession session,
      ConnectorTableHandle connectorTableHandle,
      DynamicFilter dynamicFilter,
      Constraint constraint) {
    ConnectorSplitSource splits =
        internalSplitManager.getSplits(
            GravitinoHandle.unWrap(transaction),
            session,
            GravitinoHandle.unWrap(connectorTableHandle),
            new GravitinoDynamicFilter(dynamicFilter),
            constraint);
    return new GravitinoSplitSource(splits);
  }
}
