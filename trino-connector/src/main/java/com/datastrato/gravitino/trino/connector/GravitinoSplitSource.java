/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.trino.connector;

import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitSource;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * The GravitinoFTransactionHandle is used to make Gravitino metadata operations transactional and
 * wrap the inner connector transaction for data access.
 */
public class GravitinoSplitSource implements ConnectorSplitSource {

  private final ConnectorSplitSource connectorSplitSource;

  public GravitinoSplitSource(ConnectorSplitSource connectorSplitSource) {
    this.connectorSplitSource = connectorSplitSource;
  }

  @Override
  public CompletableFuture<ConnectorSplitBatch> getNextBatch(int maxSize) {
    return connectorSplitSource
        .getNextBatch(maxSize)
        .thenApply(
            batch -> {
              List<ConnectorSplit> list =
                  batch.getSplits().stream().map(GravitinoSplit::new).collect(Collectors.toList());
              return new ConnectorSplitBatch(list, batch.isNoMoreSplits());
            });
  }

  @Override
  public void close() {
    connectorSplitSource.close();
  }

  @Override
  public boolean isFinished() {
    return connectorSplitSource.isFinished();
  }

  @Override
  public Optional<List<Object>> getTableExecuteSplitsInfo() {
    return connectorSplitSource.getTableExecuteSplitsInfo();
  }
}
