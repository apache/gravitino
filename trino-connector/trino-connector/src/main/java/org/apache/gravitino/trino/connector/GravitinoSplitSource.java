/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.trino.connector;

import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitSource;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * The GravitinoFTransactionHandle is used to make Apache Gravitino metadata operations
 * transactional and wrap the inner connector transaction for data access.
 */
public class GravitinoSplitSource implements ConnectorSplitSource {

  private final ConnectorSplitSource connectorSplitSource;

  /**
   * Constructs a new GravitinoSplitSource with the specified split source.
   *
   * @param connectorSplitSource the internal connector split source
   */
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
