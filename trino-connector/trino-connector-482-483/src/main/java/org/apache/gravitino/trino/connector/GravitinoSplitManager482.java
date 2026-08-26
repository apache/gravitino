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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.DynamicFilterSnapshot;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * Trino 482 reworked {@link ConnectorSplitSource#getNextBatch}: it now takes a {@link
 * DynamicFilterSnapshot} and returns a {@code CompletableFuture<List<ConnectorSplit>>} instead of
 * the removed {@code ConnectorSplitSource.ConnectorSplitBatch}. Because that return type no longer
 * exists, the shared {@code GravitinoSplitSource} cannot compile against the Trino 482 SPI and is
 * excluded from this module; this class provides a standalone Trino 482 split source instead. The
 * {@code getSplits} dispatch itself is inherited from {@link GravitinoSplitManager}.
 */
public class GravitinoSplitManager482 extends GravitinoSplitManager {

  /**
   * Constructs a new GravitinoSplitManager482 with the specified split manager.
   *
   * @param internalSplitManager the internal connector split manager
   */
  public GravitinoSplitManager482(ConnectorSplitManager internalSplitManager) {
    super(internalSplitManager);
  }

  @Override
  protected ConnectorSplitSource createSplitSource(ConnectorSplitSource splits) {
    return new GravitinoSplitSource482(splits);
  }

  /** A Trino 482 {@link ConnectorSplitSource} that wraps each internal split for Gravitino. */
  static class GravitinoSplitSource482 implements ConnectorSplitSource {

    private final ConnectorSplitSource connectorSplitSource;

    GravitinoSplitSource482(ConnectorSplitSource connectorSplitSource) {
      this.connectorSplitSource = connectorSplitSource;
    }

    @Override
    public CompletableFuture<List<ConnectorSplit>> getNextBatch(
        int maxSize, DynamicFilterSnapshot dynamicFilterSnapshot) {
      return connectorSplitSource
          .getNextBatch(maxSize, dynamicFilterSnapshot)
          .thenApply(
              splits ->
                  splits.stream()
                      .map(split -> (ConnectorSplit) new GravitinoSplit482(split))
                      .collect(Collectors.toList()));
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

  /** A Gravitino split wrapper for Trino 482. */
  public static class GravitinoSplit482 extends GravitinoSplit {

    /**
     * Constructs a new GravitinoSplit482 from a serialized handle string.
     *
     * @param handleString the serialized handle string
     */
    @JsonCreator
    public GravitinoSplit482(@JsonProperty(HANDLE_STRING) String handleString) {
      super(handleString);
    }

    /**
     * Constructs a new GravitinoSplit482 from a ConnectorSplit.
     *
     * @param split the internal connector split
     */
    public GravitinoSplit482(ConnectorSplit split) {
      super(split);
    }
  }
}
