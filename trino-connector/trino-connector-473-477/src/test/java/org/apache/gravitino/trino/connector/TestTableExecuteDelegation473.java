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

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableExecuteHandle;
import java.util.List;
import org.apache.gravitino.trino.connector.catalog.CatalogConnectorMetadata;
import org.apache.gravitino.trino.connector.catalog.CatalogConnectorMetadataAdapter;
import org.junit.jupiter.api.Test;

/**
 * Pins the Trino 473-477 SPI shapes of the table-execute delegation in {@link
 * GravitinoMetadata473}: both {@code finishTableExecute} and {@code executeTableExecute} return
 * void, each delegating to the internal metadata with the unwrapped handle.
 */
class TestTableExecuteDelegation473 {

  @Test
  void testFinishTableExecuteDelegatesAndUnwrapsHandle() {
    ConnectorMetadata internalMetadata = mock(ConnectorMetadata.class);
    ConnectorSession session = mock(ConnectorSession.class);
    ConnectorTableExecuteHandle internalHandle = mock(ConnectorTableExecuteHandle.class);
    GravitinoTableExecuteHandle wrapped = new GravitinoTableExecuteHandle(internalHandle);

    createMetadata(internalMetadata).finishTableExecute(session, wrapped, List.of(), List.of());

    verify(internalMetadata)
        .finishTableExecute(eq(session), eq(internalHandle), eq(List.of()), eq(List.of()));
  }

  @Test
  void testExecuteTableExecuteDelegatesAndUnwrapsHandle() {
    ConnectorMetadata internalMetadata = mock(ConnectorMetadata.class);
    ConnectorSession session = mock(ConnectorSession.class);
    ConnectorTableExecuteHandle internalHandle = mock(ConnectorTableExecuteHandle.class);
    GravitinoTableExecuteHandle wrapped = new GravitinoTableExecuteHandle(internalHandle);

    createMetadata(internalMetadata).executeTableExecute(session, wrapped);

    verify(internalMetadata).executeTableExecute(eq(session), eq(internalHandle));
  }

  private GravitinoMetadata473 createMetadata(ConnectorMetadata internalMetadata) {
    return new GravitinoMetadata473(
        mock(CatalogConnectorMetadata.class),
        mock(CatalogConnectorMetadataAdapter.class),
        internalMetadata);
  }
}
