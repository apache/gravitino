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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableExecuteHandle;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.trino.connector.catalog.CatalogConnectorMetadata;
import org.apache.gravitino.trino.connector.catalog.CatalogConnectorMetadataAdapter;
import org.junit.jupiter.api.Test;

/**
 * Pins the Trino 481 SPI shapes of the table-execute delegation in {@link GravitinoMetadata481}:
 * both {@code finishTableExecute} and {@code executeTableExecute} return a {@code Map}, each
 * delegating to the internal metadata with the unwrapped handle.
 */
class TestTableExecuteDelegation481 {

  @Test
  void testFinishTableExecuteDelegatesAndUnwrapsHandle() {
    ConnectorMetadata internalMetadata = mock(ConnectorMetadata.class);
    ConnectorSession session = mock(ConnectorSession.class);
    ConnectorTableExecuteHandle internalHandle = mock(ConnectorTableExecuteHandle.class);
    GravitinoTableExecuteHandle wrapped = new GravitinoTableExecuteHandle(internalHandle);

    Map<String, Long> finishInfo = Map.of("fragments", 3L);
    when(internalMetadata.finishTableExecute(session, internalHandle, List.of(), List.of()))
        .thenReturn(finishInfo);

    assertThat(
            createMetadata(internalMetadata)
                .finishTableExecute(session, wrapped, List.of(), List.of()))
        .isEqualTo(finishInfo);
  }

  @Test
  void testExecuteTableExecuteDelegatesAndUnwrapsHandle() {
    ConnectorMetadata internalMetadata = mock(ConnectorMetadata.class);
    ConnectorSession session = mock(ConnectorSession.class);
    ConnectorTableExecuteHandle internalHandle = mock(ConnectorTableExecuteHandle.class);
    GravitinoTableExecuteHandle wrapped = new GravitinoTableExecuteHandle(internalHandle);
    Map<String, Long> fragmentsInfo = Map.of("processed_rows", 7L);
    when(internalMetadata.executeTableExecute(session, internalHandle)).thenReturn(fragmentsInfo);

    assertThat(createMetadata(internalMetadata).executeTableExecute(session, wrapped))
        .isEqualTo(fragmentsInfo);
  }

  private GravitinoMetadata481 createMetadata(ConnectorMetadata internalMetadata) {
    return new GravitinoMetadata481(
        mock(CatalogConnectorMetadata.class),
        mock(CatalogConnectorMetadataAdapter.class),
        internalMetadata);
  }
}
