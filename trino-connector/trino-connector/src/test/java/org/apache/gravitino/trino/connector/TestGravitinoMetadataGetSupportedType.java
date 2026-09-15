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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.VarcharType;
import java.util.Collections;
import java.util.Optional;
import org.apache.gravitino.trino.connector.catalog.CatalogConnectorMetadata;
import org.apache.gravitino.trino.connector.catalog.CatalogConnectorMetadataAdapter;
import org.apache.gravitino.trino.connector.catalog.jdbc.mysql.MySQLDataTypeTransformer;
import org.junit.jupiter.api.Test;

public class TestGravitinoMetadataGetSupportedType {

  @Test
  public void testDelegatesToDataTypeTransformer() {
    CatalogConnectorMetadataAdapter metadataAdapter = mock(CatalogConnectorMetadataAdapter.class);
    when(metadataAdapter.getDataTypeTransformer()).thenReturn(new MySQLDataTypeTransformer());
    ConnectorSession session = mock(ConnectorSession.class);
    GravitinoMetadata metadata =
        new StubGravitinoMetadata(
            mock(CatalogConnectorMetadata.class), metadataAdapter, mock(ConnectorMetadata.class));

    assertEquals(
        Optional.of(TimestampType.TIMESTAMP_MICROS),
        metadata.getSupportedType(session, Collections.emptyMap(), TimestampType.TIMESTAMP_NANOS));
    assertEquals(
        Optional.empty(),
        metadata.getSupportedType(session, Collections.emptyMap(), TimestampType.TIMESTAMP_MICROS));
    assertEquals(
        Optional.empty(),
        metadata.getSupportedType(session, Collections.emptyMap(), VarcharType.VARCHAR));
  }

  private static final class StubGravitinoMetadata extends GravitinoMetadata {
    private StubGravitinoMetadata(
        CatalogConnectorMetadata catalogConnectorMetadata,
        CatalogConnectorMetadataAdapter metadataAdapter,
        ConnectorMetadata internalMetadata) {
      super(catalogConnectorMetadata, metadataAdapter, internalMetadata);
    }
  }
}
