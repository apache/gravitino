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
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.security.ConnectorIdentity;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.SupportsSchemas;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.trino.connector.catalog.CatalogConnectorContext;
import org.apache.gravitino.trino.connector.catalog.CatalogConnectorMetadata;
import org.apache.gravitino.trino.connector.catalog.CatalogConnectorMetadataAdapter;
import org.apache.gravitino.trino.connector.metadata.GravitinoCatalog;
import org.junit.jupiter.api.Test;

class TestGravitinoConnectorPassthrough {
  @Test
  void passwordCatalogLifecycleAndSchemaListingDoNotRequireNativeToken() {
    Connector delegate = mock(Connector.class);
    GravitinoConnector connector =
        connector(context("OAUTH2_PASSTHROUGH", "lakehouse-iceberg", delegate));
    ConnectorSession session = mock(ConnectorSession.class);
    when(session.getIdentity()).thenReturn(ConnectorIdentity.ofUser("catalog_manager"));
    ConnectorTransactionHandle transaction = mock(ConnectorTransactionHandle.class);
    ConnectorMetadata metadata =
        connector.getMetadata(session, new GravitinoTransactionHandle(transaction));
    metadata.beginQuery(session);
    assertEquals(List.of("demo"), metadata.listSchemaNames(session));
    metadata.cleanupQuery(session);
    verifyNoInteractions(delegate);
  }

  @Test
  void passthroughDataAccessUsesOriginalSessionAndPropagatesMissingToken() {
    Connector delegate = mock(Connector.class);
    GravitinoConnector connector =
        connector(context("OAUTH2_PASSTHROUGH", "lakehouse-iceberg", delegate));
    ConnectorSession session = mock(ConnectorSession.class);
    ConnectorTransactionHandle transaction = mock(ConnectorTransactionHandle.class);
    RuntimeException failure = new IllegalArgumentException("Missing delegated token");
    when(delegate.getMetadata(session, transaction)).thenThrow(failure);
    ConnectorMetadata metadata = connector.getInternalMetadata(session, transaction);
    verifyNoInteractions(delegate);
    assertSame(
        failure,
        assertThrows(IllegalArgumentException.class, () -> metadata.listSchemaNames(session)));
    verify(delegate).getMetadata(session, transaction);
  }

  @Test
  void otherModesAndProvidersRetainEagerMetadata() {
    for (String[] mode :
        new String[][] {{"OAUTH2", "lakehouse-iceberg"}, {"OAUTH2_PASSTHROUGH", "hive"}}) {
      Connector delegate = mock(Connector.class);
      ConnectorMetadata nativeMetadata = mock(ConnectorMetadata.class);
      ConnectorSession session = mock(ConnectorSession.class);
      ConnectorTransactionHandle transaction = mock(ConnectorTransactionHandle.class);
      when(delegate.getMetadata(session, transaction)).thenReturn(nativeMetadata);
      GravitinoConnector connector = connector(context(mode[0], mode[1], delegate));
      assertSame(nativeMetadata, connector.getInternalMetadata(session, transaction));
      verify(delegate).getMetadata(session, transaction);
    }
  }

  private GravitinoConnector connector(CatalogConnectorContext context) {
    return new GravitinoConnector(context) {
      @Override
      protected GravitinoMetadata createGravitinoMetadata(
          CatalogConnectorMetadata metadata,
          CatalogConnectorMetadataAdapter adapter,
          ConnectorMetadata delegate) {
        return new GravitinoMetadata(metadata, adapter, delegate) {};
      }
    };
  }

  private CatalogConnectorContext context(String security, String provider, Connector delegate) {
    CatalogConnectorContext context = mock(CatalogConnectorContext.class);
    GravitinoCatalog catalog = mock(GravitinoCatalog.class);
    when(catalog.geNameIdentifier()).thenReturn(NameIdentifier.of("demo", "iceberg_demo"));
    when(catalog.getProvider()).thenReturn(provider);
    GravitinoMetalake metalake = mock(GravitinoMetalake.class);
    Catalog live = mock(Catalog.class);
    SupportsSchemas schemas = mock(SupportsSchemas.class);
    when(schemas.listSchemas()).thenReturn(new String[] {"demo"});
    when(live.asSchemas()).thenReturn(schemas);
    when(metalake.loadCatalog(any())).thenReturn(live);
    when(context.getCatalog()).thenReturn(catalog);
    when(context.getMetalake()).thenReturn(metalake);
    when(context.getInternalConnector()).thenReturn(delegate);
    when(context.getConfig())
        .thenReturn(
            new GravitinoConfig(
                Map.of(
                    "gravitino.metalake", "demo",
                    "gravitino.client.authType", "oauth2",
                    "gravitino.client.session.forwardUser", "true",
                    "gravitino.iceberg.rest-catalog.security", security)));
    return context;
  }
}
