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
package org.apache.gravitino.catalog.lakehouse.iceberg;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.auxiliary.AuxiliaryIcebergCatalogBridge;
import org.apache.gravitino.connector.CatalogInfo;
import org.apache.gravitino.connector.HasPropertyMetadata;
import org.apache.gravitino.exceptions.NoSuchCatalogException;
import org.apache.gravitino.iceberg.common.IcebergConfig;
import org.apache.gravitino.iceberg.common.ops.IcebergCatalogWrapper;
import org.apache.gravitino.meta.AuditInfo;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TestIcebergAuxInProcessOperations {

  private AuxiliaryIcebergCatalogBridge.OperationsFactory operationsFactory;

  @AfterEach
  void tearDown() {
    if (operationsFactory != null) {
      AuxiliaryIcebergCatalogBridge.unregister(operationsFactory);
      operationsFactory = null;
    }
  }

  @Test
  void metalakeCatalogUsesAuxIrcWrapper() {
    IcebergCatalogWrapper ircWrapper = new IcebergCatalogWrapper(memoryConfig());
    AtomicReference<IcebergCatalogOperations> auxOps = new AtomicReference<>();
    operationsFactory =
        () -> {
          IcebergCatalogOperations created =
              IcebergCatalogOperations.createForLocalIrc(
                  catalogName -> {
                    if ("orders".equals(catalogName)) {
                      return ircWrapper;
                    }
                    throw new NoSuchCatalogException("missing %s", catalogName);
                  });
          auxOps.set(created);
          return created;
        };
    AuxiliaryIcebergCatalogBridge.register(operationsFactory);

    CatalogInfo info =
        new CatalogInfo(
            1L,
            "orders",
            Catalog.Type.RELATIONAL,
            "lakehouse-iceberg",
            null,
            memoryProperties(),
            AuditInfo.EMPTY,
            Namespace.of("metalake"));
    HasPropertyMetadata metadata = Mockito.mock(HasPropertyMetadata.class);
    Mockito.when(metadata.catalogPropertiesMetadata())
        .thenReturn(new IcebergCatalogPropertiesMetadata());

    IcebergCatalogOperations metalakeOps = new IcebergCatalogOperations();
    metalakeOps.initialize(memoryProperties(), info, metadata);
    Assertions.assertNull(metalakeOps.backend);

    NameIdentifier[] schemas = metalakeOps.listSchemas(Namespace.of("metalake", "orders"));
    Assertions.assertEquals(0, schemas.length);
    Assertions.assertSame(ircWrapper, auxOps.get().backend.wrapper());
    metalakeOps.close();
    Assertions.assertDoesNotThrow(ircWrapper::close);
  }

  private static Map<String, String> memoryProperties() {
    return ImmutableMap.of(
        IcebergConstants.CATALOG_BACKEND, "memory", IcebergConstants.WAREHOUSE, "/tmp/wh");
  }

  private static IcebergConfig memoryConfig() {
    return new IcebergConfig(memoryProperties());
  }
}
