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
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.auxiliary.AuxiliaryIcebergCatalogBridge;
import org.apache.gravitino.connector.CatalogInfo;
import org.apache.gravitino.connector.CatalogOperations;
import org.apache.gravitino.connector.HasPropertyMetadata;
import org.apache.gravitino.exceptions.NoSuchCatalogException;
import org.apache.gravitino.iceberg.common.IcebergConfig;
import org.apache.gravitino.iceberg.common.ops.IcebergCatalogWrapper;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.CatalogEntity;
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
  void memoryCatalogKeepsItsOwnClientWhenAuxIrcIsRunning() {
    AtomicInteger creates = new AtomicInteger();
    operationsFactory =
        () -> {
          creates.incrementAndGet();
          return IcebergCatalogOperations.createForLocalIrc(
              catalogName -> {
                throw new NoSuchCatalogException("unused %s", catalogName);
              });
        };
    AuxiliaryIcebergCatalogBridge.register(operationsFactory);

    IcebergCatalogOperations metalakeOps = new IcebergCatalogOperations();
    metalakeOps.initialize(
        memoryProperties(), catalogInfo("orders", memoryProperties()), metadata());
    Assertions.assertNotNull(metalakeOps.backend);
    NameIdentifier[] schemas = metalakeOps.listSchemas(Namespace.of("metalake", "orders"));
    Assertions.assertEquals(0, schemas.length);
    Assertions.assertEquals(0, creates.get());
    metalakeOps.close();
  }

  @Test
  void restCatalogBorrowsWarehouseWrapperFromAuxIrc() {
    IcebergCatalogWrapper target = new IcebergCatalogWrapper(memoryConfig());
    AtomicReference<String> lookedUp = new AtomicReference<>();
    operationsFactory =
        () ->
            IcebergCatalogOperations.createForLocalIrc(
                catalogName -> {
                  lookedUp.set(catalogName);
                  if ("orders".equals(catalogName)) {
                    return target;
                  }
                  throw new NoSuchCatalogException("missing %s", catalogName);
                });
    AuxiliaryIcebergCatalogBridge.register(operationsFactory);

    Map<String, String> restProperties =
        ImmutableMap.of(
            IcebergConstants.CATALOG_BACKEND,
            "rest",
            IcebergConstants.URI,
            "http://remote.example:9001/iceberg",
            IcebergConstants.WAREHOUSE,
            "orders");
    CatalogEntity entity =
        CatalogEntity.builder()
            .withId(1L)
            .withName("rest_catalog")
            .withNamespace(Namespace.of("metalake"))
            .withType(Catalog.Type.RELATIONAL)
            .withProvider("lakehouse-iceberg")
            .withAuditInfo(
                AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build())
            .withProperties(restProperties)
            .build();
    IcebergCatalog catalog =
        new IcebergCatalog().withCatalogConf(restProperties).withCatalogEntity(entity);
    CatalogOperations ops = catalog.ops();
    Assertions.assertTrue(ops instanceof IcebergCatalogOperations);
    IcebergCatalogOperations icebergOps = (IcebergCatalogOperations) ops;
    Assertions.assertSame(target, icebergOps.backend.wrapper());
    Assertions.assertEquals("orders", lookedUp.get());
    NameIdentifier[] schemas = icebergOps.listSchemas(Namespace.of("metalake", "rest_catalog"));
    Assertions.assertEquals(0, schemas.length);
    icebergOps.close();
    Assertions.assertDoesNotThrow(target::close);
  }

  private static CatalogInfo catalogInfo(String name, Map<String, String> properties) {
    return new CatalogInfo(
        1L,
        name,
        Catalog.Type.RELATIONAL,
        "lakehouse-iceberg",
        null,
        properties,
        AuditInfo.EMPTY,
        Namespace.of("metalake"));
  }

  private static HasPropertyMetadata metadata() {
    HasPropertyMetadata metadata = Mockito.mock(HasPropertyMetadata.class);
    Mockito.when(metadata.catalogPropertiesMetadata())
        .thenReturn(new IcebergCatalogPropertiesMetadata());
    return metadata;
  }

  private static Map<String, String> memoryProperties() {
    return ImmutableMap.of(
        IcebergConstants.CATALOG_BACKEND, "memory", IcebergConstants.WAREHOUSE, "/tmp/wh");
  }

  private static IcebergConfig memoryConfig() {
    return new IcebergConfig(memoryProperties());
  }
}
