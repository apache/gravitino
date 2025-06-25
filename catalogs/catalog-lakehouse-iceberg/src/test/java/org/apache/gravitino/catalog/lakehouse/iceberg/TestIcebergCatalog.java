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

import static org.apache.gravitino.catalog.lakehouse.iceberg.IcebergCatalog.CATALOG_PROPERTIES_META;
import static org.apache.gravitino.catalog.lakehouse.iceberg.IcebergCatalog.SCHEMA_PROPERTIES_META;
import static org.apache.gravitino.catalog.lakehouse.iceberg.IcebergCatalog.TABLE_PROPERTIES_META;

import com.google.common.collect.Maps;
import java.time.Instant;
import java.util.Map;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.catalog.PropertiesMetadataHelpers;
import org.apache.gravitino.connector.CatalogOperations;
import org.apache.gravitino.connector.HasPropertyMetadata;
import org.apache.gravitino.connector.PropertiesMetadata;
import org.apache.gravitino.iceberg.common.ops.IcebergCatalogWrapper;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.CatalogEntity;
import org.apache.iceberg.rest.responses.ListNamespacesResponse;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestIcebergCatalog {
  static final HasPropertyMetadata ICEBERG_PROPERTIES_METADATA =
      new HasPropertyMetadata() {
        @Override
        public PropertiesMetadata tablePropertiesMetadata() throws UnsupportedOperationException {
          return TABLE_PROPERTIES_META;
        }

        @Override
        public PropertiesMetadata catalogPropertiesMetadata() throws UnsupportedOperationException {
          return CATALOG_PROPERTIES_META;
        }

        @Override
        public PropertiesMetadata schemaPropertiesMetadata() throws UnsupportedOperationException {
          return SCHEMA_PROPERTIES_META;
        }

        @Override
        public PropertiesMetadata filesetPropertiesMetadata() throws UnsupportedOperationException {
          throw new UnsupportedOperationException("Fileset properties are not supported");
        }

        @Override
        public PropertiesMetadata topicPropertiesMetadata() throws UnsupportedOperationException {
          throw new UnsupportedOperationException("Topic properties are not supported");
        }

        @Override
        public PropertiesMetadata modelPropertiesMetadata() throws UnsupportedOperationException {
          throw new UnsupportedOperationException("Model properties are not supported");
        }

        @Override
        public PropertiesMetadata modelVersionPropertiesMetadata()
            throws UnsupportedOperationException {
          throw new UnsupportedOperationException("Does not support model version properties");
        }
      };

  @Test
  public void testListDatabases() {
    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("creator").withCreateTime(Instant.now()).build();

    CatalogEntity entity =
        CatalogEntity.builder()
            .withId(1L)
            .withName("catalog")
            .withNamespace(Namespace.of("metalake"))
            .withType(IcebergCatalog.Type.RELATIONAL)
            .withProvider("iceberg")
            .withAuditInfo(auditInfo)
            .build();

    Map<String, String> conf = Maps.newHashMap();
    IcebergCatalog icebergCatalog =
        new IcebergCatalog().withCatalogConf(conf).withCatalogEntity(entity);
    CatalogOperations catalogOperations = icebergCatalog.ops();
    Assertions.assertTrue(catalogOperations instanceof IcebergCatalogOperations);

    IcebergCatalogWrapper icebergCatalogWrapper =
        ((IcebergCatalogOperations) catalogOperations).icebergCatalogWrapper;
    ListNamespacesResponse listNamespacesResponse =
        icebergCatalogWrapper.listNamespace(org.apache.iceberg.catalog.Namespace.empty());
    Assertions.assertTrue(listNamespacesResponse.namespaces().isEmpty());
  }

  @Test
  void testCatalogProperty() {
    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("creator").withCreateTime(Instant.now()).build();

    CatalogEntity entity =
        CatalogEntity.builder()
            .withId(1L)
            .withName("catalog")
            .withNamespace(Namespace.of("metalake"))
            .withType(IcebergCatalog.Type.RELATIONAL)
            .withProvider("iceberg")
            .withAuditInfo(auditInfo)
            .build();

    Map<String, String> conf = Maps.newHashMap();

    try (IcebergCatalogOperations ops = new IcebergCatalogOperations()) {
      ops.initialize(conf, entity.toCatalogInfo(), ICEBERG_PROPERTIES_METADATA);
      Map<String, String> map1 = Maps.newHashMap();
      map1.put(IcebergCatalogPropertiesMetadata.CATALOG_BACKEND, "test");
      PropertiesMetadata metadata = ICEBERG_PROPERTIES_METADATA.catalogPropertiesMetadata();
      Assertions.assertThrows(
          IllegalArgumentException.class,
          () -> {
            PropertiesMetadataHelpers.validatePropertyForCreate(metadata, map1);
          });

      Map<String, String> map2 = Maps.newHashMap();
      map2.put(IcebergCatalogPropertiesMetadata.CATALOG_BACKEND, "hive");
      map2.put(IcebergCatalogPropertiesMetadata.URI, "127.0.0.1");
      map2.put(IcebergCatalogPropertiesMetadata.WAREHOUSE, "test");
      Assertions.assertDoesNotThrow(
          () -> {
            PropertiesMetadataHelpers.validatePropertyForCreate(metadata, map2);
          });

      Map<String, String> map3 = Maps.newHashMap();
      Throwable throwable =
          Assertions.assertThrows(
              IllegalArgumentException.class,
              () -> PropertiesMetadataHelpers.validatePropertyForCreate(metadata, map3));

      Assertions.assertTrue(
          throwable.getMessage().contains(IcebergCatalogPropertiesMetadata.CATALOG_BACKEND));
    }
  }

  @Test
  void testCatalogInstanciation() {
    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("creator").withCreateTime(Instant.now()).build();

    CatalogEntity entity =
        CatalogEntity.builder()
            .withId(1L)
            .withName("catalog")
            .withNamespace(Namespace.of("metalake"))
            .withType(IcebergCatalog.Type.RELATIONAL)
            .withProvider("iceberg")
            .withAuditInfo(auditInfo)
            .build();

    Map<String, String> conf = Maps.newHashMap();

    try (IcebergCatalogOperations ops = new IcebergCatalogOperations()) {
      ops.initialize(conf, entity.toCatalogInfo(), ICEBERG_PROPERTIES_METADATA);
      Map<String, String> map1 = Maps.newHashMap();
      map1.put(IcebergCatalogPropertiesMetadata.CATALOG_BACKEND, "test");
      PropertiesMetadata metadata = ICEBERG_PROPERTIES_METADATA.catalogPropertiesMetadata();
      Assertions.assertThrows(
          IllegalArgumentException.class,
          () -> {
            PropertiesMetadataHelpers.validatePropertyForCreate(metadata, map1);
          });

      Map<String, String> map2 = Maps.newHashMap();
      map2.put(IcebergCatalogPropertiesMetadata.CATALOG_BACKEND, "rest");
      map2.put(IcebergCatalogPropertiesMetadata.URI, "127.0.0.1");
      Assertions.assertDoesNotThrow(
          () -> {
            PropertiesMetadataHelpers.validatePropertyForCreate(metadata, map2);
          });

      Map<String, String> map3 = Maps.newHashMap();
      Throwable throwable =
          Assertions.assertThrows(
              IllegalArgumentException.class,
              () -> PropertiesMetadataHelpers.validatePropertyForCreate(metadata, map3));

      Assertions.assertTrue(
          throwable.getMessage().contains(IcebergCatalogPropertiesMetadata.CATALOG_BACKEND));
    }
  }
}
