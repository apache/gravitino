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
package org.apache.gravitino.catalog.hive;

import static org.apache.gravitino.catalog.hive.HiveCatalog.CATALOG_PROPERTIES_METADATA;
import static org.apache.gravitino.catalog.hive.HiveCatalog.SCHEMA_PROPERTIES_METADATA;
import static org.apache.gravitino.catalog.hive.HiveCatalog.TABLE_PROPERTIES_METADATA;
import static org.apache.gravitino.catalog.hive.HiveCatalogPropertiesMetadata.METASTORE_URIS;

import com.google.common.collect.Maps;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.catalog.PropertiesMetadataHelpers;
import org.apache.gravitino.connector.HasPropertyMetadata;
import org.apache.gravitino.connector.PropertiesMetadata;
import org.apache.gravitino.hive.hms.MiniHiveMetastoreService;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.CatalogEntity;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.thrift.TException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestHiveCatalog extends MiniHiveMetastoreService {
  public static final HasPropertyMetadata HIVE_PROPERTIES_METADATA =
      new HasPropertyMetadata() {
        @Override
        public PropertiesMetadata tablePropertiesMetadata() throws UnsupportedOperationException {
          return TABLE_PROPERTIES_METADATA;
        }

        @Override
        public PropertiesMetadata catalogPropertiesMetadata() throws UnsupportedOperationException {
          return CATALOG_PROPERTIES_METADATA;
        }

        @Override
        public PropertiesMetadata schemaPropertiesMetadata() throws UnsupportedOperationException {
          return SCHEMA_PROPERTIES_METADATA;
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
  public void testListDatabases() throws TException, InterruptedException {
    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("creator").withCreateTime(Instant.now()).build();

    CatalogEntity entity =
        CatalogEntity.builder()
            .withId(1L)
            .withName("catalog")
            .withNamespace(Namespace.of("metalake"))
            .withType(HiveCatalog.Type.RELATIONAL)
            .withProvider("hive")
            .withAuditInfo(auditInfo)
            .build();

    Map<String, String> conf = Maps.newHashMap();
    metastore.hiveConf().forEach(e -> conf.put(e.getKey(), e.getValue()));

    try (HiveCatalogOperations ops = new HiveCatalogOperations()) {
      ops.initialize(conf, entity.toCatalogInfo(), HIVE_PROPERTIES_METADATA);
      List<String> dbs = ops.clientPool.run(IMetaStoreClient::getAllDatabases);
      Assertions.assertEquals(2, dbs.size());
      Assertions.assertTrue(dbs.contains("default"));
      Assertions.assertTrue(dbs.contains(DB_NAME));
    }
  }

  @Test
  void testCatalogProperty() {
    Map<String, String> properties = Maps.newHashMap();
    metastore.hiveConf().forEach(e -> properties.put(e.getKey(), e.getValue()));
    PropertiesMetadata catalogPropertiesMetadata =
        HIVE_PROPERTIES_METADATA.catalogPropertiesMetadata();

    Assertions.assertDoesNotThrow(
        () -> {
          Map<String, String> map = Maps.newHashMap();
          map.put(METASTORE_URIS, "/tmp");
          PropertiesMetadataHelpers.validatePropertyForCreate(catalogPropertiesMetadata, map);
        });

    Throwable throwable =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                PropertiesMetadataHelpers.validatePropertyForCreate(
                    catalogPropertiesMetadata, properties));

    Assertions.assertTrue(
        throwable
            .getMessage()
            .contains(
                String.format(
                    "Properties or property prefixes are required and must be set: [%s]",
                    METASTORE_URIS)));
  }
}
