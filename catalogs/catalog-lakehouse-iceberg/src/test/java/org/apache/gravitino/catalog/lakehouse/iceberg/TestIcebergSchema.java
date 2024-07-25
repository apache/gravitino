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

import com.google.common.collect.Maps;
import java.time.Instant;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.Schema;
import org.apache.gravitino.SchemaChange;
import org.apache.gravitino.catalog.PropertiesMetadataHelpers;
import org.apache.gravitino.connector.PropertiesMetadata;
import org.apache.gravitino.connector.SupportsSchemas;
import org.apache.gravitino.exceptions.SchemaAlreadyExistsException;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.CatalogEntity;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestIcebergSchema {

  private static final String META_LAKE_NAME = "metalake";

  private static final String COMMENT_VALUE = "comment";

  private static AuditInfo AUDIT_INFO =
      AuditInfo.builder().withCreator("testIcebergUser").withCreateTime(Instant.now()).build();

  @Test
  public void testCreateIcebergSchema() {
    IcebergCatalog icebergCatalog = initIcebergCatalog("testCreateIcebergSchema");
    IcebergCatalogOperations catalogOperations = (IcebergCatalogOperations) icebergCatalog.ops();

    NameIdentifier ident = NameIdentifier.of("metalake", icebergCatalog.name(), "test");
    Map<String, String> properties = Maps.newHashMap();
    properties.put("key1", "val1");
    properties.put("key2", "val2");

    Schema schema = catalogOperations.createSchema(ident, COMMENT_VALUE, properties);
    Assertions.assertEquals(ident.name(), schema.name());
    Assertions.assertEquals(COMMENT_VALUE, schema.comment());
    Assertions.assertEquals(properties, schema.properties());

    Assertions.assertTrue(catalogOperations.schemaExists(ident));

    Set<String> names =
        Arrays.stream(catalogOperations.listSchemas(ident.namespace()))
            .map(NameIdentifier::name)
            .collect(Collectors.toSet());
    Assertions.assertTrue(names.contains(ident.name()));

    // Test schema already exists
    SupportsSchemas schemas = catalogOperations;
    Throwable exception =
        Assertions.assertThrows(
            SchemaAlreadyExistsException.class,
            () -> {
              schemas.createSchema(ident, COMMENT_VALUE, properties);
            });
    Assertions.assertTrue(exception.getMessage().contains("already exists"));
  }

  @Test
  public void testListSchema() {
    IcebergCatalog icebergCatalog = initIcebergCatalog("testListIcebergSchema");
    IcebergCatalogOperations catalogOperations = (IcebergCatalogOperations) icebergCatalog.ops();
    NameIdentifier ident = NameIdentifier.of("metalake", icebergCatalog.name(), "test");
    catalogOperations.createSchema(ident, COMMENT_VALUE, Maps.newHashMap());

    NameIdentifier[] schemas = catalogOperations.listSchemas(ident.namespace());
    Assertions.assertEquals(1, schemas.length);
    Assertions.assertEquals(ident.name(), schemas[0].name());
    Assertions.assertEquals(ident.namespace(), schemas[0].namespace());
  }

  @Test
  public void testAlterSchema() {
    IcebergCatalog icebergCatalog = initIcebergCatalog("testAlterSchema");
    IcebergCatalogOperations catalogOperations = (IcebergCatalogOperations) icebergCatalog.ops();

    NameIdentifier ident = NameIdentifier.of("metalake", icebergCatalog.name(), "test");
    Map<String, String> properties = Maps.newHashMap();
    properties.put("key1", "val1");
    properties.put("key2", "val2");

    catalogOperations.createSchema(ident, COMMENT_VALUE, properties);
    Assertions.assertTrue(catalogOperations.schemaExists(ident));

    Map<String, String> properties1 = catalogOperations.loadSchema(ident).properties();
    Assertions.assertEquals("val1", properties1.get("key1"));
    Assertions.assertEquals("val2", properties1.get("key2"));

    catalogOperations.alterSchema(
        ident, SchemaChange.removeProperty("key1"), SchemaChange.setProperty("key2", "val2-alter"));
    Schema alteredSchema = catalogOperations.loadSchema(ident);
    Map<String, String> properties2 = alteredSchema.properties();
    Assertions.assertFalse(properties2.containsKey("key1"));
    Assertions.assertEquals("val2-alter", properties2.get("key2"));

    catalogOperations.alterSchema(
        ident, SchemaChange.setProperty("key3", "val3"), SchemaChange.setProperty("key4", "val4"));
    Schema alteredSchema1 = catalogOperations.loadSchema(ident);
    Map<String, String> properties3 = alteredSchema1.properties();
    Assertions.assertEquals("val3", properties3.get("key3"));
    Assertions.assertEquals("val4", properties3.get("key4"));
  }

  @Test
  public void testDropSchema() {
    IcebergCatalog icebergCatalog = initIcebergCatalog("testDropSchema");
    IcebergCatalogOperations catalogOperations = (IcebergCatalogOperations) icebergCatalog.ops();

    NameIdentifier ident = NameIdentifier.of("metalake", icebergCatalog.name(), "test");
    Map<String, String> properties = Maps.newHashMap();
    properties.put("key1", "val1");
    properties.put("key2", "val2");

    catalogOperations.createSchema(ident, COMMENT_VALUE, properties);
    Assertions.assertTrue(catalogOperations.schemaExists(ident));
    catalogOperations.dropSchema(ident, false);
    Assertions.assertFalse(catalogOperations.schemaExists(ident));

    Assertions.assertFalse(catalogOperations.dropSchema(ident, false));

    Throwable exception =
        Assertions.assertThrows(
            IllegalArgumentException.class, () -> catalogOperations.dropSchema(ident, true));
    Assertions.assertTrue(
        exception.getMessage().contains("Iceberg does not support cascading delete operations"));
  }

  @Test
  void testSchemaProperty() {
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
      ops.initialize(conf, entity.toCatalogInfo(), TestIcebergCatalog.ICEBERG_PROPERTIES_METADATA);
      Map<String, String> map = Maps.newHashMap();
      map.put(IcebergSchemaPropertiesMetadata.COMMENT, "test");
      PropertiesMetadata metadata =
          TestIcebergCatalog.ICEBERG_PROPERTIES_METADATA.schemaPropertiesMetadata();

      IllegalArgumentException illegalArgumentException =
          Assertions.assertThrows(
              IllegalArgumentException.class,
              () -> {
                PropertiesMetadataHelpers.validatePropertyForCreate(metadata, map);
              });
      Assertions.assertTrue(
          illegalArgumentException.getMessage().contains(IcebergSchemaPropertiesMetadata.COMMENT));
    }
  }

  private IcebergCatalog initIcebergCatalog(String name) {
    CatalogEntity entity =
        CatalogEntity.builder()
            .withId(1L)
            .withName(name)
            .withNamespace(Namespace.of(META_LAKE_NAME))
            .withType(IcebergCatalog.Type.RELATIONAL)
            .withProvider("iceberg")
            .withAuditInfo(AUDIT_INFO)
            .build();

    Map<String, String> conf = Maps.newHashMap();
    return new IcebergCatalog().withCatalogConf(conf).withCatalogEntity(entity);
  }
}
