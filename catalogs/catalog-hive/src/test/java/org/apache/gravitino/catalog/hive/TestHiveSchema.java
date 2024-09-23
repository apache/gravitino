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

import static org.apache.gravitino.catalog.hive.HiveCatalogPropertiesMeta.METASTORE_URIS;
import static org.apache.gravitino.connector.BaseCatalog.CATALOG_BYPASS_PREFIX;

import com.google.common.collect.Maps;
import java.time.Instant;
import java.util.Arrays;
import java.util.Map;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.Schema;
import org.apache.gravitino.SchemaChange;
import org.apache.gravitino.exceptions.SchemaAlreadyExistsException;
import org.apache.gravitino.hive.hms.MiniHiveMetastoreService;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.CatalogEntity;
import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestHiveSchema extends MiniHiveMetastoreService {

  private HiveCatalog initHiveCatalog() {
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
    conf.put(METASTORE_URIS, hiveConf.get(HiveConf.ConfVars.METASTOREURIS.varname));
    conf.put(
        CATALOG_BYPASS_PREFIX + HiveConf.ConfVars.METASTOREWAREHOUSE.varname,
        hiveConf.get(HiveConf.ConfVars.METASTOREWAREHOUSE.varname));
    conf.put(
        CATALOG_BYPASS_PREFIX
            + HiveConf.ConfVars.METASTORE_DISALLOW_INCOMPATIBLE_COL_TYPE_CHANGES.varname,
        hiveConf.get(HiveConf.ConfVars.METASTORE_DISALLOW_INCOMPATIBLE_COL_TYPE_CHANGES.varname));

    conf.put(
        CATALOG_BYPASS_PREFIX + HiveConf.ConfVars.HIVE_IN_TEST.varname,
        hiveConf.get(HiveConf.ConfVars.HIVE_IN_TEST.varname));
    metastore.hiveConf().iterator().forEachRemaining(e -> conf.put(e.getKey(), e.getValue()));
    HiveCatalog hiveCatalog = new HiveCatalog().withCatalogConf(conf).withCatalogEntity(entity);

    return hiveCatalog;
  }

  @Test
  public void testCreateHiveSchema() {
    HiveCatalog hiveCatalog = initHiveCatalog();

    HiveCatalogOperations catalogOperations = (HiveCatalogOperations) hiveCatalog.ops();

    NameIdentifier ident = NameIdentifier.of("metalake", hiveCatalog.name(), genRandomName());
    Map<String, String> properties = Maps.newHashMap();
    properties.put("key1", "val1");
    properties.put("key2", "val2");
    String comment = "comment";

    Schema schema = catalogOperations.createSchema(ident, comment, properties);
    Assertions.assertEquals(ident.name(), schema.name());
    Assertions.assertEquals(comment, schema.comment());
    Assertions.assertEquals(properties, schema.properties());

    Assertions.assertTrue(catalogOperations.schemaExists(ident));

    NameIdentifier[] idents = catalogOperations.listSchemas(ident.namespace());
    Assertions.assertTrue(Arrays.asList(idents).contains(ident));

    Schema loadedSchema = catalogOperations.loadSchema(ident);
    Assertions.assertEquals(schema.auditInfo().creator(), loadedSchema.auditInfo().creator());
    Assertions.assertNull(loadedSchema.auditInfo().createTime());
    Assertions.assertEquals("val1", loadedSchema.properties().get("key1"));
    Assertions.assertEquals("val2", loadedSchema.properties().get("key2"));

    // Test schema already exists
    Throwable exception =
        Assertions.assertThrows(
            SchemaAlreadyExistsException.class,
            () -> {
              catalogOperations.createSchema(ident, comment, properties);
            });
    Assertions.assertTrue(exception.getMessage().contains("already exists in Hive Metastore"));
  }

  @Test
  public void testAlterSchema() {
    HiveCatalog hiveCatalog = initHiveCatalog();
    HiveCatalogOperations catalogOperations = (HiveCatalogOperations) hiveCatalog.ops();

    NameIdentifier ident = NameIdentifier.of("metalake", hiveCatalog.name(), genRandomName());
    Map<String, String> properties = Maps.newHashMap();
    properties.put("key1", "val1");
    properties.put("key2", "val2");
    String comment = "comment";

    Schema createdSchema = catalogOperations.createSchema(ident, comment, properties);
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

    Assertions.assertEquals(
        createdSchema.auditInfo().creator(), alteredSchema.auditInfo().creator());
    Assertions.assertNull(alteredSchema.auditInfo().createTime());
    Assertions.assertNull(alteredSchema.auditInfo().lastModifier());
    Assertions.assertNull(alteredSchema.auditInfo().lastModifiedTime());

    catalogOperations.alterSchema(
        ident, SchemaChange.setProperty("key3", "val3"), SchemaChange.setProperty("key4", "val4"));
    Schema alteredSchema1 = catalogOperations.loadSchema(ident);
    Map<String, String> properties3 = alteredSchema1.properties();
    Assertions.assertEquals("val3", properties3.get("key3"));
    Assertions.assertEquals("val4", properties3.get("key4"));

    Assertions.assertEquals(
        createdSchema.auditInfo().creator(), alteredSchema1.auditInfo().creator());
    Assertions.assertNull(alteredSchema1.auditInfo().createTime());
    Assertions.assertNull(alteredSchema1.auditInfo().lastModifier());
    Assertions.assertNull(alteredSchema1.auditInfo().lastModifiedTime());
  }

  @Test
  public void testDropSchema() {
    HiveCatalog hiveCatalog = initHiveCatalog();
    HiveCatalogOperations catalogOperations = (HiveCatalogOperations) hiveCatalog.ops();

    NameIdentifier ident = NameIdentifier.of("metalake", hiveCatalog.name(), genRandomName());
    Map<String, String> properties = Maps.newHashMap();
    properties.put("key1", "val1");
    properties.put("key2", "val2");
    String comment = "comment";

    catalogOperations.createSchema(ident, comment, properties);
    Assertions.assertTrue(catalogOperations.schemaExists(ident));
    catalogOperations.dropSchema(ident, true);
    Assertions.assertFalse(catalogOperations.schemaExists(ident));
    Assertions.assertFalse(
        catalogOperations.dropSchema(ident, true), "schema should not be exists");
  }
}
