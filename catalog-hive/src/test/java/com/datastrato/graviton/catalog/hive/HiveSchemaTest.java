package com.datastrato.graviton.catalog.hive;

import com.datastrato.graviton.NameIdentifier;
import com.datastrato.graviton.Namespace;
import com.datastrato.graviton.catalog.hive.miniHMS.MiniHiveMetastoreService;
import com.datastrato.graviton.catalog.hive.rel.HiveSchema;
import com.datastrato.graviton.meta.AuditInfo;
import com.datastrato.graviton.rel.SchemaChange;
import com.google.common.collect.Maps;
import java.time.Instant;
import java.util.Arrays;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class HiveSchemaTest extends MiniHiveMetastoreService {
  HiveCatalog initHiveCatalog() {
    AuditInfo auditInfo =
        new AuditInfo.Builder().withCreator("creator").withCreateTime(Instant.now()).build();

    HiveCatalog hiveCatalog =
        new HiveCatalog.Builder()
            .withId(1L)
            .withName("catalog")
            .withNamespace(Namespace.of("metalake"))
            .withType(HiveCatalog.Type.RELATIONAL)
            .withMetalakeId(1L)
            .withAuditInfo(auditInfo)
            .withHiveConf(metastore.hiveConf())
            .build();

    hiveCatalog.initialize(null);
    return hiveCatalog;
  }

  @Test
  public void testCreateHiveSchema() {
    HiveCatalog hiveCatalog = initHiveCatalog();

    NameIdentifier ident = NameIdentifier.of("metalake", hiveCatalog.name(), genRandomName());
    Map<String, String> properties = Maps.newHashMap();
    properties.put("key1", "val1");
    properties.put("key2", "val2");
    String comment = "comment";

    HiveSchema schema = hiveCatalog.createSchema(ident, comment, properties);
    Assertions.assertEquals(ident.name(), schema.name());
    Assertions.assertEquals(ident.namespace(), schema.namespace());
    Assertions.assertEquals(comment, schema.comment());
    Assertions.assertEquals(properties, schema.properties());

    Assertions.assertTrue(hiveCatalog.schemaExists(ident));

    NameIdentifier[] idents = hiveCatalog.listSchemas(ident.namespace());
    Assertions.assertTrue(Arrays.asList(idents).contains(ident));

    // Test illegal identifier
    NameIdentifier ident1 = NameIdentifier.of("metalake", hiveCatalog.name());
    Throwable exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> {
              hiveCatalog.createSchema(ident1, comment, properties);
            });
    Assertions.assertTrue(
        exception.getMessage().contains("Cannot support invalid namespace in Hive Metastore"));
  }

  @Test
  public void testAlterSchema() {
    HiveCatalog hiveCatalog = initHiveCatalog();

    NameIdentifier ident = NameIdentifier.of("metalake", hiveCatalog.name(), genRandomName());
    Map<String, String> properties = Maps.newHashMap();
    properties.put("key1", "val1");
    properties.put("key2", "val2");
    String comment = "comment";

    hiveCatalog.createSchema(ident, comment, properties);
    Assertions.assertTrue(hiveCatalog.schemaExists(ident));

    Map<String, String> properties1 = hiveCatalog.loadSchema(ident).properties();
    Assertions.assertEquals("val1", properties1.get("key1"));
    Assertions.assertEquals("val2", properties1.get("key2"));

    hiveCatalog.alterSchema(
        ident, SchemaChange.removeProperty("key1"), SchemaChange.setProperty("key2", "val2-alter"));
    Map<String, String> properties2 = hiveCatalog.loadSchema(ident).properties();
    Assertions.assertFalse(properties2.containsKey("key1"));
    Assertions.assertEquals("val2-alter", properties2.get("key2"));

    hiveCatalog.alterSchema(
        ident, SchemaChange.setProperty("key3", "val3"), SchemaChange.setProperty("key4", "val4"));
    Map<String, String> properties3 = hiveCatalog.loadSchema(ident).properties();
    Assertions.assertEquals("val3", properties3.get("key3"));
    Assertions.assertEquals("val4", properties3.get("key4"));
  }

  @Test
  public void testDropSchema() {
    HiveCatalog hiveCatalog = initHiveCatalog();

    NameIdentifier ident = NameIdentifier.of("metalake", hiveCatalog.name(), genRandomName());
    Map<String, String> properties = Maps.newHashMap();
    properties.put("key1", "val1");
    properties.put("key2", "val2");
    String comment = "comment";

    hiveCatalog.createSchema(ident, comment, properties);
    Assertions.assertTrue(hiveCatalog.schemaExists(ident));
    hiveCatalog.dropSchema(ident, true);
    Assertions.assertFalse(hiveCatalog.schemaExists(ident));
  }

  private static String genRandomName() {
    return UUID.randomUUID().toString().replace("-", "");
  }
}
