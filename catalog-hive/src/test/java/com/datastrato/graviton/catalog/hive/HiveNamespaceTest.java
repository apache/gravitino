package com.datastrato.graviton.catalog.hive;

import com.datastrato.graviton.Namespace;
import com.datastrato.graviton.NamespaceChange;
import com.datastrato.graviton.catalog.hive.miniHMS.MiniHiveMetastoreService;
import com.datastrato.graviton.meta.AuditInfo;
import com.google.common.collect.Maps;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import org.junit.Assert;
import org.junit.jupiter.api.Test;

public class HiveNamespaceTest extends MiniHiveMetastoreService {
  HiveCatalog initHiveCatalog() {
    AuditInfo auditInfo =
        new AuditInfo.Builder().withCreator("creator").withCreateTime(Instant.now()).build();

    HiveCatalog hiveCatalog =
        new HiveCatalog.Builder()
            .withId(1L)
            .withName("catalog")
            .withNamespace(Namespace.of("lakehouse"))
            .withType(HiveCatalog.Type.RELATIONAL)
            .withMetalakeId(1L)
            .withAuditInfo(auditInfo)
            .withHiveConf(metastore.hiveConf())
            .build();

    hiveCatalog.initialize(null);
    return hiveCatalog;
  }

  @Test
  public void createNamespaces() {
    HiveCatalog hiveCatalog = initHiveCatalog();

    Namespace namespace = Namespace.of(genRandomName());
    Map<String, String> properties = Maps.newHashMap();
    properties.put("key1", "val1");
    properties.put("key2", "val2");
    hiveCatalog.createNamespace(namespace, properties);
    Assert.assertTrue(hiveCatalog.namespaceExists(namespace));
  }

  @Test
  public void alterNamespaces() {
    HiveCatalog hiveCatalog = initHiveCatalog();

    Namespace namespace = Namespace.of(genRandomName());
    Map<String, String> properties = Maps.newHashMap();
    properties.put("key1", "val1");
    properties.put("key2", "val2");
    hiveCatalog.createNamespace(namespace, properties);
    Assert.assertTrue(hiveCatalog.namespaceExists(namespace));

    Map<String, String> properties1 = hiveCatalog.loadNamespaceMetadata(namespace);
    Assert.assertTrue(properties1.get("key1").equals("val1"));
    Assert.assertTrue(properties1.get("key2").equals("val2"));

    hiveCatalog.alterNamespace(namespace, NamespaceChange.removeProperty("key1"));
    hiveCatalog.alterNamespace(namespace, NamespaceChange.setProperty("key2", "val2-alter"));
    Map<String, String> properties2 = hiveCatalog.loadNamespaceMetadata(namespace);
    Assert.assertFalse(properties2.containsKey("key1"));
    Assert.assertTrue(properties2.get("key2").equals("val2-alter"));

    hiveCatalog.alterNamespace(
        namespace,
        NamespaceChange.setProperty("key3", "val3"),
        NamespaceChange.setProperty("key4", "val4"));
    Map<String, String> properties3 = hiveCatalog.loadNamespaceMetadata(namespace);
    Assert.assertTrue(properties3.get("key3").equals("val3"));
    Assert.assertTrue(properties3.get("key4").equals("val4"));
  }

  @Test
  public void dropNamespaces() {
    HiveCatalog hiveCatalog = initHiveCatalog();

    Namespace namespace = Namespace.of(genRandomName());
    Map<String, String> properties = Maps.newHashMap();
    properties.put("key1", "val1");
    properties.put("key2", "val2");
    hiveCatalog.createNamespace(namespace, properties);

    Assert.assertTrue(hiveCatalog.namespaceExists(namespace));
    hiveCatalog.dropNamespace(namespace, true);
    Assert.assertFalse(hiveCatalog.namespaceExists(namespace));
  }

  private static String genRandomName() {
    return UUID.randomUUID().toString().replace("-", "");
  }
}
