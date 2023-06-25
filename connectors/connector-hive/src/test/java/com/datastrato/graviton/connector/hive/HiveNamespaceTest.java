package com.datastrato.graviton.connector.hive;

import com.datastrato.graviton.Namespace;
import com.datastrato.graviton.connector.hive.miniHMS.MiniHiveMetastoreService;
import com.datastrato.graviton.meta.catalog.NamespaceChange;
import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.UUID;

public class HiveNamespaceTest extends MiniHiveMetastoreService {
  @Test
  public void createNamespaces() {
    HiveConnector hiveConnector = new HiveConnector(metastore.hiveConf());

    Namespace namespace = Namespace.of(genRandomName());
    Map<String, String> properties = Maps.newHashMap();
    properties.put("key1", "val1");
    properties.put("key2", "val2");
    hiveConnector.createNamespace(namespace, properties);
    Assert.assertTrue(hiveConnector.namespaceExists(namespace));
  }

  @Test
  public void alterNamespaces() {
    HiveConnector hiveConnector = new HiveConnector(metastore.hiveConf());

    Namespace namespace = Namespace.of(genRandomName());
    Map<String, String> properties = Maps.newHashMap();
    properties.put("key1", "val1");
    properties.put("key2", "val2");
    hiveConnector.createNamespace(namespace, properties);
    Assert.assertTrue(hiveConnector.namespaceExists(namespace));

    Map<String, String> properties1 = hiveConnector.loadNamespaceMetadata(namespace);
    Assert.assertTrue(properties1.get("key1").equals("val1"));
    Assert.assertTrue(properties1.get("key2").equals("val2"));

    hiveConnector.alterNamespace(namespace, NamespaceChange.removeProperty("key1"));
    hiveConnector.alterNamespace(namespace, NamespaceChange.setProperty("key2", "val2-alter"));
    Map<String, String> properties2 = hiveConnector.loadNamespaceMetadata(namespace);
    Assert.assertFalse(properties2.containsKey("key1"));
    Assert.assertTrue(properties2.get("key2").equals("val2-alter"));
  }

  @Test
  public void dropNamespaces() throws Exception {
    HiveConnector hiveConnector = new HiveConnector(metastore.hiveConf());

    Namespace namespace = Namespace.of(genRandomName());
    Map<String, String> properties = Maps.newHashMap();
    properties.put("key1", "val1");
    properties.put("key2", "val2");
    hiveConnector.createNamespace(namespace, properties);

    Assert.assertTrue(hiveConnector.namespaceExists(namespace));
    hiveConnector.dropNamespace(namespace, true);
    Assert.assertFalse(hiveConnector.namespaceExists(namespace));
  }

  private static String genRandomName() {
    return UUID.randomUUID().toString().replace("-", "");
  }
}
