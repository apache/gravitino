package com.datastrato.graviton.catalog.hive;

import com.datastrato.graviton.Namespace;
import com.datastrato.graviton.catalog.hive.miniHMS.MiniHiveMetastoreService;
import com.datastrato.graviton.meta.AuditInfo;
import com.datastrato.graviton.meta.CatalogEntity;
import com.google.common.collect.Maps;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.thrift.TException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class HiveCatalogTest extends MiniHiveMetastoreService {
  @Test
  public void testListDatabases() throws TException, InterruptedException {
    AuditInfo auditInfo =
        new AuditInfo.Builder().withCreator("creator").withCreateTime(Instant.now()).build();

    CatalogEntity entity =
        new CatalogEntity.Builder()
            .withId(1L)
            .withName("catalog")
            .withNamespace(Namespace.of("metalake"))
            .withType(HiveCatalog.Type.RELATIONAL)
            .withMetalakeId(1L)
            .withAuditInfo(auditInfo)
            .build();

    Map<String, String> conf = Maps.newHashMap();
    metastore.hiveConf().forEach(e -> conf.put(e.getKey(), e.getValue()));

    try (HiveCatalogOperations ops = new HiveCatalogOperations(entity)) {
      ops.initialize(conf);
      List<String> dbs = ops.clientPool.run(IMetaStoreClient::getAllDatabases);
      Assertions.assertEquals(2, dbs.size());
      Assertions.assertTrue(dbs.contains("default"));
      Assertions.assertTrue(dbs.contains(DB_NAME));
    }
  }
}
