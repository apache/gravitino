package com.datastrato.graviton.catalog.hive;

import com.datastrato.graviton.Namespace;
import com.datastrato.graviton.catalog.hive.miniHMS.MiniHiveMetastoreService;
import com.datastrato.graviton.meta.AuditInfo;
import java.time.Instant;
import java.util.List;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.thrift.TException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class HiveCatalogTest extends MiniHiveMetastoreService {
  @Test
  public void listDatabases() throws TException, InterruptedException {
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

    List<String> dbs = hiveCatalog.clientPool.run(client -> client.getAllDatabases());
    Assertions.assertEquals(2, dbs.size());
    Assertions.assertTrue(dbs.contains("default"));
    Assertions.assertTrue(dbs.contains(DB_NAME));
  }
}
