package com.datastrato.graviton.catalog.hive;

import com.datastrato.graviton.Config;
import com.datastrato.graviton.meta.BaseCatalog;
import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HiveCatalog extends BaseCatalog {
  public static final Logger LOG = LoggerFactory.getLogger(HiveCatalog.class);

  @VisibleForTesting HiveClientPool clientPool;

  private HiveConf hiveConf;

  @Override
  public void close() {
    if (clientPool != null) {
      clientPool.close();
    }
  }

  @Override
  public void initialize(Config config) throws RuntimeException {
    // todo(xun): add hive client pool size in config
    this.clientPool = new HiveClientPool(1, hiveConf);
  }

  public static class Builder extends BaseCatalog.BaseCatalogBuilder<Builder, HiveCatalog> {
    HiveConf hiveConf;

    HiveCatalog hiveCatalog = new HiveCatalog();

    @Override
    protected HiveCatalog internalBuild() {
      hiveCatalog.id = id;
      hiveCatalog.lakehouseId = lakehouseId;
      hiveCatalog.name = name;
      hiveCatalog.namespace = namespace;
      hiveCatalog.type = type;
      hiveCatalog.comment = comment;
      hiveCatalog.properties = properties;
      hiveCatalog.auditInfo = auditInfo;
      hiveCatalog.hiveConf = hiveConf;

      return hiveCatalog;
    }

    public Builder withHiveConf(Configuration conf) {
      this.hiveConf = new HiveConf(conf, HiveCatalog.class);
      this.hiveConf.addResource(conf);
      return this;
    }
  }
}
