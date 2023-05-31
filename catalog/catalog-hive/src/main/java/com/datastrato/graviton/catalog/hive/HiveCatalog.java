package com.datastrato.graviton.catalog.hive;

import com.datastrato.graviton.Config;
import com.datastrato.graviton.catalog.hive.dyn.DynMethods;
import com.datastrato.graviton.meta.BaseCatalog;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaHookLoader;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.RetryingMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HiveCatalog extends BaseCatalog {
  public static final Logger LOG = LoggerFactory.getLogger(HiveCatalog.class);

  private static final DynMethods.StaticMethod GET_HIVE_CLIENT =
      DynMethods.builder("getProxy")
          .impl(
              RetryingMetaStoreClient.class,
              HiveConf.class,
              HiveMetaHookLoader.class,
              String.class) // Hive 1 and 2
          .impl(
              RetryingMetaStoreClient.class,
              Configuration.class,
              HiveMetaHookLoader.class,
              String.class) // Hive 3
          .buildStatic();
  public IMetaStoreClient client;
  private HiveConf hiveConf;

  private IMetaStoreClient initHiveClient() throws MetaException {
    try {
      try {
        return GET_HIVE_CLIENT.invoke(
            hiveConf, (HiveMetaHookLoader) tbl -> null, HiveMetaStoreClient.class.getName());
      } catch (RuntimeException e) {
        // any MetaException would be wrapped into RuntimeException during reflection, so let's
        // double-check type here
        if (e.getCause() instanceof MetaException) {
          throw (MetaException) e.getCause();
        }
        throw e;
      }
    } catch (MetaException e) {
      throw new RuntimeException("Failed to connect to Hive Metastore", e);
    } catch (Throwable t) {
      if (t.getMessage().contains("Another instance of Derby may have already booted")) {
        throw new RuntimeException(
            "Failed to start an embedded metastore because embedded "
                + "Derby supports only one client at a time. To fix this, "
                + "use a metastore that supports multiple clients.",
            t);
      }

      throw new RuntimeException("Failed to connect to Hive Metastore", t);
    }
  }

  @Override
  public void close() {
    try {
      client.close();
    } catch (Exception e) {
      LOG.error("Error closing Hive client", e);
    }
  }

  @Override
  public void initialize(Config config) throws RuntimeException {
    try {
      this.client = initHiveClient();
    } catch (MetaException e) {
      throw new RuntimeException(e);
    }
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
