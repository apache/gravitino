package com.datastrato.graviton.connector.hive;

import com.datastrato.graviton.connector.hive.dyn.DynMethods;
import com.datastrato.graviton.connectors.core.MetaOperation;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaHookLoader;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.RetryingMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class HiveMetaOperation implements MetaOperation<Database> {
  public static final Logger LOGGER = LoggerFactory.getLogger(HiveMetaOperation.class);

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

  public final IMetaStoreClient client;
  private final HiveConf hiveConf;

  public HiveMetaOperation(Configuration conf) {
    this.hiveConf = new HiveConf(conf, HiveMetaOperation.class);
    this.hiveConf.addResource(conf);
    try {
      this.client = initHiveClient();
    } catch (MetaException e) {
      throw new RuntimeException(e);
    }
  }

  public IMetaStoreClient initHiveClient() throws MetaException {
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
                        + "use a metastore that supports multiple clients.", t);
      }

      throw new RuntimeException("Failed to connect to Hive Metastore", t);
    }
  }

  @Override
  public void close() {
    try {
      client.close();
    } catch (Exception e) {
      LOGGER.error("Error closing Hive client", e);
    }
  }

  @Override
  public Optional<Database> getDatabase(String databaseName) throws Exception {
    return Optional.of(client.getDatabase(databaseName));
  }

  @Override
  public List<String> getAllDatabases(String filter) throws Exception {
    if (filter == null || filter.isEmpty()) {
      return new ArrayList<>(client.getAllDatabases());
    }
    return client.getAllDatabases()
            .stream()
            .filter(n -> n.matches(filter)).toList();
  }

  @Override
  public void createDatabase(Database database) throws Exception {
    try {
      client.createDatabase(database);
    } catch (TException e) {
      LOGGER.error(e.getMessage());
      e.printStackTrace();
      throw e;
    }
  }

  @Override
  public void dropDatabase(String databaseName, boolean deleteData) throws Exception {
    client.dropDatabase(databaseName, deleteData, true);
  }

  @Override
  public void renameDatabase(String databaseName, String newDatabaseName) throws Exception {
    client.alterDatabase(databaseName, new Database(newDatabaseName, null, null, null));;
  }
}
