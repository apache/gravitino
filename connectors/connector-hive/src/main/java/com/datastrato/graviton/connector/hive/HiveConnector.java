package com.datastrato.graviton.connector.hive;

import com.datastrato.graviton.Namespace;
import com.datastrato.graviton.connector.hive.dyn.DynMethods;
import com.datastrato.graviton.connector.hive.json.HiveDatabase;
import com.datastrato.graviton.connectors.core.Connector;
import com.datastrato.graviton.meta.catalog.*;
import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaHookLoader;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.RetryingMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class HiveConnector implements SupportsNamespaces, Connector {
  public static final Logger LOG = LoggerFactory.getLogger(HiveConnector.class);

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

  public HiveConnector(Configuration conf) {
    this.hiveConf = new HiveConf(conf, HiveConnector.class);
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
      LOG.error("Error closing Hive client", e);
    }
  }

  @Override
  public Namespace[] listNamespaces() throws NoSuchNamespaceException {
    return listNamespaces(Namespace.empty());
  }

  @Override
  public Namespace[] listNamespaces(Namespace namespace) throws NoSuchNamespaceException {
    if (!isValidateNamespace(namespace) && !namespace.isEmpty()) {
      throw new NoSuchNamespaceException("Namespace does not exist: " + namespace);
    }

    // hive namespace dose not have multi level namespace
    if (!namespace.isEmpty()) {
      return new Namespace[0];
    }

    try {
      List<Namespace> namespaces = client.getAllDatabases()
              .stream().map(Namespace::of)
              .collect(Collectors.toList());

      LOG.debug("Listing namespace {} returned tables: {}", namespace, namespaces);
      return namespaces.toArray(new Namespace[0]);
    } catch (TException e) {
      throw new RuntimeException(
              "Failed to list all namespace: " + namespace + " in Hive Metastore", e);
    }
  }

  @Override
  public boolean namespaceExists(Namespace namespace) {
    return Arrays.asList(listNamespaces()).contains(namespace);
  }

  @Override
  public void createNamespace(Namespace namespace, Map<String, String> metadata)
          throws NamespaceAlreadyExistsException {
    Preconditions.checkArgument(!namespace.isEmpty(),
            "Cannot create namespace with invalid name: %s", namespace);
    Preconditions.checkArgument(isValidateNamespace(namespace),
            "Cannot support multi part namespace in Hive Metastore: %s", namespace);

    try {
      HiveDatabase hiveDatabase = new HiveDatabase.Builder()
              .withConf(hiveConf)
              .withNamespace(namespace)
              .withMetadata(metadata)
              .build();

      client.createDatabase(hiveDatabase.getInnerDatabase());

      LOG.info("Created namespace: {}", namespace);
    } catch (AlreadyExistsException e) {
      throw new NamespaceAlreadyExistsException(String.format("Namespace '%s' already exists!", namespace));
    } catch (TException e) {
      throw new RuntimeException(
              "Failed to create namespace " + namespace + " in Hive Metastore", e);

    }
  }

  @Override
  public Map<String, String> loadNamespaceMetadata(Namespace namespace) throws NoSuchNamespaceException {
    if (!isValidateNamespace(namespace)) {
      throw new NoSuchNamespaceException("Namespace does not exist: %s", namespace);
    }

    try {
      Database database = client.getDatabase(namespace.level(0));
      HiveDatabase hiveDatabase = new HiveDatabase.Builder()
              .withHiveDatabase(database)
              .build();
      Map<String, String> metadata = hiveDatabase.convertToMetadata();

      LOG.debug("Loaded metadata for namespace {} found {}", namespace, metadata.keySet());
      return metadata;

    } catch (NoSuchObjectException | UnknownDBException e) {
      throw new NoSuchNamespaceException("Namespace does not exist: %s", namespace);

    } catch (TException e) {
      throw new RuntimeException(
              "Failed to list namespace under namespace: " + namespace + " in Hive Metastore", e);
    }
  }

  @Override
  public void alterNamespace(Namespace namespace, NamespaceChange change) throws NoSuchNamespaceException {
    Preconditions.checkArgument(!namespace.isEmpty(),
            "Cannot create namespace with invalid name: %s", namespace);
    Preconditions.checkArgument(isValidateNamespace(namespace),
            "Cannot support multi part namespace in Hive Metastore: %s", namespace);

    try {
      // load the database parameters
      Database database = client.getDatabase(namespace.level(0));
      HiveDatabase hiveDatabase = new HiveDatabase.Builder()
              .withHiveDatabase(database)
              .build();
      Map<String, String> metadata = hiveDatabase.convertToMetadata();
      LOG.debug("Loaded metadata for namespace {} found {}", namespace, metadata.keySet());

      if (change instanceof NamespaceChange.SetProperty) {
        metadata.put(((NamespaceChange.SetProperty) change).getProperty(),
                ((NamespaceChange.SetProperty) change).getValue());
      } else if (change instanceof NamespaceChange.RemoveProperty) {
        metadata.remove(((NamespaceChange.RemoveProperty) change).getProperty());
      } else {
        throw new RuntimeException("Unsupported namespace change type: " + change);
      }

      // alter the hive database parameters
      Database alteredDatabase = database.deepCopy();
      alteredDatabase.setParameters(metadata);
      client.alterDatabase(namespace.level(0), alteredDatabase);
      LOG.info("Altered namespace: {}", alteredDatabase);
      // todo(xun): hive dose not support renaming database name directly,
      //  perhaps we can use namespace to mapping the database names indirectly
    } catch (TException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean dropNamespace(Namespace namespace, boolean cascade)
          throws NonEmptyNamespaceException, NoSuchNamespaceException {
    if (!isValidateNamespace(namespace)) {
      return false;
    }

    try {
      client.dropDatabase(namespace.level(0), false, false, cascade);

      LOG.info("Dropped namespace: {}", namespace);
      return true;
    } catch (InvalidOperationException e) {
      throw new NamespaceNotEmptyException(
              String.format("Namespace %s is not empty. One or more tables exist.", namespace, e));
    } catch (NoSuchObjectException e) {
      return false;
    } catch (TException e) {
      throw new RuntimeException("Failed to drop namespace " + namespace + " in Hive Metastore", e);
    }
  }

  public static boolean isValidateNamespace(Namespace namespace) {
    return namespace.levels().length == 1;
  }
}
