package com.datastrato.graviton.catalog.hive;

import com.datastrato.graviton.*;
import com.datastrato.graviton.catalog.hive.json.HiveDatabase;
import com.datastrato.graviton.exceptions.NamespaceAlreadyExistsException;
import com.datastrato.graviton.exceptions.NoSuchNamespaceException;
import com.datastrato.graviton.exceptions.NonEmptyNamespaceException;
import com.datastrato.graviton.meta.BaseCatalog;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HiveCatalog extends BaseCatalog implements SupportsNamespaces {
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

  @Override
  public Namespace[] listNamespaces() throws NoSuchNamespaceException {
    return listNamespaces(Namespace.of("metalake", this.name));
  }

  @Override
  public Namespace[] listNamespaces(Namespace namespace) throws NoSuchNamespaceException {
    // hive namespace expression is `metalake.catalog.db`
    if (namespace.levels().length != 2) {
      throw new NoSuchNamespaceException("Namespace does not exist: " + namespace);
    }

    try {
      Namespace[] namespaces =
          clientPool
              .run(
                  client ->
                      client.getAllDatabases().stream()
                          .map(ns -> Namespace.of(namespace.level(0), namespace.level(1), ns)))
              .toArray(Namespace[]::new);

      LOG.debug("Listing namespace {} returned namespaces: {}", namespace, namespaces);
      return namespaces;
    } catch (TException e) {
      throw new RuntimeException(
          "Failed to list all namespace: " + namespace + " in Hive Metastore", e);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean namespaceExists(Namespace namespace) {
    try {
      clientPool.run(client -> client.getDatabase(getDbNameFromNamespace(namespace)));
    } catch (TException | InterruptedException e) {
      return false;
    }
    return true;
  }

  @Override
  public void createNamespace(Namespace namespace, Map<String, String> metadata)
      throws NamespaceAlreadyExistsException {
    Preconditions.checkArgument(
        !namespace.isEmpty(),
        String.format("Cannot create namespace with invalid name: %s", namespace));
    Preconditions.checkArgument(
        isValidateNamespace(namespace),
        String.format("Cannot support multi part namespace in Hive Metastore: %s", namespace));

    try {
      HiveDatabase hiveDatabase =
          new HiveDatabase.Builder()
              .withConf(hiveConf)
              .withNamespace(namespace)
              .withMetadata(metadata)
              .build();

      clientPool.run(
          client -> {
            client.createDatabase(hiveDatabase.getInnerDatabase());
            return null;
          });

      LOG.info("Created namespace: {}", namespace);
    } catch (AlreadyExistsException e) {
      throw new NamespaceAlreadyExistsException(
          String.format("Namespace '%s' already exists!", namespace));
    } catch (TException e) {
      throw new RuntimeException(
          "Failed to create namespace " + namespace + " in Hive Metastore", e);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Map<String, String> loadNamespaceMetadata(Namespace namespace)
      throws NoSuchNamespaceException {
    if (!isValidateNamespace(namespace)) {
      throw new NoSuchNamespaceException(String.format("Namespace does not exist: %s", namespace));
    }

    try {
      Database database =
          clientPool.run(client -> client.getDatabase(getDbNameFromNamespace(namespace)));
      HiveDatabase hiveDatabase = new HiveDatabase.Builder().withHiveDatabase(database).build();
      Map<String, String> metadata = hiveDatabase.getMetadata();

      LOG.debug("Loaded metadata for namespace {} found {}", namespace, metadata.keySet());
      return metadata;
    } catch (NoSuchObjectException | UnknownDBException e) {
      throw new NoSuchNamespaceException(
          String.format("Namespace does not exist: %s", namespace), e);
    } catch (TException e) {
      throw new RuntimeException(
          "Failed to list namespace under namespace: " + namespace + " in Hive Metastore", e);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void alterNamespace(Namespace namespace, NamespaceChange... changes)
      throws NoSuchNamespaceException {
    Preconditions.checkArgument(
        !namespace.isEmpty(),
        String.format("Cannot create namespace with invalid name: %s", namespace));
    Preconditions.checkArgument(
        isValidateNamespace(namespace),
        String.format("Cannot support multi part namespace in Hive Metastore: %s", namespace));

    try {
      // load the database parameters
      Database database =
          clientPool.run(client -> client.getDatabase(getDbNameFromNamespace(namespace)));
      HiveDatabase hiveDatabase = new HiveDatabase.Builder().withHiveDatabase(database).build();
      Map<String, String> metadata = hiveDatabase.getMetadata();
      LOG.debug("Loaded metadata for namespace {} found {}", namespace, metadata.keySet());

      for (NamespaceChange change : changes) {
        if (change instanceof NamespaceChange.SetProperty) {
          metadata.put(
              ((NamespaceChange.SetProperty) change).getProperty(),
              ((NamespaceChange.SetProperty) change).getValue());
        } else if (change instanceof NamespaceChange.RemoveProperty) {
          metadata.remove(((NamespaceChange.RemoveProperty) change).getProperty());
        } else {
          throw new IllegalArgumentException("Unsupported namespace change type: " + change);
        }
      }

      // alter the hive database parameters
      Database alteredDatabase = database.deepCopy();
      alteredDatabase.setParameters(metadata);

      clientPool.run(
          client -> {
            client.alterDatabase(getDbNameFromNamespace(namespace), alteredDatabase);
            return null;
          });

      LOG.info("Altered namespace: {}", alteredDatabase);
      // todo(xun): hive dose not support renaming database name directly,
      //  perhaps we can use namespace to mapping the database names indirectly
    } catch (TException | InterruptedException e) {
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
      clientPool.run(
          client -> {
            client.dropDatabase(getDbNameFromNamespace(namespace), false, false, cascade);
            return null;
          });

      LOG.info("Dropped namespace: {}", namespace);
      return true;
    } catch (InvalidOperationException e) {
      throw new NonEmptyNamespaceException(
          String.format("Namespace %s is not empty. One or more tables exist.", namespace, e));
    } catch (NoSuchObjectException | InterruptedException e) {
      return false;
    } catch (TException e) {
      throw new RuntimeException("Failed to drop namespace " + namespace + " in Hive Metastore", e);
    }
  }

  public static boolean isValidateNamespace(Namespace namespace) {
    return namespace.levels().length == 3;
  }

  private String getDbNameFromNamespace(Namespace namespace) {
    return namespace.level(2);
  }

  public static class Builder extends BaseCatalog.BaseCatalogBuilder<Builder, HiveCatalog> {
    HiveConf hiveConf;

    HiveCatalog hiveCatalog = new HiveCatalog();

    @Override
    protected HiveCatalog internalBuild() {
      hiveCatalog.id = id;
      hiveCatalog.metalakeId = metalakeId;
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
