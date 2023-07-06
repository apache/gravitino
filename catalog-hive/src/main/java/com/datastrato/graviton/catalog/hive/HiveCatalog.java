package com.datastrato.graviton.catalog.hive;

import com.datastrato.graviton.*;
import com.datastrato.graviton.catalog.hive.rel.HiveSchema;
import com.datastrato.graviton.exceptions.NoSuchNamespaceException;
import com.datastrato.graviton.exceptions.NoSuchSchemaException;
import com.datastrato.graviton.exceptions.NonEmptySchemaException;
import com.datastrato.graviton.exceptions.SchemaAlreadyExistsException;
import com.datastrato.graviton.meta.AuditInfo;
import com.datastrato.graviton.meta.BaseCatalog;
import com.datastrato.graviton.rel.SchemaChange;
import com.datastrato.graviton.rel.SupportsSchemas;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.time.Instant;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HiveCatalog extends BaseCatalog implements SupportsSchemas {
  
  public static final Logger LOG = LoggerFactory.getLogger(HiveCatalog.class);

  @VisibleForTesting HiveClientPool clientPool;

  private HiveConf hiveConf;

  @Override
  public void close() {
    if (clientPool != null) {
      clientPool.close();
      clientPool = null;
    }
  }

  @Override
  public void initialize(Config config) throws RuntimeException {
    // todo(xun): add hive client pool size in config
    this.clientPool = new HiveClientPool(1, hiveConf);
  }

  @Override
  public NameIdentifier[] listSchemas(Namespace namespace) throws NoSuchNamespaceException {
    if (!isValidNamespace(namespace)) {
      throw new NoSuchNamespaceException("Namespace is invalid " + namespace);
    }

    try {
      NameIdentifier[] schemas =
          clientPool.run(
              c ->
                  c.getAllDatabases().stream()
                      .map(db -> NameIdentifier.of(namespace, db))
                      .toArray(NameIdentifier[]::new));
      return schemas;

    } catch (TException e) {
      throw new RuntimeException(
          "Failed to list all schemas (database) under namespace : "
              + namespace
              + " in Hive Metastore",
          e);

    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public HiveSchema createSchema(NameIdentifier ident, String comment, Map<String, String> metadata)
      throws SchemaAlreadyExistsException {
    Preconditions.checkArgument(
        !ident.name().isEmpty(),
        String.format("Cannot create schema with invalid name: %s", ident.name()));
    Preconditions.checkArgument(
        isValidNamespace(ident.namespace()),
        String.format("Cannot support invalid namespace in Hive Metastore: %s", ident.namespace()));

    try {
      HiveSchema hiveSchema =
          new HiveSchema.Builder()
              .withId(1L /*TODO. Use ID generator*/)
              .withCatalogId(getId())
              .withName(ident.name())
              .withNamespace(ident.namespace())
              .withComment(comment)
              .withProperties(metadata)
              .withAuditInfo(
                  new AuditInfo.Builder()
                      .withCreator(currentUser())
                      .withCreateTime(Instant.now())
                      .build())
              .withConf(hiveConf)
              .build();

      clientPool.run(
          client -> {
            client.createDatabase(hiveSchema.toInnerDB());
            return null;
          });

      // TODO. We should also store the customized HiveSchema entity fields into our own
      //  underlying storage, like id, auditInfo, etc.

      LOG.info("Created Hive schema (database) {} in Hive Metastore", ident.name());

      return hiveSchema;

    } catch (AlreadyExistsException e) {
      throw new SchemaAlreadyExistsException(
          String.format(
              "Hive schema (database) '%s' already exists in Hive Metastore", ident.name()));

    } catch (TException e) {
      throw new RuntimeException(
          "Failed to create Hive schema (database) " + ident.name() + " in Hive Metastore", e);

    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public HiveSchema loadSchema(NameIdentifier ident) throws NoSuchSchemaException {
    Preconditions.checkArgument(
        !ident.name().isEmpty(),
        String.format("Cannot create schema with invalid name: %s", namespace));
    Preconditions.checkArgument(
        isValidNamespace(ident.namespace()),
        String.format("Cannot support invalid namespace in Hive Metastore: %s", namespace));

    try {
      Database database = clientPool.run(client -> client.getDatabase(ident.name()));
      HiveSchema.Builder builder = new HiveSchema.Builder();

      // TODO. We should also fetch the customized HiveSchema entity fields from our own
      //  underlying storage, like id, auditInfo, etc.

      builder =
          builder
              .withId(1L /* TODO. Fetch id from underlying storage */)
              .withCatalogId(getId())
              .withNamespace(ident.namespace())
              .withAuditInfo(
                  /* TODO. Fetch audit info from underlying storage */
                  new AuditInfo.Builder()
                      .withCreator(currentUser())
                      .withCreateTime(Instant.now())
                      .build())
              .withConf(hiveConf);
      HiveSchema hiveSchema = HiveSchema.fromInnerDB(database, builder);

      LOG.info("Loaded Hive schema (database) {} from Hive Metastore ", ident.name());

      return hiveSchema;

    } catch (NoSuchObjectException | UnknownDBException e) {
      throw new NoSuchSchemaException(
          String.format(
              "Hive schema (database) does not exist: %s in Hive Metastore", ident.name()),
          e);

      // TODO. We should also delete Hive schema (database) from our own underlying storage

    } catch (TException e) {
      throw new RuntimeException(
          "Failed to load Hive schema (database) " + ident.name() + " from Hive Metastore", e);

    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public HiveSchema alterSchema(NameIdentifier ident, SchemaChange... changes)
      throws NoSuchSchemaException {
    Preconditions.checkArgument(
        !ident.name().isEmpty(),
        String.format("Cannot create schema with invalid name: %s", namespace));
    Preconditions.checkArgument(
        isValidNamespace(ident.namespace()),
        String.format("Cannot support invalid namespace in Hive Metastore: %s", namespace));

    try {
      // load the database parameters
      Database database = clientPool.run(client -> client.getDatabase(ident.name()));
      Map<String, String> metadata = HiveSchema.convertToMetadata(database);
      LOG.debug(
          "Loaded metadata for Hive schema (database) {} found {}",
          ident.name(),
          metadata.keySet());

      for (SchemaChange change : changes) {
        if (change instanceof SchemaChange.SetProperty) {
          metadata.put(
              ((SchemaChange.SetProperty) change).getProperty(),
              ((SchemaChange.SetProperty) change).getValue());
        } else if (change instanceof SchemaChange.RemoveProperty) {
          metadata.remove(((SchemaChange.RemoveProperty) change).getProperty());
        } else {
          throw new IllegalArgumentException(
              "Unsupported schema change type: " + change.getClass().getSimpleName());
        }
      }

      // alter the hive database parameters
      Database alteredDatabase = database.deepCopy();
      alteredDatabase.setParameters(metadata);

      clientPool.run(
          client -> {
            client.alterDatabase(ident.name(), alteredDatabase);
            return null;
          });

      // TODO. We should also update the customized HiveSchema entity fields into our own if
      //  necessary
      HiveSchema.Builder builder = new HiveSchema.Builder();
      builder =
          builder
              .withId(1L /* TODO. Fetch id from underlying storage */)
              .withCatalogId(getId())
              .withNamespace(ident.namespace())
              .withAuditInfo(
                  /* TODO. Fetch audit info from underlying storage */
                  new AuditInfo.Builder()
                      .withCreator(currentUser())
                      .withCreateTime(Instant.now())
                      .withLastModifier(currentUser())
                      .withLastModifiedTime(Instant.now())
                      .build())
              .withConf(hiveConf);
      HiveSchema hiveSchema = HiveSchema.fromInnerDB(alteredDatabase, builder);

      LOG.info("Altered Hive schema (database) {} in Hive Metastore", ident.name());
      // todo(xun): hive dose not support renaming database name directly,
      //  perhaps we can use namespace to mapping the database names indirectly

      return hiveSchema;

    } catch (TException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean dropSchema(NameIdentifier ident, boolean cascade) throws NonEmptySchemaException {
    if (ident.name().isEmpty()) {
      LOG.error("Cannot drop schema with invalid name: {}", ident.name());
      return false;
    }
    if (!isValidNamespace(ident.namespace())) {
      LOG.error("Cannot support invalid namespace in Hive Metastore: {}", ident.namespace());
      return false;
    }

    try {
      clientPool.run(
          client -> {
            client.dropDatabase(ident.name(), false, false, cascade);
            return null;
          });

      // TODO. we should also delete the Hive schema (database) from our own underlying storage

      LOG.info("Dropped Hive schema (database) {}", ident.name());
      return true;

    } catch (InvalidOperationException e) {
      throw new NonEmptySchemaException(
          String.format(
              "Hive schema (database) %s is not empty. One or more tables exist.",
              ident.name(), e));

    } catch (NoSuchObjectException e) {
      LOG.warn("Hive schema (database) {} does not exist in Hive Metastore", ident.name());
      return false;

    } catch (TException e) {
      throw new RuntimeException(
          "Failed to drop Hive schema (database) " + namespace + " in Hive Metastore", e);

    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  public boolean isValidNamespace(Namespace namespace) {
    return namespace.levels().length == 2 && namespace.level(1).equals(name());
  }

  // TODO. We should figure out a better way to get the current user from servlet container.
  private static String currentUser() {
    String username = null;
    try {
      username = UserGroupInformation.getCurrentUser().getShortUserName();
    } catch (IOException e) {
      LOG.warn("Failed to get Hadoop user", e);
    }

    if (username != null) {
      return username;
    } else {
      LOG.warn("Hadoop user is null, defaulting to user.name");
      return System.getProperty("user.name");
    }
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
