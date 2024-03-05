/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.hive;

import static com.datastrato.gravitino.catalog.BaseCatalog.CATALOG_BYPASS_PREFIX;
import static com.datastrato.gravitino.catalog.hive.HiveCatalogPropertiesMeta.CLIENT_POOL_CACHE_EVICTION_INTERVAL_MS;
import static com.datastrato.gravitino.catalog.hive.HiveCatalogPropertiesMeta.CLIENT_POOL_SIZE;
import static com.datastrato.gravitino.catalog.hive.HiveCatalogPropertiesMeta.METASTORE_URIS;
import static com.datastrato.gravitino.catalog.hive.HiveCatalogPropertiesMeta.PRINCIPAL;
import static com.datastrato.gravitino.catalog.hive.HiveTable.SUPPORT_TABLE_TYPES;
import static com.datastrato.gravitino.catalog.hive.HiveTablePropertiesMetadata.COMMENT;
import static com.datastrato.gravitino.catalog.hive.HiveTablePropertiesMetadata.TABLE_TYPE;
import static org.apache.hadoop.hive.metastore.TableType.EXTERNAL_TABLE;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.catalog.CatalogOperations;
import com.datastrato.gravitino.catalog.PropertiesMetadata;
import com.datastrato.gravitino.catalog.ProxyPlugin;
import com.datastrato.gravitino.catalog.hive.HiveTablePropertiesMetadata.TableType;
import com.datastrato.gravitino.catalog.hive.converter.ToHiveType;
import com.datastrato.gravitino.exceptions.NoSuchCatalogException;
import com.datastrato.gravitino.exceptions.NoSuchSchemaException;
import com.datastrato.gravitino.exceptions.NoSuchTableException;
import com.datastrato.gravitino.exceptions.NonEmptySchemaException;
import com.datastrato.gravitino.exceptions.SchemaAlreadyExistsException;
import com.datastrato.gravitino.exceptions.TableAlreadyExistsException;
import com.datastrato.gravitino.meta.AuditInfo;
import com.datastrato.gravitino.meta.CatalogEntity;
import com.datastrato.gravitino.rel.Column;
import com.datastrato.gravitino.rel.SchemaChange;
import com.datastrato.gravitino.rel.SupportsSchemas;
import com.datastrato.gravitino.rel.Table;
import com.datastrato.gravitino.rel.TableCatalog;
import com.datastrato.gravitino.rel.TableChange;
import com.datastrato.gravitino.rel.expressions.Expression;
import com.datastrato.gravitino.rel.expressions.NamedReference;
import com.datastrato.gravitino.rel.expressions.distributions.Distribution;
import com.datastrato.gravitino.rel.expressions.distributions.Distributions;
import com.datastrato.gravitino.rel.expressions.sorts.SortOrder;
import com.datastrato.gravitino.rel.expressions.transforms.Transform;
import com.datastrato.gravitino.rel.expressions.transforms.Transforms;
import com.datastrato.gravitino.rel.indexes.Index;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Operations for interacting with the Hive catalog in Gravitino. */
public class HiveCatalogOperations implements CatalogOperations, SupportsSchemas, TableCatalog {

  public static final Logger LOG = LoggerFactory.getLogger(HiveCatalogOperations.class);
  public static final String GRAVITINO_KEYTAB_FORMAT = "keytabs/gravitino-%s-keytab";

  @VisibleForTesting CachedClientPool clientPool;

  @VisibleForTesting HiveConf hiveConf;

  private final CatalogEntity entity;

  private HiveTablePropertiesMetadata tablePropertiesMetadata;

  private HiveCatalogPropertiesMeta catalogPropertiesMetadata;

  private HiveSchemaPropertiesMetadata schemaPropertiesMetadata;

  private ScheduledThreadPoolExecutor checkTgtExecutor;
  private String kerberosRealm;
  private ProxyPlugin proxyPlugin;

  // Map that maintains the mapping of keys in Gravitino to that in Hive, for example, users
  // will only need to set the configuration 'METASTORE_URL' in Gravitino and Gravitino will change
  // it to `METASTOREURIS` automatically and pass it to Hive.
  public static final Map<String, String> GRAVITINO_CONFIG_TO_HIVE =
      ImmutableMap.of(
          METASTORE_URIS,
          ConfVars.METASTOREURIS.varname,
          PRINCIPAL,
          ConfVars.METASTORE_KERBEROS_PRINCIPAL.varname);

  /**
   * Constructs a new instance of HiveCatalogOperations.
   *
   * @param entity The catalog entity associated with this operations instance.
   */
  public HiveCatalogOperations(CatalogEntity entity) {
    this.entity = entity;
  }

  /**
   * Initializes the Hive catalog operations with the provided configuration.
   *
   * @param conf The configuration map for the Hive catalog operations.
   * @throws RuntimeException if initialization fails.
   */
  @Override
  public void initialize(Map<String, String> conf) throws RuntimeException {
    this.tablePropertiesMetadata = new HiveTablePropertiesMetadata();
    this.catalogPropertiesMetadata = new HiveCatalogPropertiesMeta();
    this.schemaPropertiesMetadata = new HiveSchemaPropertiesMetadata();

    // Key format like gravitino.bypass.a.b
    Map<String, String> byPassConfig = Maps.newHashMap();
    // Hold keys that lie in GRAVITINO_CONFIG_TO_HIVE
    Map<String, String> gravitinoConfig = Maps.newHashMap();

    conf.forEach(
        (key, value) -> {
          if (key.startsWith(CATALOG_BYPASS_PREFIX)) {
            // Trim bypass prefix and pass it to hive conf
            byPassConfig.put(key.substring(CATALOG_BYPASS_PREFIX.length()), value);
          } else if (GRAVITINO_CONFIG_TO_HIVE.containsKey(key)) {
            gravitinoConfig.put(GRAVITINO_CONFIG_TO_HIVE.get(key), value);
          }
        });

    Map<String, String> mergeConfig = Maps.newHashMap(byPassConfig);
    // `gravitinoConfig` overwrite byPassConfig if possible
    mergeConfig.putAll(gravitinoConfig);

    Configuration hadoopConf = new Configuration();
    // Set byPass first to make gravitino config overwrite it, only keys in byPassConfig
    // and gravitinoConfig will be passed to Hive config, and gravitinoConfig has higher priority
    mergeConfig.forEach(hadoopConf::set);
    hiveConf = new HiveConf(hadoopConf, HiveCatalogOperations.class);
    UserGroupInformation.setConfiguration(hadoopConf);

    initKerberosIfNecessary(conf, hadoopConf);

    this.clientPool =
        new CachedClientPool(getClientPoolSize(conf), hiveConf, getCacheEvictionInterval(conf));
  }

  private void initKerberosIfNecessary(Map<String, String> conf, Configuration hadoopConf) {
    if (UserGroupInformation.AuthenticationMethod.KERBEROS
        == SecurityUtil.getAuthenticationMethod(hadoopConf)) {
      try {
        File keytabsDir = new File("keytabs");
        if (!keytabsDir.exists()) {
          // Ignore the return value, because there exists many Hive catalog operations making
          // this directory.
          keytabsDir.mkdir();
        }

        // The id of entity is a random unique id.
        File keytabFile = new File(String.format(GRAVITINO_KEYTAB_FORMAT, entity.id()));
        keytabFile.deleteOnExit();
        if (keytabFile.exists() && !keytabFile.delete()) {
          throw new IllegalStateException(
              String.format("Fail to delete keytab file %s", keytabFile.getAbsolutePath()));
        }

        String keytabUri =
            (String)
                catalogPropertiesMetadata.getOrDefault(conf, HiveCatalogPropertiesMeta.KET_TAB_URI);
        Preconditions.checkArgument(StringUtils.isNotBlank(keytabUri), "Keytab uri can't be blank");
        // TODO: Support to download the file from Kerberos HDFS
        Preconditions.checkArgument(
            !keytabUri.trim().startsWith("hdfs"), "Keytab uri doesn't support to use HDFS");

        int fetchKeytabFileTimeout =
            (int)
                catalogPropertiesMetadata.getOrDefault(
                    conf, HiveCatalogPropertiesMeta.FETCH_TIMEOUT_SEC);

        FetchFileUtils.fetchFileFromUri(keytabUri, keytabFile, fetchKeytabFileTimeout, hadoopConf);

        hiveConf.setVar(ConfVars.METASTORE_KERBEROS_KEYTAB_FILE, keytabFile.getAbsolutePath());

        String catalogPrincipal = (String) catalogPropertiesMetadata.getOrDefault(conf, PRINCIPAL);
        Preconditions.checkArgument(
            StringUtils.isNotBlank(catalogPrincipal), "The principal can't be blank");
        @SuppressWarnings("null")
        List<String> principalComponents = Splitter.on('@').splitToList(catalogPrincipal);
        Preconditions.checkArgument(
            principalComponents.size() == 2, "The principal has the wrong format");
        this.kerberosRealm = principalComponents.get(1);

        checkTgtExecutor =
            new ScheduledThreadPoolExecutor(
                1, getThreadFactory(String.format("Kerberos-check-%s", entity.id())));

        UserGroupInformation.loginUserFromKeytab(catalogPrincipal, keytabFile.getAbsolutePath());

        UserGroupInformation kerberosLoginUgi = UserGroupInformation.getCurrentUser();

        int checkInterval =
            (int)
                catalogPropertiesMetadata.getOrDefault(
                    conf, HiveCatalogPropertiesMeta.CHECK_INTERVAL_SEC);

        checkTgtExecutor.scheduleAtFixedRate(
            () -> {
              try {
                kerberosLoginUgi.checkTGTAndReloginFromKeytab();
              } catch (Throwable throwable) {
                LOG.error("Fail to refresh ugi token: ", throwable);
              }
            },
            checkInterval,
            checkInterval,
            TimeUnit.SECONDS);

      } catch (IOException ioe) {
        throw new UncheckedIOException(ioe);
      }
    }
  }

  @VisibleForTesting
  int getClientPoolSize(Map<String, String> conf) {
    return (int) catalogPropertiesMetadata.getOrDefault(conf, CLIENT_POOL_SIZE);
  }

  long getCacheEvictionInterval(Map<String, String> conf) {
    return (long)
        catalogPropertiesMetadata.getOrDefault(conf, CLIENT_POOL_CACHE_EVICTION_INTERVAL_MS);
  }

  /** Closes the Hive catalog and releases the associated client pool. */
  @Override
  public void close() {
    if (clientPool != null) {
      clientPool.close();
      clientPool = null;
    }

    if (checkTgtExecutor != null) {
      checkTgtExecutor.shutdown();
      checkTgtExecutor = null;
    }

    File keytabFile = new File(String.format(GRAVITINO_KEYTAB_FORMAT, entity.id()));
    if (keytabFile.exists() && !keytabFile.delete()) {
      LOG.error("Fail to delete key tab file {}", keytabFile.getAbsolutePath());
    }
  }

  /**
   * Lists the schemas under the given namespace.
   *
   * @param namespace The namespace to list the schemas for.
   * @return An array of {@link NameIdentifier} representing the schemas.
   * @throws NoSuchCatalogException If the provided namespace is invalid or does not exist.
   */
  @Override
  public NameIdentifier[] listSchemas(Namespace namespace) throws NoSuchCatalogException {
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

  /**
   * Creates a new schema with the provided identifier, comment, and metadata.
   *
   * @param ident The identifier of the schema to create.
   * @param comment The comment for the schema.
   * @param properties The metadata properties for the schema.
   * @return The created {@link HiveSchema}.
   * @throws NoSuchCatalogException If the provided namespace is invalid or does not exist.
   * @throws SchemaAlreadyExistsException If a schema with the same name already exists.
   */
  @Override
  public HiveSchema createSchema(
      NameIdentifier ident, String comment, Map<String, String> properties)
      throws NoSuchCatalogException, SchemaAlreadyExistsException {
    try {
      HiveSchema hiveSchema =
          new HiveSchema.Builder()
              .withName(ident.name())
              .withComment(comment)
              .withProperties(properties)
              .withConf(hiveConf)
              .withAuditInfo(
                  AuditInfo.builder()
                      .withCreator(UserGroupInformation.getCurrentUser().getUserName())
                      .withCreateTime(Instant.now())
                      .build())
              .build();

      clientPool.run(
          client -> {
            client.createDatabase(hiveSchema.toHiveDB());
            return null;
          });

      LOG.info("Created Hive schema (database) {} in Hive Metastore", ident.name());
      return hiveSchema;

    } catch (AlreadyExistsException e) {
      throw new SchemaAlreadyExistsException(
          e, "Hive schema (database) '%s' already exists in Hive Metastore", ident.name());

    } catch (TException e) {
      throw new RuntimeException(
          "Failed to create Hive schema (database) " + ident.name() + " in Hive Metastore", e);

    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Loads the schema with the provided identifier.
   *
   * @param ident The identifier of the schema to load.
   * @return The loaded {@link HiveSchema}.
   * @throws NoSuchSchemaException If the schema with the provided identifier does not exist.
   */
  @Override
  public HiveSchema loadSchema(NameIdentifier ident) throws NoSuchSchemaException {
    try {
      Database database = clientPool.run(client -> client.getDatabase(ident.name()));
      HiveSchema hiveSchema = HiveSchema.fromHiveDB(database, hiveConf);

      LOG.info("Loaded Hive schema (database) {} from Hive Metastore ", ident.name());
      return hiveSchema;

    } catch (NoSuchObjectException | UnknownDBException e) {
      throw new NoSuchSchemaException(
          e, "Hive schema (database) does not exist: %s in Hive Metastore", ident.name());

    } catch (TException e) {
      throw new RuntimeException(
          "Failed to load Hive schema (database) " + ident.name() + " from Hive Metastore", e);

    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Alters the schema with the provided identifier according to the specified changes.
   *
   * @param ident The identifier of the schema to alter.
   * @param changes The changes to apply to the schema.
   * @return The altered {@link HiveSchema}.
   * @throws NoSuchSchemaException If the schema with the provided identifier does not exist.
   */
  @Override
  public HiveSchema alterSchema(NameIdentifier ident, SchemaChange... changes)
      throws NoSuchSchemaException {
    try {
      // load the database parameters
      Database database = clientPool.run(client -> client.getDatabase(ident.name()));
      Map<String, String> properties = HiveSchema.buildSchemaProperties(database);
      if (LOG.isDebugEnabled()) {
        LOG.debug(
            "Loaded properties for Hive schema (database) {} found {}",
            ident.name(),
            properties.keySet());
      }

      for (SchemaChange change : changes) {
        if (change instanceof SchemaChange.SetProperty) {
          properties.put(
              ((SchemaChange.SetProperty) change).getProperty(),
              ((SchemaChange.SetProperty) change).getValue());
        } else if (change instanceof SchemaChange.RemoveProperty) {
          properties.remove(((SchemaChange.RemoveProperty) change).getProperty());
        } else {
          throw new IllegalArgumentException(
              "Unsupported schema change type: " + change.getClass().getSimpleName());
        }
      }

      // alter the hive database parameters
      Database alteredDatabase = database.deepCopy();
      alteredDatabase.setParameters(properties);

      clientPool.run(
          client -> {
            client.alterDatabase(ident.name(), alteredDatabase);
            return null;
          });

      LOG.info("Altered Hive schema (database) {} in Hive Metastore", ident.name());
      return HiveSchema.fromHiveDB(alteredDatabase, hiveConf);

    } catch (NoSuchObjectException e) {
      throw new NoSuchSchemaException(
          e, "Hive schema (database) %s does not exist in Hive Metastore", ident.name());

    } catch (TException | InterruptedException e) {
      throw new RuntimeException(
          "Failed to alter Hive schema (database) " + ident.name() + " in Hive metastore", e);

    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Drops the schema with the provided identifier.
   *
   * @param ident The identifier of the schema to drop.
   * @param cascade If set to true, drops all the tables in the schema as well.
   * @return true if the schema was dropped successfully, false otherwise.
   * @throws NonEmptySchemaException If the schema is not empty and 'cascade' is set to false.
   */
  @Override
  public boolean dropSchema(NameIdentifier ident, boolean cascade) throws NonEmptySchemaException {
    try {
      clientPool.run(
          client -> {
            client.dropDatabase(ident.name(), false, false, cascade);
            return null;
          });
      LOG.info("Dropped Hive schema (database) {}", ident.name());
      return true;

    } catch (InvalidOperationException e) {
      throw new NonEmptySchemaException(
          e, "Hive schema (database) %s is not empty. One or more tables exist.", ident.name());

    } catch (NoSuchObjectException e) {
      LOG.warn("Hive schema (database) {} does not exist in Hive Metastore", ident.name());
      return false;

    } catch (TException e) {
      throw new RuntimeException(
          "Failed to drop Hive schema (database) " + ident.name() + " in Hive Metastore", e);

    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Lists all the tables under the specified namespace.
   *
   * @param namespace The namespace to list tables for.
   * @return An array of {@link NameIdentifier} representing the tables in the namespace.
   * @throws NoSuchSchemaException If the schema with the provided namespace does not exist.
   */
  @Override
  public NameIdentifier[] listTables(Namespace namespace) throws NoSuchSchemaException {
    NameIdentifier schemaIdent = NameIdentifier.of(namespace.levels());
    if (!schemaExists(schemaIdent)) {
      throw new NoSuchSchemaException("Schema (database) does not exist %s", namespace);
    }

    try {
      // When a table is created using the HMS interface without specifying the `tableType`,
      // although Hive treats it as a `MANAGED_TABLE`, it cannot be queried through the `getTable`
      // interface in HMS with the specified `tableType`. This is because when creating a table
      // without  specifying the `tableType`, the underlying engine of HMS does not store the
      // information of `tableType`. However, once the `getTable` interface specifies a
      // `tableType`, HMS will use it as a filter condition to query its underlying storage and
      // these types of tables will be  filtered out.
      // Therefore, in order to avoid missing these types of tables, we need to query HMS twice. The
      // first time is to retrieve all types of table names (including the missing type tables), and
      // then based on
      // those names we can obtain metadata for each individual table and get the type we needed.
      List<String> allTables = clientPool.run(c -> c.getAllTables(schemaIdent.name()));
      return clientPool.run(
          c ->
              c.getTableObjectsByName(schemaIdent.name(), allTables).stream()
                  .filter(tb -> SUPPORT_TABLE_TYPES.contains(tb.getTableType()))
                  .map(tb -> NameIdentifier.of(namespace, tb.getTableName()))
                  .toArray(NameIdentifier[]::new));
    } catch (UnknownDBException e) {
      throw new NoSuchSchemaException(
          "Schema (database) does not exist %s in Hive Metastore", namespace);

    } catch (TException e) {
      throw new RuntimeException(
          "Failed to list all tables under the namespace : " + namespace + " in Hive Metastore", e);

    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Loads a table from the Hive Metastore.
   *
   * @param tableIdent The identifier of the table to load.
   * @return The loaded HiveTable instance representing the table.
   * @throws NoSuchTableException If the specified table does not exist in the Hive Metastore.
   */
  @Override
  public Table loadTable(NameIdentifier tableIdent) throws NoSuchTableException {
    org.apache.hadoop.hive.metastore.api.Table table = loadHiveTable(tableIdent);
    HiveTable hiveTable =
        HiveTable.fromHiveTable(table)
            .withProxyPlugin(proxyPlugin)
            .withClientPool(clientPool)
            .build();

    LOG.info("Loaded Hive table {} from Hive Metastore ", tableIdent.name());
    return hiveTable;
  }

  private org.apache.hadoop.hive.metastore.api.Table loadHiveTable(NameIdentifier tableIdent) {
    NameIdentifier schemaIdent = NameIdentifier.of(tableIdent.namespace().levels());

    try {
      org.apache.hadoop.hive.metastore.api.Table table =
          clientPool.run(c -> c.getTable(schemaIdent.name(), tableIdent.name()));
      return table;

    } catch (NoSuchObjectException e) {
      throw new NoSuchTableException(
          e, "Hive table does not exist: %s in Hive Metastore", tableIdent.name());

    } catch (InterruptedException | TException e) {
      throw new RuntimeException(
          "Failed to load Hive table " + tableIdent.name() + " from Hive metastore", e);
    }
  }

  private void validatePartitionForCreate(Column[] columns, Transform[] partitioning) {
    int partitionStartIndex = columns.length - partitioning.length;

    for (int i = 0; i < partitioning.length; i++) {
      Preconditions.checkArgument(
          partitioning[i] instanceof Transforms.IdentityTransform,
          "Hive partition only supports identity transform");

      Transforms.IdentityTransform identity = (Transforms.IdentityTransform) partitioning[i];
      Preconditions.checkArgument(
          identity.fieldName().length == 1, "Hive partition does not support nested field");

      // The partition field must be placed at the end of the columns in order.
      // For example, if the table has columns [a, b, c, d], then the partition field must be
      // [b, c, d] or [c, d] or [d].
      Preconditions.checkArgument(
          columns[partitionStartIndex + i].name().equals(identity.fieldName()[0]),
          "The partition field must be placed at the end of the columns in order");
    }
  }

  private void validateColumnChangeForAlter(
      TableChange[] changes, org.apache.hadoop.hive.metastore.api.Table hiveTable) {
    Set<String> partitionFields =
        hiveTable.getPartitionKeys().stream().map(FieldSchema::getName).collect(Collectors.toSet());
    Set<String> existingFields =
        hiveTable.getSd().getCols().stream().map(FieldSchema::getName).collect(Collectors.toSet());
    existingFields.addAll(partitionFields);

    Arrays.stream(changes)
        .filter(c -> c instanceof TableChange.ColumnChange)
        .forEach(
            c -> {
              String fieldToAdd = String.join(".", ((TableChange.ColumnChange) c).fieldName());
              Preconditions.checkArgument(
                  c instanceof TableChange.UpdateColumnComment
                      || !partitionFields.contains(fieldToAdd),
                  "Cannot alter partition column: " + fieldToAdd);

              if (c instanceof TableChange.UpdateColumnNullability) {
                throw new IllegalArgumentException(
                    "Hive does not support altering column nullability");
              }

              if (c instanceof TableChange.UpdateColumnPosition
                  && afterPartitionColumn(
                      partitionFields, ((TableChange.UpdateColumnPosition) c).getPosition())) {
                throw new IllegalArgumentException(
                    "Cannot alter column position to after partition column");
              }

              if (c instanceof TableChange.AddColumn) {
                TableChange.AddColumn addColumn = (TableChange.AddColumn) c;

                if (existingFields.contains(fieldToAdd)) {
                  throw new IllegalArgumentException(
                      "Cannot add column with duplicate name: " + fieldToAdd);
                }

                if (addColumn.getPosition() == null) {
                  // If the position is not specified, the column will be added to the end of the
                  // non-partition columns.
                  return;
                }

                if ((afterPartitionColumn(partitionFields, addColumn.getPosition()))) {
                  throw new IllegalArgumentException("Cannot add column after partition column");
                }
              }
            });
  }

  private boolean afterPartitionColumn(
      Set<String> partitionFields, TableChange.ColumnPosition columnPosition) {
    Preconditions.checkArgument(columnPosition != null, "Column position cannot be null");

    if (columnPosition instanceof TableChange.After) {
      return partitionFields.contains(((TableChange.After) columnPosition).getColumn());
    }
    return false;
  }

  private void validateDistributionAndSort(Distribution distribution, SortOrder[] sortOrder) {
    if (distribution != Distributions.NONE) {
      boolean allNameReference =
          Arrays.stream(distribution.expressions())
              .allMatch(t -> t instanceof NamedReference.FieldReference);
      Preconditions.checkArgument(
          allNameReference, "Hive distribution only supports field reference");
    }

    if (ArrayUtils.isNotEmpty(sortOrder)) {
      boolean allNameReference =
          Arrays.stream(sortOrder)
              .allMatch(t -> t.expression() instanceof NamedReference.FieldReference);
      Preconditions.checkArgument(allNameReference, "Hive sort order only supports name reference");
    }
  }

  /**
   * Creates a new table in the Hive Metastore.
   *
   * @param tableIdent The identifier of the table to create.
   * @param columns The array of columns for the new table.
   * @param comment The comment for the new table.
   * @param properties The properties for the new table.
   * @param partitioning The partitioning for the new table.
   * @return The newly created HiveTable instance.
   * @throws NoSuchSchemaException If the schema for the table does not exist.
   * @throws TableAlreadyExistsException If the table with the same name already exists.
   */
  @Override
  public Table createTable(
      NameIdentifier tableIdent,
      Column[] columns,
      String comment,
      Map<String, String> properties,
      Transform[] partitioning,
      Distribution distribution,
      SortOrder[] sortOrders,
      Index[] indexes)
      throws NoSuchSchemaException, TableAlreadyExistsException {
    Preconditions.checkArgument(
        indexes.length == 0,
        "Hive-catalog does not support indexes, since indexing was removed since 3.0");
    NameIdentifier schemaIdent = NameIdentifier.of(tableIdent.namespace().levels());

    validatePartitionForCreate(columns, partitioning);
    validateDistributionAndSort(distribution, sortOrders);

    Arrays.stream(columns)
        .forEach(
            c -> {
              validateNullable(c.name(), c.nullable());
              validateColumnDefaultValue(c.name(), c.defaultValue());
            });

    TableType tableType = (TableType) tablePropertiesMetadata.getOrDefault(properties, TABLE_TYPE);
    Preconditions.checkArgument(
        SUPPORT_TABLE_TYPES.contains(tableType.name()),
        "Unsupported table type: " + tableType.name());

    try {
      if (!schemaExists(schemaIdent)) {
        LOG.warn("Hive schema (database) does not exist: {}", schemaIdent);
        throw new NoSuchSchemaException("Hive Schema (database) does not exist: %s ", schemaIdent);
      }

      HiveTable hiveTable =
          new HiveTable.Builder()
              .withName(tableIdent.name())
              .withSchemaName(schemaIdent.name())
              .withClientPool(clientPool)
              .withComment(comment)
              .withColumns(columns)
              .withProperties(properties)
              .withDistribution(distribution)
              .withSortOrders(sortOrders)
              .withProxyPlugin(proxyPlugin)
              .withAuditInfo(
                  AuditInfo.builder()
                      .withCreator(UserGroupInformation.getCurrentUser().getUserName())
                      .withCreateTime(Instant.now())
                      .build())
              .withPartitioning(partitioning)
              .build();
      clientPool.run(
          c -> {
            c.createTable(hiveTable.toHiveTable(tablePropertiesMetadata));
            return null;
          });

      LOG.info("Created Hive table {} in Hive Metastore", tableIdent.name());
      return hiveTable;

    } catch (AlreadyExistsException e) {
      throw new TableAlreadyExistsException(e, "Table already exists: %s", tableIdent.name());
    } catch (TException | InterruptedException e) {
      throw new RuntimeException(
          "Failed to create Hive table " + tableIdent.name() + " in Hive Metastore", e);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Apply the {@link TableChange change} to an existing Hive table.
   *
   * <p>Note: When changing column position, since HMS will check the compatibility of column type
   * between the old column position and the new column position, you need to make sure that the new
   * column position is compatible with the old column position, otherwise the operation will fail
   * in HMS.
   *
   * @param tableIdent The identifier of the table to alter.
   * @param changes The changes to apply to the table.
   * @return This method always throws UnsupportedOperationException.
   * @throws NoSuchTableException This exception will not be thrown in this method.
   * @throws IllegalArgumentException This exception will not be thrown in this method.
   */
  @Override
  public Table alterTable(NameIdentifier tableIdent, TableChange... changes)
      throws NoSuchTableException, IllegalArgumentException {
    NameIdentifier schemaIdent = NameIdentifier.of(tableIdent.namespace().levels());

    try {
      // TODO(@Minghuang): require a table lock to avoid race condition
      HiveTable table = (HiveTable) loadTable(tableIdent);
      org.apache.hadoop.hive.metastore.api.Table alteredHiveTable =
          table.toHiveTable(tablePropertiesMetadata);

      validateColumnChangeForAlter(changes, alteredHiveTable);

      for (TableChange change : changes) {
        // Table change
        if (change instanceof TableChange.RenameTable) {
          doRenameTable(alteredHiveTable, (TableChange.RenameTable) change);

        } else if (change instanceof TableChange.UpdateComment) {
          doUpdateComment(alteredHiveTable, (TableChange.UpdateComment) change);

        } else if (change instanceof TableChange.SetProperty) {
          doSetProperty(alteredHiveTable, (TableChange.SetProperty) change);

        } else if (change instanceof TableChange.RemoveProperty) {
          doRemoveProperty(alteredHiveTable, (TableChange.RemoveProperty) change);

        } else if (change instanceof TableChange.ColumnChange) {
          // Column change
          StorageDescriptor sd = alteredHiveTable.getSd();
          List<FieldSchema> cols = sd.getCols();

          if (change instanceof TableChange.AddColumn) {
            TableChange.AddColumn addColumn = (TableChange.AddColumn) change;
            validateNullable(String.join(".", addColumn.fieldName()), addColumn.isNullable());
            doAddColumn(cols, addColumn);

          } else if (change instanceof TableChange.DeleteColumn) {
            doDeleteColumn(cols, (TableChange.DeleteColumn) change);

          } else if (change instanceof TableChange.RenameColumn) {
            doRenameColumn(cols, (TableChange.RenameColumn) change);

          } else if (change instanceof TableChange.UpdateColumnComment) {
            doUpdateColumnComment(cols, (TableChange.UpdateColumnComment) change);

          } else if (change instanceof TableChange.UpdateColumnPosition) {
            doUpdateColumnPosition(cols, (TableChange.UpdateColumnPosition) change);

          } else if (change instanceof TableChange.UpdateColumnType) {
            doUpdateColumnType(cols, (TableChange.UpdateColumnType) change);

          } else if (change instanceof TableChange.UpdateColumnAutoIncrement) {
            throw new IllegalArgumentException(
                "Hive does not support altering column auto increment");
          } else {
            throw new IllegalArgumentException(
                "Unsupported column change type: " + change.getClass().getSimpleName());
          }
        } else {
          throw new IllegalArgumentException(
              "Unsupported table change type: "
                  + (change == null ? "null" : change.getClass().getSimpleName()));
        }
      }

      clientPool.run(
          c -> {
            c.alter_table(schemaIdent.name(), tableIdent.name(), alteredHiveTable);
            return null;
          });

      LOG.info("Altered Hive table {} in Hive Metastore", tableIdent.name());
      return HiveTable.fromHiveTable(alteredHiveTable)
          .withProxyPlugin(proxyPlugin)
          .withClientPool(clientPool)
          .build();

    } catch (TException | InterruptedException e) {
      if (e.getMessage() != null
          && e.getMessage().contains("types incompatible with the existing columns")) {
        throw new IllegalArgumentException(
            "Failed to alter Hive table ["
                + tableIdent.name()
                + "] in Hive metastore, "
                + "since Hive metastore will check the compatibility of column type between the old and new column positions, "
                + "please ensure that the type of the new column position is compatible with the old one, "
                + "otherwise the alter operation will fail in Hive metastore.",
            e);
      }
      throw new RuntimeException(
          "Failed to alter Hive table " + tableIdent.name() + " in Hive metastore", e);
    } catch (IllegalArgumentException | NoSuchTableException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private void validateColumnDefaultValue(String fieldName, Expression defaultValue) {
    // The DEFAULT constraint for column is supported since Hive3.0, see
    // https://issues.apache.org/jira/browse/HIVE-18726
    if (!defaultValue.equals(Column.DEFAULT_VALUE_NOT_SET)) {
      throw new IllegalArgumentException(
          "The DEFAULT constraint for column is only supported since Hive 3.0, "
              + "but the current Gravitino Hive catalog only supports Hive 2.x. Illegal column: "
              + fieldName);
    }
  }

  private void validateNullable(String fieldName, boolean nullable) {
    // The NOT NULL constraint for column is supported since Hive3.0, see
    // https://issues.apache.org/jira/browse/HIVE-16575
    if (!nullable) {
      throw new IllegalArgumentException(
          "The NOT NULL constraint for column is only supported since Hive 3.0, "
              + "but the current Gravitino Hive catalog only supports Hive 2.x. Illegal column: "
              + fieldName);
    }
  }

  private int columnPosition(List<FieldSchema> columns, TableChange.ColumnPosition position) {
    Preconditions.checkArgument(position != null, "Column position cannot be null");
    if (position instanceof TableChange.After) {
      String afterColumn = ((TableChange.After) position).getColumn();
      int indexOfColumn = indexOfColumn(columns, afterColumn);
      Preconditions.checkArgument(indexOfColumn != -1, "Column does not exist: " + afterColumn);
      return indexOfColumn + 1;
    } else if (position instanceof TableChange.First) {
      return 0;
    } else {
      throw new UnsupportedOperationException(
          "Unsupported column position type: " + position.getClass().getSimpleName());
    }
  }

  /**
   * Returns the index of a column in the given list of FieldSchema objects, based on the field
   * name.
   *
   * @param columns The list of Hive columns.
   * @param fieldName The name of the field to be searched.
   * @return The index of the column if found, otherwise -1.
   */
  private int indexOfColumn(List<FieldSchema> columns, String fieldName) {
    return columns.stream()
        .map(FieldSchema::getName)
        .collect(Collectors.toList())
        .indexOf(fieldName);
  }

  private void doRenameTable(
      org.apache.hadoop.hive.metastore.api.Table hiveTable, TableChange.RenameTable change) {
    hiveTable.setTableName(change.getNewName());
  }

  private void doUpdateComment(
      org.apache.hadoop.hive.metastore.api.Table hiveTable, TableChange.UpdateComment change) {
    Map<String, String> parameters = hiveTable.getParameters();
    parameters.put(COMMENT, change.getNewComment());
  }

  private void doSetProperty(
      org.apache.hadoop.hive.metastore.api.Table hiveTable, TableChange.SetProperty change) {
    Map<String, String> parameters = hiveTable.getParameters();
    parameters.put(change.getProperty(), change.getValue());
  }

  private void doRemoveProperty(
      org.apache.hadoop.hive.metastore.api.Table hiveTable, TableChange.RemoveProperty change) {
    Map<String, String> parameters = hiveTable.getParameters();
    parameters.remove(change.getProperty());
  }

  private void doAddColumn(List<FieldSchema> cols, TableChange.AddColumn change) {
    int targetPosition;
    if (change.isAutoIncrement()) {
      throw new IllegalArgumentException("Hive catalog does not support auto-increment column");
    }
    if (change.getPosition() instanceof TableChange.Default) {
      // add to the end by default
      targetPosition = cols.size();
      LOG.info(
          "Hive catalog add column {} to the end of non-partition columns by default",
          change.fieldName()[0]);
    } else {
      targetPosition = columnPosition(cols, change.getPosition());
    }
    cols.add(
        targetPosition,
        new FieldSchema(
            change.fieldName()[0],
            ToHiveType.convert(change.getDataType()).getQualifiedName(),
            change.getComment()));
  }

  private void doDeleteColumn(List<FieldSchema> cols, TableChange.DeleteColumn change) {
    String columnName = change.fieldName()[0];
    if (!cols.removeIf(c -> c.getName().equals(columnName)) && !change.getIfExists()) {
      throw new IllegalArgumentException("DeleteColumn does not exist: " + columnName);
    }
  }

  private void doRenameColumn(List<FieldSchema> cols, TableChange.RenameColumn change) {
    String columnName = change.fieldName()[0];
    if (indexOfColumn(cols, columnName) == -1) {
      throw new IllegalArgumentException("RenameColumn does not exist: " + columnName);
    }

    String newName = change.getNewName();
    if (indexOfColumn(cols, newName) != -1) {
      throw new IllegalArgumentException("Column already exists: " + newName);
    }
    cols.get(indexOfColumn(cols, columnName)).setName(newName);
  }

  private void doUpdateColumnComment(
      List<FieldSchema> cols, TableChange.UpdateColumnComment change) {
    cols.get(indexOfColumn(cols, change.fieldName()[0])).setComment(change.getNewComment());
  }

  private void doUpdateColumnPosition(
      List<FieldSchema> cols, TableChange.UpdateColumnPosition change) {
    String columnName = change.fieldName()[0];
    int sourceIndex = indexOfColumn(cols, columnName);
    if (sourceIndex == -1) {
      throw new IllegalArgumentException("UpdateColumnPosition does not exist: " + columnName);
    }

    // update column position: remove then add to given position
    FieldSchema hiveColumn = cols.remove(sourceIndex);
    cols.add(columnPosition(cols, change.getPosition()), hiveColumn);
  }

  private void doUpdateColumnType(List<FieldSchema> cols, TableChange.UpdateColumnType change) {
    String columnName = change.fieldName()[0];
    int indexOfColumn = indexOfColumn(cols, columnName);
    if (indexOfColumn == -1) {
      throw new IllegalArgumentException("UpdateColumnType does not exist: " + columnName);
    }
    cols.get(indexOfColumn).setType(ToHiveType.convert(change.getNewDataType()).getQualifiedName());
  }

  /**
   * Drops a table from the Hive Metastore. Deletes the table and removes the directory associated
   * with the table from the file system if the table is not EXTERNAL table. In case of an external
   * table, only the associated metadata information is removed.
   *
   * @param tableIdent The identifier of the table to drop.
   * @return true if the table is successfully dropped; false if the table does not exist.
   */
  @Override
  public boolean dropTable(NameIdentifier tableIdent) {
    if (isExternalTable(tableIdent)) {
      return dropHiveTable(tableIdent, false, false);
    } else {
      return dropHiveTable(tableIdent, true, false);
    }
  }

  /**
   * Purges a table from the Hive Metastore. Completely purge the table skipping trash for managed
   * table, external table aren't supported to purge table.
   *
   * @param tableIdent The identifier of the table to purge.
   * @return true if the table is successfully purged; false if the table does not exist.
   * @throws UnsupportedOperationException If the table type is EXTERNAL_TABLE, it cannot be purged.
   */
  @Override
  public boolean purgeTable(NameIdentifier tableIdent) throws UnsupportedOperationException {
    if (isExternalTable(tableIdent)) {
      throw new UnsupportedOperationException("Can't purge a external hive table");
    } else {
      return dropHiveTable(tableIdent, true, true);
    }
  }

  /**
   * Checks if the given namespace is a valid namespace for the Hive schema.
   *
   * @param tableIdent The namespace to validate.
   * @param deleteData Whether to delete the table data.
   * @param ifPurge Whether to purge the table.
   * @return true if the namespace is valid; otherwise, false.
   */
  private boolean dropHiveTable(NameIdentifier tableIdent, boolean deleteData, boolean ifPurge) {
    NameIdentifier schemaIdent = NameIdentifier.of(tableIdent.namespace().levels());

    try {
      clientPool.run(
          c -> {
            c.dropTable(schemaIdent.name(), tableIdent.name(), deleteData, false, ifPurge);
            return null;
          });

      LOG.info("Dropped Hive table {}", tableIdent.name());
      return true;

    } catch (NoSuchObjectException e) {
      LOG.warn("Hive table {} does not exist in Hive Metastore", tableIdent.name());
      return false;
    } catch (TException | InterruptedException e) {
      throw new RuntimeException(
          "Failed to drop Hive table " + tableIdent.name() + " in Hive Metastore", e);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public PropertiesMetadata tablePropertiesMetadata() throws UnsupportedOperationException {
    return tablePropertiesMetadata;
  }

  @Override
  public PropertiesMetadata catalogPropertiesMetadata() throws UnsupportedOperationException {
    return catalogPropertiesMetadata;
  }

  @Override
  public PropertiesMetadata schemaPropertiesMetadata() throws UnsupportedOperationException {
    return schemaPropertiesMetadata;
  }

  @Override
  public PropertiesMetadata filesetPropertiesMetadata() throws UnsupportedOperationException {
    throw new UnsupportedOperationException(
        "Hive catalog does not support fileset properties metadata");
  }

  CachedClientPool getClientPool() {
    return clientPool;
  }

  HiveConf getHiveConf() {
    return hiveConf;
  }

  private boolean isExternalTable(NameIdentifier tableIdent) {
    org.apache.hadoop.hive.metastore.api.Table hiveTable = loadHiveTable(tableIdent);
    return EXTERNAL_TABLE.name().equalsIgnoreCase(hiveTable.getTableType());
  }

  private static ThreadFactory getThreadFactory(String factoryName) {
    return new ThreadFactoryBuilder().setDaemon(true).setNameFormat(factoryName + "-%d").build();
  }

  public String getKerberosRealm() {
    return kerberosRealm;
  }

  void setProxyPlugin(HiveProxyPlugin hiveProxyPlugin) {
    this.proxyPlugin = hiveProxyPlugin;
  }
}
