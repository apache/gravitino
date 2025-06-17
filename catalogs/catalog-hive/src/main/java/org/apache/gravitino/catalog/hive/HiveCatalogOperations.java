/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.catalog.hive;

import static org.apache.gravitino.catalog.hive.HiveCatalogPropertiesMetadata.LIST_ALL_TABLES;
import static org.apache.gravitino.catalog.hive.HiveCatalogPropertiesMetadata.METASTORE_URIS;
import static org.apache.gravitino.catalog.hive.HiveCatalogPropertiesMetadata.PRINCIPAL;
import static org.apache.gravitino.catalog.hive.HiveTable.SUPPORT_TABLE_TYPES;
import static org.apache.gravitino.catalog.hive.HiveTablePropertiesMetadata.COMMENT;
import static org.apache.gravitino.catalog.hive.HiveTablePropertiesMetadata.TABLE_TYPE;
import static org.apache.gravitino.connector.BaseCatalog.CATALOG_BYPASS_PREFIX;
import static org.apache.gravitino.hive.converter.HiveDataTypeConverter.CONVERTER;
import static org.apache.hadoop.hive.metastore.TableType.EXTERNAL_TABLE;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
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
import org.apache.gravitino.Catalog;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.SchemaChange;
import org.apache.gravitino.connector.CatalogInfo;
import org.apache.gravitino.connector.CatalogOperations;
import org.apache.gravitino.connector.HasPropertyMetadata;
import org.apache.gravitino.connector.ProxyPlugin;
import org.apache.gravitino.connector.SupportsSchemas;
import org.apache.gravitino.exceptions.ConnectionFailedException;
import org.apache.gravitino.exceptions.NoSuchCatalogException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.NoSuchTableException;
import org.apache.gravitino.exceptions.NonEmptySchemaException;
import org.apache.gravitino.exceptions.SchemaAlreadyExistsException;
import org.apache.gravitino.exceptions.TableAlreadyExistsException;
import org.apache.gravitino.hive.CachedClientPool;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.TableCatalog;
import org.apache.gravitino.rel.TableChange;
import org.apache.gravitino.rel.expressions.NamedReference;
import org.apache.gravitino.rel.expressions.distributions.Distribution;
import org.apache.gravitino.rel.expressions.distributions.Distributions;
import org.apache.gravitino.rel.expressions.sorts.SortOrder;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.expressions.transforms.Transforms;
import org.apache.gravitino.rel.indexes.Index;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Operations for interacting with an Apache Hive catalog in Apache Gravitino. */
public class HiveCatalogOperations implements CatalogOperations, SupportsSchemas, TableCatalog {

  public static final Logger LOG = LoggerFactory.getLogger(HiveCatalogOperations.class);
  public static final String GRAVITINO_KEYTAB_FORMAT = "keytabs/gravitino-hive-%s-keytab";

  @VisibleForTesting CachedClientPool clientPool;

  @VisibleForTesting HiveConf hiveConf;

  private CatalogInfo info;

  private HasPropertyMetadata propertiesMetadata;

  private ScheduledThreadPoolExecutor checkTgtExecutor;
  private String kerberosRealm;
  private ProxyPlugin proxyPlugin;
  private boolean listAllTables = true;
  // The maximum number of tables that can be returned by the listTableNamesByFilter function.
  // The default value is -1, which means that all tables are returned.
  private static final short MAX_TABLES = -1;

  // Map that maintains the mapping of keys in Gravitino to that in Hive, for example, users
  // will only need to set the configuration 'METASTORE_URL' in Gravitino and Gravitino will change
  // it to `METASTOREURIS` automatically and pass it to Hive.
  public static final Map<String, String> GRAVITINO_CONFIG_TO_HIVE =
      ImmutableMap.of(METASTORE_URIS, ConfVars.METASTOREURIS.varname);

  /**
   * Initializes the Hive catalog operations with the provided configuration.
   *
   * @param conf The configuration map for the Hive catalog operations.
   * @param info The catalog info associated with this operations instance.
   * @param propertiesMetadata The properties metadata of Hive catalog.
   * @throws RuntimeException if initialization fails.
   */
  @Override
  public void initialize(
      Map<String, String> conf, CatalogInfo info, HasPropertyMetadata propertiesMetadata)
      throws RuntimeException {
    this.info = info;
    this.propertiesMetadata = propertiesMetadata;

    // Key format like gravitino.bypass.a.b
    Map<String, String> byPassConfig = Maps.newHashMap();
    // Hold keys that lie in GRAVITINO_CONFIG_TO_HIVE
    Map<String, String> gravitinoConfig = Maps.newHashMap();

    conf.forEach(
        (key, value) -> {
          if (key.startsWith(CATALOG_BYPASS_PREFIX)) {
            // Trim bypass prefix and pass it to hive conf
            String hiveKey = key.substring(CATALOG_BYPASS_PREFIX.length());
            if (!hiveKey.isEmpty()) {
              byPassConfig.put(hiveKey, value);
            } else {
              LOG.warn("Ignoring invalid configuration key: {}", key);
            }
          } else if (GRAVITINO_CONFIG_TO_HIVE.containsKey(key)) {
            gravitinoConfig.put(GRAVITINO_CONFIG_TO_HIVE.get(key), value);
          }
        });

    Map<String, String> mergeConfig = Maps.newHashMap(byPassConfig);
    // `gravitinoConfig` overwrite byPassConfig if possible
    mergeConfig.putAll(gravitinoConfig);

    Configuration hadoopConf = new Configuration();
    // Set byPass first to make Gravitino config overwrite it, only keys in byPassConfig
    // and gravitinoConfig will be passed to Hive config, and gravitinoConfig has higher priority
    mergeConfig.forEach(hadoopConf::set);
    hiveConf = new HiveConf(hadoopConf, HiveCatalogOperations.class);

    initKerberosIfNecessary(conf, hadoopConf);

    this.clientPool = new CachedClientPool(hiveConf, conf);

    this.listAllTables = enableListAllTables(conf);
  }

  private void initKerberosIfNecessary(Map<String, String> conf, Configuration hadoopConf) {
    if (UserGroupInformation.AuthenticationMethod.KERBEROS
        == SecurityUtil.getAuthenticationMethod(hadoopConf)) {
      try {
        Path keytabsPath = Paths.get("keytabs");
        if (!Files.exists(keytabsPath)) {
          // Ignore the return value, because there exists many Hive catalog operations making
          // this directory.
          Files.createDirectory(keytabsPath);
        }

        // The id of entity is a random unique id.
        Path keytabPath = Paths.get(String.format(GRAVITINO_KEYTAB_FORMAT, info.id()));
        keytabPath.toFile().deleteOnExit();
        if (Files.exists(keytabPath)) {
          try {
            Files.delete(keytabPath);
          } catch (IOException e) {
            throw new IllegalStateException(
                String.format("Fail to delete keytab file %s", keytabPath.toAbsolutePath()), e);
          }
        }

        String keytabUri =
            (String)
                propertiesMetadata
                    .catalogPropertiesMetadata()
                    .getOrDefault(conf, HiveCatalogPropertiesMetadata.KEY_TAB_URI);
        Preconditions.checkArgument(StringUtils.isNotBlank(keytabUri), "Keytab uri can't be blank");
        // TODO: Support to download the file from Kerberos HDFS
        Preconditions.checkArgument(
            !keytabUri.trim().startsWith("hdfs"), "Keytab uri doesn't support to use HDFS");

        int fetchKeytabFileTimeout =
            (int)
                propertiesMetadata
                    .catalogPropertiesMetadata()
                    .getOrDefault(conf, HiveCatalogPropertiesMetadata.FETCH_TIMEOUT_SEC);

        FetchFileUtils.fetchFileFromUri(
            keytabUri, keytabPath.toFile(), fetchKeytabFileTimeout, hadoopConf);

        hiveConf.setVar(
            ConfVars.METASTORE_KERBEROS_KEYTAB_FILE, keytabPath.toAbsolutePath().toString());

        String catalogPrincipal =
            (String) propertiesMetadata.catalogPropertiesMetadata().getOrDefault(conf, PRINCIPAL);
        Preconditions.checkArgument(
            StringUtils.isNotBlank(catalogPrincipal), "The principal can't be blank");
        @SuppressWarnings("null")
        List<String> principalComponents = Splitter.on('@').splitToList(catalogPrincipal);
        Preconditions.checkArgument(
            principalComponents.size() == 2, "The principal has the wrong format");
        this.kerberosRealm = principalComponents.get(1);

        checkTgtExecutor =
            new ScheduledThreadPoolExecutor(
                1, getThreadFactory(String.format("Kerberos-check-%s", info.id())));

        LOG.info("krb5 path: {}", System.getProperty("java.security.krb5.conf"));
        refreshKerberosConfig();
        UserGroupInformation.setConfiguration(hadoopConf);
        UserGroupInformation.loginUserFromKeytab(
            catalogPrincipal, keytabPath.toAbsolutePath().toString());

        UserGroupInformation kerberosLoginUgi = UserGroupInformation.getCurrentUser();

        int checkInterval =
            (int)
                propertiesMetadata
                    .catalogPropertiesMetadata()
                    .getOrDefault(conf, HiveCatalogPropertiesMetadata.CHECK_INTERVAL_SEC);

        checkTgtExecutor.scheduleAtFixedRate(
            () -> {
              try {
                kerberosLoginUgi.checkTGTAndReloginFromKeytab();
              } catch (Exception e) {
                LOG.error("Fail to refresh ugi token: ", e);
              }
            },
            checkInterval,
            checkInterval,
            TimeUnit.SECONDS);

      } catch (IOException ioe) {
        throw new UncheckedIOException(ioe);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  private void refreshKerberosConfig() {
    Class<?> classRef;
    try {
      if (System.getProperty("java.vendor").contains("IBM")) {
        classRef = Class.forName("com.ibm.security.krb5.internal.Config");
      } else {
        classRef = Class.forName("sun.security.krb5.Config");
      }

      Method refershMethod = classRef.getMethod("refresh");
      refershMethod.invoke(null);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  boolean enableListAllTables(Map<String, String> conf) {
    return (boolean)
        propertiesMetadata.catalogPropertiesMetadata().getOrDefault(conf, LIST_ALL_TABLES);
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

    Path keytabPath = Paths.get(String.format(GRAVITINO_KEYTAB_FORMAT, info.id()));
    if (Files.exists(keytabPath)) {
      try {
        Files.delete(keytabPath);
      } catch (IOException e) {
        LOG.error("Fail to delete key tab file {}", keytabPath.toAbsolutePath(), e);
      }
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
          HiveSchema.builder()
              .withName(ident.name())
              .withComment(comment)
              .withProperties(properties)
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
      HiveSchema hiveSchema = HiveSchema.fromHiveDB(database);

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

      // alter the Hive database parameters
      Database alteredDatabase = database.deepCopy();
      alteredDatabase.setParameters(properties);

      clientPool.run(
          client -> {
            client.alterDatabase(ident.name(), alteredDatabase);
            return null;
          });

      LOG.info("Altered Hive schema (database) {} in Hive Metastore", ident.name());
      return HiveSchema.fromHiveDB(alteredDatabase);

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
   * @return true if the schema was dropped successfully, false if the schema does not exist.
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
      if (!listAllTables) {
        // The reason for using the listTableNamesByFilter function is that the
        // getTableObjectiesByName function has poor performance. Currently, we focus on the
        // Iceberg, Paimon and Hudi table. In the future, if necessary, we will need to filter out
        // other tables. In addition, the current return also includes tables of type VIRTUAL-VIEW.
        String icebergAndPaimonFilter = getIcebergAndPaimonFilter();
        List<String> icebergAndPaimonTables =
            clientPool.run(
                c ->
                    c.listTableNamesByFilter(
                        schemaIdent.name(), icebergAndPaimonFilter, MAX_TABLES));
        allTables.removeAll(icebergAndPaimonTables);

        // filter out the Hudi tables
        String hudiFilter =
            String.format(
                "%sprovider like \"hudi\"", hive_metastoreConstants.HIVE_FILTER_FIELD_PARAMS);
        List<String> hudiTables =
            clientPool.run(
                c -> c.listTableNamesByFilter(schemaIdent.name(), hudiFilter, MAX_TABLES));
        removeHudiTables(allTables, hudiTables);
      }
      return allTables.stream()
          .map(tbName -> NameIdentifier.of(namespace, tbName))
          .toArray(NameIdentifier[]::new);

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

  private static String getIcebergAndPaimonFilter() {
    String icebergFilter =
        String.format(
            "%stable_type like \"ICEBERG\"", hive_metastoreConstants.HIVE_FILTER_FIELD_PARAMS);
    String paimonFilter =
        String.format(
            "%stable_type like \"PAIMON\"", hive_metastoreConstants.HIVE_FILTER_FIELD_PARAMS);
    return String.format("%s or %s", icebergFilter, paimonFilter);
  }

  private void removeHudiTables(List<String> allTables, List<String> hudiTables) {
    for (String hudiTable : hudiTables) {
      allTables.removeIf(
          t ->
              t.equals(hudiTable)
                  || t.startsWith(hudiTable + "_ro")
                  || t.startsWith(hudiTable + "_rt"));
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
              String fieldName = String.join(".", ((TableChange.ColumnChange) c).fieldName());
              Preconditions.checkArgument(
                  c instanceof TableChange.UpdateColumnComment
                      || !partitionFields.contains(fieldName),
                  "Cannot alter partition column: " + fieldName);

              if (c instanceof TableChange.UpdateColumnPosition
                  && afterPartitionColumn(
                      partitionFields, ((TableChange.UpdateColumnPosition) c).getPosition())) {
                throw new IllegalArgumentException(
                    "Cannot alter column position to after partition column");
              }

              if (c instanceof TableChange.DeleteColumn) {
                existingFields.remove(fieldName);
              }

              if (c instanceof TableChange.AddColumn) {
                TableChange.AddColumn addColumn = (TableChange.AddColumn) c;

                if (existingFields.contains(fieldName)) {
                  throw new IllegalArgumentException(
                      "Cannot add column with duplicate name: " + fieldName);
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

    TableType tableType =
        (TableType)
            propertiesMetadata.tablePropertiesMetadata().getOrDefault(properties, TABLE_TYPE);
    Preconditions.checkArgument(
        SUPPORT_TABLE_TYPES.contains(tableType.name()),
        "Unsupported table type: " + tableType.name());

    try {
      if (!schemaExists(schemaIdent)) {
        LOG.warn("Hive schema (database) does not exist: {}", schemaIdent);
        throw new NoSuchSchemaException("Hive Schema (database) does not exist: %s ", schemaIdent);
      }

      HiveTable hiveTable =
          HiveTable.builder()
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
            c.createTable(hiveTable.toHiveTable(propertiesMetadata.tablePropertiesMetadata()));
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
          table.toHiveTable(propertiesMetadata.tablePropertiesMetadata());

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
            CONVERTER.fromGravitino(change.getDataType()).getQualifiedName(),
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
    cols.get(indexOfColumn)
        .setType(CONVERTER.fromGravitino(change.getNewDataType()).getQualifiedName());
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
    try {
      if (isExternalTable(tableIdent)) {
        return dropHiveTable(tableIdent, false, false);
      } else {
        return dropHiveTable(tableIdent, true, false);
      }
    } catch (NoSuchTableException e) {
      return false;
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
    try {
      if (isExternalTable(tableIdent)) {
        throw new UnsupportedOperationException("Can't purge a external hive table");
      } else {
        return dropHiveTable(tableIdent, true, true);
      }
    } catch (NoSuchTableException e) {
      return false;
    }
  }

  /**
   * Performs `getAllDatabases` operation in Hive Metastore to test the connection.
   *
   * @param catalogIdent the name of the catalog.
   * @param type the type of the catalog.
   * @param provider the provider of the catalog.
   * @param comment the comment of the catalog.
   * @param properties the properties of the catalog.
   */
  @Override
  public void testConnection(
      NameIdentifier catalogIdent,
      Catalog.Type type,
      String provider,
      String comment,
      Map<String, String> properties) {
    try {
      clientPool.run(IMetaStoreClient::getAllDatabases);
    } catch (Exception e) {
      throw new ConnectionFailedException(
          e, "Failed to run getAllDatabases in Hive Metastore: %s", e.getMessage());
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
