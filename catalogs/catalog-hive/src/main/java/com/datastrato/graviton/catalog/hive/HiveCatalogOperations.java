/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.catalog.hive;

import static com.datastrato.graviton.catalog.BaseCatalog.CATALOG_BYPASS_PREFIX;
import static com.datastrato.graviton.catalog.hive.HiveCatalogPropertiesMeta.CATALOG_CLIENT_POOL_MAXSIZE;
import static com.datastrato.graviton.catalog.hive.HiveCatalogPropertiesMeta.DEFAULT_CATALOG_CLIENT_POOL_MAXSIZE;
import static com.datastrato.graviton.catalog.hive.HiveTable.SUPPORT_TABLE_TYPES;
import static com.datastrato.graviton.catalog.hive.HiveTablePropertiesMetadata.COMMENT;
import static com.datastrato.graviton.catalog.hive.HiveTablePropertiesMetadata.TABLE_TYPE;

import com.datastrato.graviton.NameIdentifier;
import com.datastrato.graviton.Namespace;
import com.datastrato.graviton.catalog.CatalogOperations;
import com.datastrato.graviton.catalog.PropertiesMetadata;
import com.datastrato.graviton.catalog.hive.converter.ToHiveType;
import com.datastrato.graviton.exceptions.NoSuchCatalogException;
import com.datastrato.graviton.exceptions.NoSuchSchemaException;
import com.datastrato.graviton.exceptions.NoSuchTableException;
import com.datastrato.graviton.exceptions.NonEmptySchemaException;
import com.datastrato.graviton.exceptions.SchemaAlreadyExistsException;
import com.datastrato.graviton.exceptions.TableAlreadyExistsException;
import com.datastrato.graviton.meta.AuditInfo;
import com.datastrato.graviton.meta.CatalogEntity;
import com.datastrato.graviton.rel.Column;
import com.datastrato.graviton.rel.Distribution;
import com.datastrato.graviton.rel.SchemaChange;
import com.datastrato.graviton.rel.SortOrder;
import com.datastrato.graviton.rel.SupportsSchemas;
import com.datastrato.graviton.rel.Table;
import com.datastrato.graviton.rel.TableCatalog;
import com.datastrato.graviton.rel.TableChange;
import com.datastrato.graviton.rel.transforms.Transform;
import com.datastrato.graviton.rel.transforms.Transforms;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Operations for interacting with the Hive catalog in Graviton. */
public class HiveCatalogOperations implements CatalogOperations, SupportsSchemas, TableCatalog {

  public static final Logger LOG = LoggerFactory.getLogger(HiveCatalogOperations.class);

  @VisibleForTesting HiveClientPool clientPool;

  @VisibleForTesting HiveConf hiveConf;

  private final CatalogEntity entity;

  private HiveTablePropertiesMetadata tablePropertiesMetadata;

  private HiveCatalogPropertiesMeta catalogPropertiesMetadata;

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
    Configuration hadoopConf = new Configuration();
    conf.forEach(hadoopConf::set);
    hiveConf = new HiveConf(hadoopConf, HiveCatalogOperations.class);

    // Overwrite hive conf with graviton conf if exists
    conf.forEach(
        (key, value) -> {
          if (key.startsWith(CATALOG_BYPASS_PREFIX)) {
            // Trim bypass prefix and pass it to hive conf
            hiveConf.set(key.substring(CATALOG_BYPASS_PREFIX.length()), value);
          }
        });

    this.clientPool = new HiveClientPool(getCatalogClientPoolMaxsize(conf), hiveConf);
    this.tablePropertiesMetadata = new HiveTablePropertiesMetadata();
    this.catalogPropertiesMetadata = new HiveCatalogPropertiesMeta();
  }

  @VisibleForTesting
  int getCatalogClientPoolMaxsize(Map<String, String> conf) {
    return Integer.parseInt(
        conf.getOrDefault(
            CATALOG_CLIENT_POOL_MAXSIZE, String.valueOf(DEFAULT_CATALOG_CLIENT_POOL_MAXSIZE)));
  }

  /** Closes the Hive catalog and releases the associated client pool. */
  @Override
  public void close() {
    if (clientPool != null) {
      clientPool.close();
      clientPool = null;
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
   * @param metadata The metadata properties for the schema.
   * @return The created {@link HiveSchema}.
   * @throws NoSuchCatalogException If the provided namespace is invalid or does not exist.
   * @throws SchemaAlreadyExistsException If a schema with the same name already exists.
   */
  @Override
  public HiveSchema createSchema(NameIdentifier ident, String comment, Map<String, String> metadata)
      throws NoSuchCatalogException, SchemaAlreadyExistsException {
    try {
      HiveSchema hiveSchema =
          new HiveSchema.Builder()
              .withName(ident.name())
              .withComment(comment)
              .withProperties(metadata)
              .withConf(hiveConf)
              .withAuditInfo(
                  new AuditInfo.Builder()
                      .withCreator(currentUser())
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
          String.format(
              "Hive schema (database) '%s' already exists in Hive Metastore", ident.name()),
          e);

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
          String.format(
              "Hive schema (database) does not exist: %s in Hive Metastore", ident.name()),
          e);

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

      LOG.info("Altered Hive schema (database) {} in Hive Metastore", ident.name());
      return HiveSchema.fromHiveDB(alteredDatabase, hiveConf);

    } catch (NoSuchObjectException e) {
      throw new NoSuchSchemaException(
          String.format("Hive schema (database) %s does not exist in Hive Metastore", ident.name()),
          e);

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
          String.format(
              "Hive schema (database) %s is not empty. One or more tables exist.", ident.name()),
          e);

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
      throw new NoSuchSchemaException("Schema (database) does not exist " + namespace);
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
          "Schema (database) does not exist " + namespace + " in Hive Metastore");

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
    NameIdentifier schemaIdent = NameIdentifier.of(tableIdent.namespace().levels());

    try {
      org.apache.hadoop.hive.metastore.api.Table table =
          clientPool.run(c -> c.getTable(schemaIdent.name(), tableIdent.name()));
      HiveTable hiveTable = HiveTable.fromHiveTable(table);

      LOG.info("Loaded Hive table {} from Hive Metastore ", tableIdent.name());
      return hiveTable;

    } catch (NoSuchObjectException e) {
      throw new NoSuchTableException(
          String.format("Hive table does not exist: %s in Hive Metastore", tableIdent.name()), e);

    } catch (InterruptedException | TException e) {
      throw new RuntimeException(
          "Failed to load Hive table " + tableIdent.name() + " from Hive metastore", e);
    }
  }

  private void validateDistributionAndSort(Distribution distribution, SortOrder[] sortOrder) {
    if (distribution != Distribution.NONE) {
      boolean allNameReference =
          Arrays.stream(distribution.transforms())
              .allMatch(t -> t instanceof Transforms.NamedReference);
      Preconditions.checkArgument(
          allNameReference, "Hive distribution only supports name reference");
    }

    if (ArrayUtils.isNotEmpty(sortOrder)) {
      boolean allNameReference =
          Arrays.stream(sortOrder)
              .allMatch(t -> t.getTransform() instanceof Transforms.NamedReference);
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
   * @param partitions The partitioning for the new table.
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
      Transform[] partitions,
      Distribution distribution,
      SortOrder[] sortOrders)
      throws NoSuchSchemaException, TableAlreadyExistsException {
    NameIdentifier schemaIdent = NameIdentifier.of(tableIdent.namespace().levels());

    Preconditions.checkArgument(
        Arrays.stream(partitions).allMatch(p -> p instanceof Transforms.NamedReference),
        "Hive partition only supports identity transform");
    validateDistributionAndSort(distribution, sortOrders);

    TableType tableType = (TableType) tablePropertiesMetadata.getOrDefault(properties, TABLE_TYPE);
    Preconditions.checkArgument(
        SUPPORT_TABLE_TYPES.contains(tableType.name()),
        "Unsupported table type: " + tableType.name());

    try {
      if (!schemaExists(schemaIdent)) {
        LOG.warn("Hive schema (database) does not exist: {}", schemaIdent);
        throw new NoSuchSchemaException("Hive Schema (database) does not exist " + schemaIdent);
      }

      HiveTable hiveTable =
          new HiveTable.Builder()
              .withName(tableIdent.name())
              .withSchemaName(schemaIdent.name())
              .withComment(comment)
              .withColumns(columns)
              .withProperties(properties)
              .withDistribution(distribution)
              .withSortOrders(sortOrders)
              .withAuditInfo(
                  new AuditInfo.Builder()
                      .withCreator(currentUser())
                      .withCreateTime(Instant.now())
                      .build())
              .withPartitions(partitions)
              .build();
      clientPool.run(
          c -> {
            c.createTable(hiveTable.toHiveTable(tablePropertiesMetadata));
            return null;
          });

      LOG.info("Created Hive table {} in Hive Metastore", tableIdent.name());
      return hiveTable;

    } catch (AlreadyExistsException e) {
      throw new TableAlreadyExistsException("Table already exists: " + tableIdent.name(), e);
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
            doAddColumn(cols, (TableChange.AddColumn) change);

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
      return HiveTable.fromHiveTable(alteredHiveTable);

    } catch (NoSuchObjectException e) {
      throw new NoSuchTableException(
          String.format("Hive table does not exist: %s in Hive Metastore", tableIdent.name()), e);
    } catch (TException | InterruptedException e) {
      if (e.getMessage().contains("types incompatible with the existing columns")) {
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
    } catch (IllegalArgumentException e) {
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

  void doAddColumn(List<FieldSchema> cols, TableChange.AddColumn change) {
    // add to the end by default
    int targetPosition =
        change.getPosition() == null ? cols.size() : columnPosition(cols, change.getPosition());
    cols.add(
        targetPosition,
        new FieldSchema(
            change.fieldNames()[0],
            change.getDataType().accept(ToHiveType.INSTANCE).getQualifiedName(),
            change.getComment()));
  }

  private void doDeleteColumn(List<FieldSchema> cols, TableChange.DeleteColumn change) {
    String columnName = change.fieldNames()[0];
    if (!cols.removeIf(c -> c.getName().equals(columnName)) && !change.getIfExists()) {
      throw new IllegalArgumentException("DeleteColumn does not exist: " + columnName);
    }
  }

  private void doRenameColumn(List<FieldSchema> cols, TableChange.RenameColumn change) {
    String columnName = change.fieldNames()[0];
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
    cols.get(indexOfColumn(cols, change.fieldNames()[0])).setComment(change.getNewComment());
  }

  private void doUpdateColumnPosition(
      List<FieldSchema> cols, TableChange.UpdateColumnPosition change) {
    String columnName = change.fieldNames()[0];
    int sourceIndex = indexOfColumn(cols, columnName);
    if (sourceIndex == -1) {
      throw new IllegalArgumentException("UpdateColumnPosition does not exist: " + columnName);
    }

    // update column position: remove then add to given position
    FieldSchema hiveColumn = cols.remove(sourceIndex);
    cols.add(columnPosition(cols, change.getPosition()), hiveColumn);
  }

  private void doUpdateColumnType(List<FieldSchema> cols, TableChange.UpdateColumnType change) {
    String columnName = change.fieldNames()[0];
    int indexOfColumn = indexOfColumn(cols, columnName);
    if (indexOfColumn == -1) {
      throw new IllegalArgumentException("UpdateColumnType does not exist: " + columnName);
    }
    cols.get(indexOfColumn)
        .setType(change.getNewDataType().accept(ToHiveType.INSTANCE).getQualifiedName());
  }

  /**
   * Drops a table from the Hive Metastore.
   *
   * @param tableIdent The identifier of the table to drop.
   * @return true if the table is successfully dropped; false if the table does not exist.
   */
  @Override
  public boolean dropTable(NameIdentifier tableIdent) {
    return dropHiveTable(tableIdent, false, false);
  }

  /**
   * Purges a table from the Hive Metastore.
   *
   * @param tableIdent The identifier of the table to purge.
   * @return true if the table is successfully purged; false if the table does not exist.
   * @throws UnsupportedOperationException If the table type is EXTERNAL_TABLE, it cannot be purged.
   */
  @Override
  public boolean purgeTable(NameIdentifier tableIdent) throws UnsupportedOperationException {
    // TODO(minghuang): HiveTable support specify `tableType`, then reject purge Hive table with
    //  `tableType` EXTERNAL_TABLE
    return dropHiveTable(tableIdent, true, true);
  }

  /**
   * Checks if the given namespace is a valid namespace for the Hive schema.
   *
   * @param tableIdent The namespace to validate.
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

  @Override
  public PropertiesMetadata tablePropertiesMetadata() throws UnsupportedOperationException {
    return tablePropertiesMetadata;
  }

  @Override
  public PropertiesMetadata catalogPropertiesMetadata() throws UnsupportedOperationException {
    return catalogPropertiesMetadata;
  }
}
