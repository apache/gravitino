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

package org.apache.gravitino.spark.connector.glue;

import com.google.common.base.Preconditions;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.gravitino.catalog.glue.GlueConstants;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.spark.connector.PropertiesConverter;
import org.apache.gravitino.spark.connector.SparkTransformConverter;
import org.apache.gravitino.spark.connector.SparkTypeConverter;
import org.apache.gravitino.spark.connector.catalog.BaseCatalog;
import org.apache.iceberg.spark.SparkCatalog;
import org.apache.kyuubi.spark.connector.hive.HiveTable;
import org.apache.kyuubi.spark.connector.hive.HiveTableCatalog;
import org.apache.spark.sql.catalyst.analysis.NamespaceAlreadyExistsException;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.SupportsNamespaces;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Gravitino Glue catalog implementation for Apache Spark.
 *
 * <p>This catalog handles mixed table types stored in AWS Glue Data Catalog:
 *
 * <ul>
 *   <li>Non-Iceberg tables (Hive, Delta, Parquet): routed to HiveTableCatalog for I/O
 *   <li>Iceberg tables: routed to Iceberg's GlueCatalog for I/O
 * </ul>
 *
 * <p>Table routing is based on the {@code table-format} property in Glue table parameters. Tables
 * with {@code table-format=ICEBERG} are delegated to the Iceberg backend.
 *
 * <p>Derby sync: Gravitino creates/modifies tables in AWS Glue. HiveTableCatalog (used as
 * sparkCatalog) uses an embedded Derby metastore for metadata validation. We must keep Derby in
 * sync with Glue for loadSparkTable() to succeed. Derby is populated lazily on createTable() and
 * loadTable(), and cleaned up on dropTable()/purgeTable()/renameTable().
 */
public class GravitinoGlueCatalog extends BaseCatalog {

  private static final Logger LOG = LoggerFactory.getLogger(GravitinoGlueCatalog.class);

  // Lazily initialized Iceberg GlueCatalog for Iceberg tables
  private volatile SparkCatalog icebergGlueCatalog;

  // Store original config for Iceberg catalog initialization
  private String catalogName;
  private Map<String, String> catalogProperties;

  public GravitinoGlueCatalog() {}

  /**
   * Creates a new HiveTableCatalog instance. Override in tests to inject mock instances.
   *
   * @return a new HiveTableCatalog
   */
  protected HiveTableCatalog createHiveTableCatalog() {
    return new HiveTableCatalog();
  }

  @Override
  protected TableCatalog createAndInitSparkCatalog(
      String name, CaseInsensitiveStringMap options, Map<String, String> properties) {
    this.catalogName = name;
    this.catalogProperties = properties;

    TableCatalog hiveCatalog = createHiveTableCatalog();
    Map<String, String> all =
        getPropertiesConverter().toSparkCatalogProperties(options, properties);
    hiveCatalog.initialize(name, new CaseInsensitiveStringMap(all));
    return hiveCatalog;
  }

  /**
   * Overrides createTable to handle Iceberg and non-Iceberg tables differently.
   *
   * <p>For Iceberg tables: delegates directly to the Iceberg GlueCatalog, which creates both the
   * Iceberg metadata in S3 and the Glue table entry with {@code table_type=ICEBERG}. BaseCatalog's
   * {@code loadSparkTable()} would fail because Derby never has Iceberg entries.
   *
   * <p>For non-Iceberg tables: pre-creates a placeholder in Derby before calling Gravitino.
   * BaseCatalog.createTable() calls loadSparkTable() after creating in Gravitino, which routes to
   * HiveTableCatalog.loadTable() → Derby. Without the placeholder, Derby returns
   * NoSuchTableException.
   */
  @Override
  public org.apache.spark.sql.connector.catalog.Table createTable(
      Identifier ident, StructType schema, Transform[] partitions, Map<String, String> properties)
      throws TableAlreadyExistsException, NoSuchNamespaceException {
    syncNamespaceToDerby(ident.namespace());
    if (isIcebergProperties(properties)) {
      SparkCatalog icebergCatalog = getOrCreateIcebergGlueCatalog();
      icebergCatalog.createTable(ident, schema, partitions, properties);
      try {
        org.apache.gravitino.rel.Table gravitinoTable = loadGravitinoTable(ident);
        org.apache.spark.sql.connector.catalog.Table icebergSparkTable =
            loadIcebergSparkTable(ident, icebergCatalog);
        return createSparkTable(
            ident,
            gravitinoTable,
            icebergSparkTable,
            sparkCatalog,
            getPropertiesConverter(),
            getSparkTransformConverter(),
            getSparkTypeConverter());
      } catch (NoSuchTableException e) {
        throw new RuntimeException("Failed to load Iceberg table after creation: " + ident, e);
      }
    }
    try {
      sparkCatalog.createTable(ident, schema, partitions, properties);
    } catch (TableAlreadyExistsException e) {
      // Already in Derby — OK.
    } catch (Exception e) {
      LOG.warn("Pre-create in Derby failed for {}: {}", ident, e.getMessage());
    }
    return super.createTable(ident, schema, partitions, properties);
  }

  /**
   * Overrides loadTable to handle Iceberg and non-Iceberg tables differently.
   *
   * <p>For Iceberg tables: loads the Gravitino metadata from Glue and delegates table loading to
   * the Iceberg GlueCatalog, bypassing Derby entirely. Iceberg tables are never registered in Derby
   * because HiveTableCatalog cannot represent them.
   *
   * <p>For non-Iceberg tables: ensures the table exists in Derby before calling super.loadTable().
   * This handles tables that exist in Gravitino/Glue but were not created through this catalog
   * instance (e.g., tables from a previous test run, or tables loaded after a JVM restart).
   */
  @Override
  public org.apache.spark.sql.connector.catalog.Table loadTable(Identifier ident)
      throws NoSuchTableException {
    org.apache.gravitino.rel.Table gravitinoTable = loadGravitinoTable(ident);
    if (isIcebergTable(gravitinoTable)) {
      SparkCatalog icebergCatalog = getOrCreateIcebergGlueCatalog();
      org.apache.spark.sql.connector.catalog.Table icebergSparkTable =
          loadIcebergSparkTable(ident, icebergCatalog);
      return createSparkTable(
          ident,
          gravitinoTable,
          icebergSparkTable,
          sparkCatalog,
          getPropertiesConverter(),
          getSparkTransformConverter(),
          getSparkTypeConverter());
    }
    // Non-Iceberg: ensure Derby is in sync, then delegate to BaseCatalog.loadTable().
    try {
      sparkCatalog.loadTable(ident);
    } catch (NoSuchTableException e) {
      syncTableToDerby(ident);
    }
    return super.loadTable(ident);
  }

  /**
   * Overrides dropTable to also remove the table from Derby. Without this, Derby accumulates stale
   * entries that cause TableAlreadyExistsException on the next createTable call for the same name.
   */
  @Override
  public boolean dropTable(Identifier ident) {
    dropFromDerby(ident);
    return super.dropTable(ident);
  }

  /** Overrides purgeTable to also remove the table from Derby. */
  @Override
  public boolean purgeTable(Identifier ident) {
    dropFromDerby(ident);
    return super.purgeTable(ident);
  }

  /**
   * Overrides renameTable to keep Derby in sync after the Gravitino rename. Gravitino/Glue is the
   * source of truth; Derby is updated lazily (old entry dropped; new entry synced on next
   * loadTable). Calling sparkCatalog.renameTable() before super.renameTable() can cause the old
   * entry to disappear from Glue before Gravitino's alterTable runs.
   */
  @Override
  public void renameTable(Identifier oldIdent, Identifier newIdent)
      throws NoSuchTableException, TableAlreadyExistsException {
    super.renameTable(oldIdent, newIdent);
  }

  @Override
  protected org.apache.spark.sql.connector.catalog.Table createSparkTable(
      Identifier identifier,
      Table gravitinoTable,
      org.apache.spark.sql.connector.catalog.Table sparkTable,
      TableCatalog sparkHiveCatalog,
      PropertiesConverter propertiesConverter,
      SparkTransformConverter sparkTransformConverter,
      SparkTypeConverter sparkTypeConverter) {

    if (isIcebergTable(gravitinoTable)) {
      SparkCatalog icebergCatalog = getOrCreateIcebergGlueCatalog();
      org.apache.spark.sql.connector.catalog.Table icebergSparkTable =
          loadIcebergSparkTable(identifier, icebergCatalog);
      return new org.apache.gravitino.spark.connector.iceberg.SparkIcebergTable(
          identifier,
          gravitinoTable,
          (org.apache.iceberg.spark.source.SparkTable) icebergSparkTable,
          icebergCatalog,
          propertiesConverter,
          sparkTransformConverter,
          sparkTypeConverter);
    }

    return new org.apache.gravitino.spark.connector.hive.SparkHiveTable(
        identifier,
        gravitinoTable,
        (HiveTable) sparkTable,
        (HiveTableCatalog) sparkHiveCatalog,
        propertiesConverter,
        sparkTransformConverter,
        sparkTypeConverter);
  }

  @Override
  protected PropertiesConverter getPropertiesConverter() {
    return GluePropertiesConverter.getInstance();
  }

  @Override
  protected SparkTransformConverter getSparkTransformConverter() {
    return new SparkTransformConverter(false);
  }

  @Override
  protected SparkTypeConverter getSparkTypeConverter() {
    return new org.apache.gravitino.spark.connector.hive.SparkHiveTypeConverter();
  }

  private static boolean isIcebergProperties(Map<String, String> properties) {
    if (properties == null) {
      return false;
    }
    String provider = properties.get("provider");
    if ("iceberg".equalsIgnoreCase(provider)) {
      return true;
    }
    String tableFormat = properties.get(GlueConstants.TABLE_FORMAT);
    return GlueConstants.TABLE_FORMAT_ICEBERG.equalsIgnoreCase(tableFormat);
  }

  static boolean isIcebergTable(Table gravitinoTable) {
    Map<String, String> properties = gravitinoTable.properties();
    if (properties == null) {
      return false;
    }
    // Gravitino convention: table-format=ICEBERG
    String tableFormat = properties.get(GlueConstants.TABLE_FORMAT);
    if (GlueConstants.TABLE_FORMAT_ICEBERG.equalsIgnoreCase(tableFormat)) {
      return true;
    }
    // Iceberg Glue catalog convention: table_type=ICEBERG stored in Glue table parameters
    return "ICEBERG".equalsIgnoreCase(properties.get("table_type"));
  }

  /**
   * Gets or creates the Iceberg GlueCatalog using double-checked locking.
   *
   * @return the Iceberg SparkCatalog
   */
  private SparkCatalog getOrCreateIcebergGlueCatalog() {
    if (icebergGlueCatalog == null) {
      synchronized (this) {
        if (icebergGlueCatalog == null) {
          Preconditions.checkArgument(
              catalogName != null && catalogProperties != null,
              "Catalog name and properties must be set before accessing Iceberg catalog");
          icebergGlueCatalog = createIcebergGlueCatalog();
        }
      }
    }
    return icebergGlueCatalog;
  }

  /**
   * Creates a new Iceberg GlueCatalog with appropriate configuration.
   *
   * @return the configured Iceberg SparkCatalog
   */
  private SparkCatalog createIcebergGlueCatalog() {
    GluePropertiesConverter converter = GluePropertiesConverter.getInstance();
    Map<String, String> icebergProperties = converter.toIcebergCatalogProperties(catalogProperties);

    // Iceberg GlueCatalog requires a warehouse path to derive default table locations.
    // Read from the active SparkSession if not explicitly set in catalog properties.
    if (!icebergProperties.containsKey("warehouse")) {
      try {
        org.apache.spark.sql.SparkSession spark = org.apache.spark.sql.SparkSession.active();
        String warehouseDir = spark.conf().get("spark.sql.warehouse.dir", null);
        if (warehouseDir != null) {
          icebergProperties.put("warehouse", warehouseDir);
        }
      } catch (Exception e) {
        LOG.warn(
            "Could not read spark.sql.warehouse.dir for Iceberg Glue catalog: {}", e.getMessage());
      }
    }

    SparkCatalog sparkCatalog = new SparkCatalog();
    sparkCatalog.initialize(
        catalogName + "_iceberg", new CaseInsensitiveStringMap(icebergProperties));
    return sparkCatalog;
  }

  /**
   * Loads a Spark table from the Iceberg GlueCatalog.
   *
   * @param identifier the table identifier
   * @param icebergCatalog the Iceberg SparkCatalog
   * @return the Spark table
   */
  private org.apache.spark.sql.connector.catalog.Table loadIcebergSparkTable(
      Identifier identifier, SparkCatalog icebergCatalog) {
    try {
      return icebergCatalog.loadTable(identifier);
    } catch (org.apache.spark.sql.catalyst.analysis.NoSuchTableException e) {
      throw new RuntimeException(
          String.format(
              "Failed to load Iceberg table: %s",
              String.join(".", getDatabase(identifier), identifier.name())),
          e);
    }
  }

  /** Creates the namespace in Derby if it does not already exist. */
  private void syncNamespaceToDerby(String[] namespace) {
    if (!(sparkCatalog instanceof SupportsNamespaces)) {
      return;
    }
    SupportsNamespaces ns = (SupportsNamespaces) sparkCatalog;
    try {
      ns.loadNamespaceMetadata(namespace);
    } catch (NoSuchNamespaceException e) {
      try {
        ns.createNamespace(namespace, Collections.emptyMap());
      } catch (NamespaceAlreadyExistsException ex) {
        // Created concurrently — OK.
      }
    }
  }

  /**
   * Loads the table from Gravitino and creates a corresponding entry in Derby. Called when
   * loadTable() detects the table is missing from Derby (e.g., after a JVM restart or when testing
   * against a pre-populated Glue catalog).
   */
  private void syncTableToDerby(Identifier ident) {
    try {
      Table gravitinoTable = loadGravitinoTable(ident);
      SparkTypeConverter typeConverter = getSparkTypeConverter();
      SparkTransformConverter transformConverter = getSparkTransformConverter();

      StructField[] fields =
          Arrays.stream(gravitinoTable.columns())
              .map(
                  col ->
                      DataTypes.createStructField(
                          col.name(), typeConverter.toSparkType(col.dataType()), col.nullable()))
              .toArray(StructField[]::new);
      StructType schema = new StructType(fields);

      Transform[] sparkPartitions =
          transformConverter.toSparkTransform(gravitinoTable.partitioning(), null, null);

      Map<String, String> props = new HashMap<>();
      // "location" is the Glue StorageDescriptor location key stored in Gravitino properties.
      String location = gravitinoTable.properties().get("location");
      if (location != null) {
        props.put(TableCatalog.PROP_LOCATION, location);
      }
      String provider = gravitinoTable.properties().get("provider");
      if (provider != null) {
        props.put("provider", provider);
      }

      syncNamespaceToDerby(ident.namespace());
      sparkCatalog.createTable(ident, schema, sparkPartitions, props);
    } catch (NoSuchTableException e) {
      LOG.warn("Cannot sync to Derby: table not found in Gravitino: {}", ident);
    } catch (TableAlreadyExistsException e) {
      // Race: another thread created it first — OK.
    } catch (Exception e) {
      LOG.warn("Failed to sync table {} to Derby: {}", ident, e.getMessage());
    }
  }

  /** Silently removes the table from Derby. Used during drop/purge to avoid stale entries. */
  private void dropFromDerby(Identifier ident) {
    try {
      sparkCatalog.invalidateTable(ident);
      sparkCatalog.dropTable(ident);
    } catch (Exception e) {
      LOG.debug("Drop from Derby failed for {} (may not exist): {}", ident, e.getMessage());
    }
  }
}
