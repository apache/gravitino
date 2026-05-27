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
import org.apache.gravitino.spark.connector.PropertiesConverter;
import org.apache.gravitino.spark.connector.SparkTransformConverter;
import org.apache.gravitino.spark.connector.SparkTypeConverter;
import org.apache.gravitino.spark.connector.catalog.BaseCatalog;
import org.apache.gravitino.spark.connector.hive.SparkHiveTable;
import org.apache.gravitino.spark.connector.hive.SparkHiveTypeConverter;
import org.apache.gravitino.spark.connector.iceberg.SparkIcebergTable;
import org.apache.iceberg.spark.SparkCatalog;
import org.apache.iceberg.spark.source.SparkTable;
import org.apache.kyuubi.spark.connector.hive.HiveTable;
import org.apache.kyuubi.spark.connector.hive.HiveTableCatalog;
import org.apache.spark.sql.catalyst.analysis.NamespaceAlreadyExistsException;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.SupportsNamespaces;
import org.apache.spark.sql.connector.catalog.Table;
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

  /** Creates a new GravitinoGlueCatalog. */
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
   * Routes Spark table loading to the correct backend after Gravitino creates the table.
   *
   * <p>Iceberg tables are loaded from the Iceberg GlueCatalog; they are never registered in Derby.
   * Hive tables are loaded from Derby, syncing from Glue first if the entry is missing.
   */
  @Override
  protected Table loadSparkTable(Identifier ident) {
    try {
      org.apache.gravitino.rel.Table gravitinoTable = loadGravitinoTable(ident);
      if (isIcebergTable(gravitinoTable)) {
        return loadIcebergSparkTable(ident, getOrCreateIcebergGlueCatalog());
      }
      syncNamespaceToDerby(ident.namespace());
      try {
        return sparkCatalog.loadTable(ident);
      } catch (NoSuchTableException e) {
        syncTableToDerby(ident, gravitinoTable);
        return sparkCatalog.loadTable(ident);
      }
    } catch (NoSuchTableException e) {
      throw new RuntimeException(
          String.format("Failed to load spark table: %s.%s", getDatabase(ident), ident.name()), e);
    }
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
   * Overrides renameTable to keep Derby in sync after the Gravitino rename. The old Derby entry is
   * dropped eagerly; the new entry is synced lazily on the next loadTable call.
   */
  @Override
  public void renameTable(Identifier oldIdent, Identifier newIdent)
      throws NoSuchTableException, TableAlreadyExistsException {
    super.renameTable(oldIdent, newIdent);
    dropFromDerby(oldIdent);
  }

  @Override
  protected Table createSparkTable(
      Identifier identifier,
      org.apache.gravitino.rel.Table gravitinoTable,
      Table sparkTable,
      TableCatalog sparkHiveCatalog,
      PropertiesConverter propertiesConverter,
      SparkTransformConverter sparkTransformConverter,
      SparkTypeConverter sparkTypeConverter) {

    if (isIcebergTable(gravitinoTable)) {
      return new SparkIcebergTable(
          identifier,
          gravitinoTable,
          (SparkTable) sparkTable,
          getOrCreateIcebergGlueCatalog(),
          propertiesConverter,
          sparkTransformConverter,
          sparkTypeConverter);
    }

    return new SparkHiveTable(
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
    return new SparkHiveTypeConverter();
  }

  /**
   * Returns true if the Gravitino table is an Iceberg-format table based on its properties.
   *
   * @param gravitinoTable the Gravitino table to inspect
   * @return true for Iceberg tables, false otherwise
   */
  static boolean isIcebergTable(org.apache.gravitino.rel.Table gravitinoTable) {
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
    SparkCatalog catalog = new SparkCatalog();
    catalog.initialize(catalogName + "_iceberg", new CaseInsensitiveStringMap(icebergProperties));
    return catalog;
  }

  /**
   * Loads the raw Spark table from the Iceberg GlueCatalog.
   *
   * @param identifier the table identifier
   * @param icebergCatalog the Iceberg SparkCatalog
   * @return the Spark table
   */
  private Table loadIcebergSparkTable(Identifier identifier, SparkCatalog icebergCatalog)
      throws NoSuchTableException {
    return icebergCatalog.loadTable(identifier);
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
   * Creates a Derby entry from an already-loaded Gravitino table. Called when loadTable() detects
   * the table is missing from Derby (e.g., after a JVM restart or against a pre-populated catalog).
   * The caller supplies the Gravitino table to avoid a redundant load and eliminate the TOCTOU
   * window that would exist if we re-fetched it here.
   */
  private void syncTableToDerby(Identifier ident, org.apache.gravitino.rel.Table gravitinoTable) {
    try {
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
      Map<String, String> gravitinoProps = gravitinoTable.properties();
      String location = gravitinoProps.get(GlueConstants.LOCATION);
      if (location != null) {
        props.put(TableCatalog.PROP_LOCATION, location);
      }
      String inputFormat = gravitinoProps.get(GlueConstants.INPUT_FORMAT_CLASS);
      if (inputFormat != null) {
        props.put("hive.input-format", inputFormat);
      }
      String outputFormat = gravitinoProps.get(GlueConstants.OUTPUT_FORMAT);
      if (outputFormat != null) {
        props.put("hive.output-format", outputFormat);
      }
      String serdeLib = gravitinoProps.get(GlueConstants.SERDE_LIB);
      if (serdeLib != null) {
        props.put("hive.serde", serdeLib);
      }

      syncNamespaceToDerby(ident.namespace());
      sparkCatalog.createTable(ident, schema, sparkPartitions, props);
    } catch (TableAlreadyExistsException e) {
      // Race: another thread created it first — OK.
    } catch (Exception e) {
      LOG.warn("Failed to sync table {} to Derby", ident, e);
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
