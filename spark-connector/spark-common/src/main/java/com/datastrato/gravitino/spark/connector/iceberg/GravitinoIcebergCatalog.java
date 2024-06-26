/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.spark.connector.iceberg;

import com.datastrato.gravitino.rel.Table;
import com.datastrato.gravitino.spark.connector.PropertiesConverter;
import com.datastrato.gravitino.spark.connector.SparkTransformConverter;
import com.datastrato.gravitino.spark.connector.SparkTypeConverter;
import com.datastrato.gravitino.spark.connector.catalog.BaseCatalog;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.Optional;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.spark.SparkCatalog;
import org.apache.iceberg.spark.procedures.SparkProcedures;
import org.apache.iceberg.spark.source.HasIcebergCatalog;
import org.apache.iceberg.spark.source.SparkTable;
import org.apache.spark.sql.catalyst.analysis.NoSuchFunctionException;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.NoSuchProcedureException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.connector.catalog.FunctionCatalog;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.catalog.functions.UnboundFunction;
import org.apache.spark.sql.connector.iceberg.catalog.Procedure;
import org.apache.spark.sql.connector.iceberg.catalog.ProcedureCatalog;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/**
 * The GravitinoIcebergCatalog class extends the BaseCatalog to integrate with the Iceberg table
 * format, providing specialized support for Iceberg-specific functionalities within Spark's
 * ecosystem. This implementation can further adapt to specific interfaces such as
 * StagingTableCatalog and FunctionCatalog, allowing for advanced operations like table staging and
 * function management tailored to the needs of Iceberg tables.
 */
public class GravitinoIcebergCatalog extends BaseCatalog
    implements FunctionCatalog, ProcedureCatalog, HasIcebergCatalog {

  @Override
  protected TableCatalog createAndInitSparkCatalog(
      String name, CaseInsensitiveStringMap options, Map<String, String> properties) {
    String catalogBackendName =
        Optional.ofNullable(
                properties.get(IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_BACKEND_NAME))
            .orElse(name);
    Map<String, String> all =
        getPropertiesConverter().toSparkCatalogProperties(options, properties);
    TableCatalog icebergCatalog = new SparkCatalog();
    icebergCatalog.initialize(catalogBackendName, new CaseInsensitiveStringMap(all));
    return icebergCatalog;
  }

  @Override
  protected org.apache.spark.sql.connector.catalog.Table createSparkTable(
      Identifier identifier,
      Table gravitinoTable,
      org.apache.spark.sql.connector.catalog.Table sparkTable,
      TableCatalog sparkIcebergCatalog,
      PropertiesConverter propertiesConverter,
      SparkTransformConverter sparkTransformConverter,
      SparkTypeConverter sparkTypeConverter) {
    return new SparkIcebergTable(
        identifier,
        gravitinoTable,
        (SparkTable) sparkTable,
        (SparkCatalog) sparkIcebergCatalog,
        propertiesConverter,
        sparkTransformConverter,
        sparkTypeConverter);
  }

  @Override
  protected PropertiesConverter getPropertiesConverter() {
    return IcebergPropertiesConverter.getInstance();
  }

  @Override
  protected SparkTransformConverter getSparkTransformConverter() {
    return new SparkTransformConverter(true);
  }

  @Override
  public Identifier[] listFunctions(String[] namespace) throws NoSuchNamespaceException {
    return ((SparkCatalog) sparkCatalog).listFunctions(namespace);
  }

  @Override
  public UnboundFunction loadFunction(Identifier ident) throws NoSuchFunctionException {
    return ((SparkCatalog) sparkCatalog).loadFunction(ident);
  }

  /**
   * Proceduers will validate the equality of the catalog registered to Spark catalogManager and the
   * catalog passed to `ProcedureBuilder` which invokes loadProceduer(). To meet the requirement ,
   * override the method to pass `GravitinoIcebergCatalog` to the `ProcedureBuilder` instead of the
   * internal spark catalog.
   */
  @Override
  public Procedure loadProcedure(Identifier identifier) throws NoSuchProcedureException {
    String[] namespace = identifier.namespace();
    String name = identifier.name();

    try {
      if (isSystemNamespace(namespace)) {
        SparkProcedures.ProcedureBuilder builder = SparkProcedures.newBuilder(name);
        if (builder != null) {
          return builder.withTableCatalog(this).build();
        }
      }
    } catch (NoSuchMethodException
        | IllegalAccessException
        | InvocationTargetException
        | ClassNotFoundException e) {
      throw new RuntimeException("Failed to load Iceberg Procedure " + identifier, e);
    }

    throw new NoSuchProcedureException(identifier);
  }

  @Override
  public Catalog icebergCatalog() {
    return ((SparkCatalog) sparkCatalog).icebergCatalog();
  }

  @Override
  public org.apache.spark.sql.connector.catalog.Table loadTable(Identifier ident, String version)
      throws NoSuchTableException {
    try {
      com.datastrato.gravitino.rel.Table gravitinoTable = loadGravitinoTable(ident);
      org.apache.spark.sql.connector.catalog.Table sparkTable = loadSparkTable(ident, version);
      // Will create a catalog specific table
      return createSparkTable(
          ident,
          gravitinoTable,
          sparkTable,
          sparkCatalog,
          propertiesConverter,
          sparkTransformConverter,
          getSparkTypeConverter());
    } catch (com.datastrato.gravitino.exceptions.NoSuchTableException e) {
      throw new NoSuchTableException(ident);
    }
  }

  @Override
  public org.apache.spark.sql.connector.catalog.Table loadTable(Identifier ident, long timestamp)
      throws NoSuchTableException {
    try {
      com.datastrato.gravitino.rel.Table gravitinoTable = loadGravitinoTable(ident);
      org.apache.spark.sql.connector.catalog.Table sparkTable = loadSparkTable(ident, timestamp);
      // Will create a catalog specific table
      return createSparkTable(
          ident,
          gravitinoTable,
          sparkTable,
          sparkCatalog,
          propertiesConverter,
          sparkTransformConverter,
          getSparkTypeConverter());
    } catch (com.datastrato.gravitino.exceptions.NoSuchTableException e) {
      throw new NoSuchTableException(ident);
    }
  }

  private boolean isSystemNamespace(String[] namespace)
      throws NoSuchMethodException, InvocationTargetException, IllegalAccessException,
          ClassNotFoundException {
    Class<?> baseCatalog = Class.forName("org.apache.iceberg.spark.BaseCatalog");
    Method isSystemNamespace = baseCatalog.getDeclaredMethod("isSystemNamespace", String[].class);
    isSystemNamespace.setAccessible(true);
    return (Boolean) isSystemNamespace.invoke(baseCatalog, (Object) namespace);
  }

  private org.apache.spark.sql.connector.catalog.Table loadSparkTable(
      Identifier ident, String version) {
    try {
      return sparkCatalog.loadTable(ident, version);
    } catch (NoSuchTableException e) {
      throw new RuntimeException(
          String.format(
              "Failed to load the real sparkTable: %s",
              String.join(".", getDatabase(ident), ident.name())),
          e);
    }
  }

  private org.apache.spark.sql.connector.catalog.Table loadSparkTable(
      Identifier ident, long timestamp) {
    try {
      return sparkCatalog.loadTable(ident, timestamp);
    } catch (NoSuchTableException e) {
      throw new RuntimeException(
          String.format(
              "Failed to load the real sparkTable: %s",
              String.join(".", getDatabase(ident), ident.name())),
          e);
    }
  }
}
