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

package org.apache.gravitino.spark.connector.iceberg;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergConstants;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergPropertiesUtils;
import org.apache.gravitino.credential.CredentialConstants;
import org.apache.gravitino.credential.CredentialPropertyUtils;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.spark.connector.GravitinoSparkConfig;
import org.apache.gravitino.spark.connector.PropertiesConverter;
import org.apache.gravitino.spark.connector.SparkTransformConverter;
import org.apache.gravitino.spark.connector.SparkTypeConverter;
import org.apache.gravitino.spark.connector.catalog.BaseCatalog;
import org.apache.gravitino.spark.connector.catalog.GravitinoCatalogManager;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.spark.SparkCatalog;
import org.apache.iceberg.spark.procedures.SparkProcedures;
import org.apache.iceberg.spark.source.HasIcebergCatalog;
import org.apache.iceberg.spark.source.SparkTable;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The GravitinoIcebergCatalog class extends the BaseCatalog to integrate with the Apache Iceberg
 * table format, providing specialized support for Iceberg-specific functionalities within Apache
 * Spark's ecosystem. This implementation can further adapt to specific interfaces such as
 * StagingTableCatalog and FunctionCatalog, allowing for advanced operations like table staging and
 * function management tailored to the needs of Iceberg tables.
 */
public class GravitinoIcebergCatalog extends BaseCatalog
    implements FunctionCatalog, ProcedureCatalog, HasIcebergCatalog {

  private static final Logger LOG = LoggerFactory.getLogger(GravitinoIcebergCatalog.class);

  @Override
  protected TableCatalog createAndInitSparkCatalog(
      String name, CaseInsensitiveStringMap options, Map<String, String> properties) {
    String jdbcDriver = properties.get(IcebergConstants.GRAVITINO_JDBC_DRIVER);
    if (StringUtils.isNotBlank(jdbcDriver)) {
      // If `spark.sql.hive.metastore.jars` is set, Spark will use an isolated client class loader
      // to load JDBC drivers, which makes Iceberg could not find corresponding JDBC driver.
      try {
        Class.forName(jdbcDriver);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
    String catalogBackendName = IcebergPropertiesUtils.getCatalogBackendName(properties);
    SparkConf sparkConf = SparkSession.active().sparkContext().conf();
    Optional<String> icebergRestUri =
        resolveIcebergRestUri(
            properties,
            key -> sparkConf.get(key, null),
            () -> GravitinoCatalogManager.get().getIcebergRestUri());
    Map<String, String> all;
    if (icebergRestUri.isPresent()) {
      all =
          buildAutoRoutedIcebergRestProperties(
              name, options, properties, icebergRestUri.get(), sparkConf);
    } else {
      all = getPropertiesConverter().toSparkCatalogProperties(options, properties);
      CredentialPropertyUtils.applyIcebergCredentials(
          CredentialPropertyUtils.getCredentials(gravitinoCatalogClient), all);
    }
    TableCatalog icebergCatalog = new SparkCatalog();
    icebergCatalog.initialize(catalogBackendName, new CaseInsensitiveStringMap(all));
    return icebergCatalog;
  }

  /**
   * Resolves the Iceberg REST server endpoint to route this catalog through, if any. Only hive/jdbc
   * backed catalogs are eligible; a catalog already configured with {@code catalog-backend=rest} or
   * {@code custom} is left untouched, and so is a catalog with no {@code catalog-backend} property
   * at all.
   *
   * <p>An eligible catalog whose warehouse has a native Iceberg FileIO (s3/gs/abfs-family schemes)
   * fails immediately unless {@code credential-providers} is configured: routing replaces any
   * static storage credentials for that FileIO with vended ones, so such a catalog would silently
   * lose storage access once routed. A warehouse scheme with no native FileIO (e.g. {@code
   * hdfs://}, {@code file://}) carries no such risk and is unaffected.
   */
  static Optional<String> resolveIcebergRestUri(
      Map<String, String> properties,
      UnaryOperator<String> sessionConfig,
      Supplier<Optional<String>> endpointDiscovery) {
    String backend = properties.get(IcebergConstants.CATALOG_BACKEND);
    if (backend == null) {
      return Optional.empty();
    }
    String normalizedBackend = backend.toLowerCase(Locale.ROOT);
    boolean eligible =
        IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_BACKEND_HIVE.equals(normalizedBackend)
            || IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_BACKEND_JDBC.equals(
                normalizedBackend);
    if (!eligible) {
      return Optional.empty();
    }

    String routingEnabled =
        sessionConfig.apply(GravitinoSparkConfig.GRAVITINO_ICEBERG_REST_ROUTING_ENABLED);
    if (StringUtils.isNotBlank(routingEnabled)
        && !"true".equalsIgnoreCase(routingEnabled)
        && !"false".equalsIgnoreCase(routingEnabled)) {
      throw new IllegalArgumentException(
          GravitinoSparkConfig.GRAVITINO_ICEBERG_REST_ROUTING_ENABLED
              + " must be true or false, but was: "
              + routingEnabled);
    }
    if ("false".equalsIgnoreCase(routingEnabled)) {
      return Optional.empty();
    }

    boolean warehouseHasNativeFileIo =
        IcebergPropertiesConverter.deriveFileIoImpl(properties.get(IcebergConstants.WAREHOUSE))
            != null;
    if (warehouseHasNativeFileIo
        && StringUtils.isBlank(properties.get(CredentialConstants.CREDENTIAL_PROVIDERS))) {
      throw new IllegalStateException(
          "Catalog's warehouse has a native Iceberg FileIO but no credential-providers "
              + "configured; routing through the Iceberg REST server replaces any static storage "
              + "credentials for that FileIO with vended ones, so this catalog would lose storage "
              + "access once routed. Configure credential-providers on the catalog, or set "
              + GravitinoSparkConfig.GRAVITINO_ICEBERG_REST_ROUTING_ENABLED
              + "=false to use legacy Hive/JDBC backend translation.");
    }

    String manualUri = sessionConfig.apply(GravitinoSparkConfig.GRAVITINO_ICEBERG_REST_URI);
    if (StringUtils.isNotBlank(manualUri)) {
      return Optional.of(manualUri);
    }

    boolean routingExplicitlyEnabled = "true".equalsIgnoreCase(routingEnabled);
    Optional<String> discoveredUri;
    try {
      discoveredUri = endpointDiscovery.get();
    } catch (RuntimeException e) {
      // endpointDiscovery can throw for reasons other than connectivity (e.g. a bug in client
      // bootstrapping), so the exception's own type is included rather than assuming it is
      // always a reachability/config problem.
      if (!routingExplicitlyEnabled) {
        LOG.warn(
            "Failed to discover the Iceberg REST endpoint ({}); falling back to legacy Hive/JDBC "
                + "backend translation. Set {}=true to require Iceberg REST routing.",
            e.getClass().getSimpleName(),
            GravitinoSparkConfig.GRAVITINO_ICEBERG_REST_ROUTING_ENABLED,
            e);
        return Optional.empty();
      }
      throw new IllegalStateException(
          "Failed to discover the Iceberg REST endpoint ("
              + e.getClass().getSimpleName()
              + "). Configure "
              + GravitinoSparkConfig.GRAVITINO_ICEBERG_REST_URI
              + ", use a Gravitino server that supports /api/system/iceberg-rest, or set "
              + GravitinoSparkConfig.GRAVITINO_ICEBERG_REST_ROUTING_ENABLED
              + "=false to use legacy Hive/JDBC backend translation.",
          e);
    }
    if (!discoveredUri.isPresent()) {
      if (!routingExplicitlyEnabled) {
        LOG.warn(
            "No Iceberg REST endpoint is available; falling back to legacy Hive/JDBC backend "
                + "translation. Set {}=true to require Iceberg REST routing.",
            GravitinoSparkConfig.GRAVITINO_ICEBERG_REST_ROUTING_ENABLED);
        return Optional.empty();
      }
      throw new IllegalStateException(
          "No Iceberg REST endpoint is available. Configure "
              + GravitinoSparkConfig.GRAVITINO_ICEBERG_REST_URI
              + ", use a Gravitino server that supports /api/system/iceberg-rest, or set "
              + GravitinoSparkConfig.GRAVITINO_ICEBERG_REST_ROUTING_ENABLED
              + "=false to use legacy Hive/JDBC backend translation.");
    }
    return discoveredUri;
  }

  private Map<String, String> buildAutoRoutedIcebergRestProperties(
      String gravitinoCatalogName,
      CaseInsensitiveStringMap options,
      Map<String, String> properties,
      String restUri,
      SparkConf sparkConf) {
    IcebergPropertiesConverter converter = (IcebergPropertiesConverter) getPropertiesConverter();
    Map<String, String> all =
        new HashMap<>(
            converter.buildIcebergRestProperties(
                gravitinoCatalogName,
                restUri,
                properties,
                getAutoRoutedIcebergRestClientConfig(sparkConf)));
    if (options != null) {
      all.putAll(options);
      // options can re-introduce a reserved routing key (e.g. a Spark-level `uri`/`prefix`
      // catalog option); re-derive them so options can never redirect a routed catalog.
      converter.reapplyReservedRestProperties(gravitinoCatalogName, restUri, all);
    }
    return all;
  }

  static Map<String, String> getAutoRoutedIcebergRestClientConfig(SparkConf sparkConf) {
    Map<String, String> explicitRestConfig =
        Stream.of(
                sparkConf.getAllWithPrefix(
                    GravitinoSparkConfig.GRAVITINO_ICEBERG_REST_CONFIG_PREFIX))
            .collect(Collectors.toMap(t -> t._1, t -> t._2, (oldVal, newVal) -> newVal));
    return IcebergRestOAuthConfig.resolve(sparkConf, explicitRestConfig);
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
    return isIcebergFunctionNamespace(namespace)
        ? ((SparkCatalog) sparkCatalog).listFunctions(namespace)
        : super.listFunctions(namespace);
  }

  @Override
  public UnboundFunction loadFunction(Identifier ident) throws NoSuchFunctionException {
    return isIcebergFunctionNamespace(ident.namespace())
        ? ((SparkCatalog) sparkCatalog).loadFunction(ident)
        : super.loadFunction(ident);
  }

  /**
   * Procedures will validate the equality of the catalog registered to Spark catalogManager and the
   * catalog passed to `ProcedureBuilder` which invokes loadProcedure(). To meet the requirement ,
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
      org.apache.gravitino.rel.Table gravitinoTable = loadGravitinoTable(ident);
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
    } catch (org.apache.gravitino.exceptions.NoSuchTableException e) {
      throw new NoSuchTableException(ident);
    }
  }

  @Override
  public org.apache.spark.sql.connector.catalog.Table loadTable(Identifier ident, long timestamp)
      throws NoSuchTableException {
    try {
      org.apache.gravitino.rel.Table gravitinoTable = loadGravitinoTable(ident);
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
    } catch (org.apache.gravitino.exceptions.NoSuchTableException e) {
      throw new NoSuchTableException(ident);
    }
  }

  private boolean isIcebergFunctionNamespace(String[] namespace) {
    try {
      return namespace.length == 0 || isSystemNamespace(namespace);
    } catch (IllegalAccessException
        | InvocationTargetException
        | NoSuchMethodException
        | ClassNotFoundException e) {
      throw new RuntimeException("Failed to check Iceberg function namespace", e);
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
