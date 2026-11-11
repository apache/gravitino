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

package org.apache.gravitino.spark.connector.jdbc;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.apache.commons.collections4.MapUtils;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.credential.Credential;
import org.apache.gravitino.credential.JdbcCredential;
import org.apache.gravitino.spark.connector.PropertiesConverter;
import org.apache.gravitino.spark.connector.SparkTransformConverter;
import org.apache.gravitino.spark.connector.SparkTypeConverter;
import org.apache.gravitino.spark.connector.catalog.BaseCatalog;
import org.apache.spark.sql.catalyst.analysis.NamespaceAlreadyExistsException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.SupportsNamespaces;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.errors.QueryCompilationErrors;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCRDD;
import org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCTable;
import org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCTableCatalog;
import org.apache.spark.sql.jdbc.JdbcDialect;
import org.apache.spark.sql.jdbc.JdbcDialects;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

public class GravitinoJdbcCatalog extends BaseCatalog {

  private static final Logger LOG = LoggerFactory.getLogger(GravitinoJdbcCatalog.class);

  private static final String PREFER_TIMESTAMP_NTZ = "preferTimestampNTZ";

  private boolean useTimestampNtz;

  // The Spark JDBC options the catalog was initialized with, used to read a table's schema again.
  private Map<String, String> jdbcOptions = Collections.emptyMap();

  @Override
  protected TableCatalog createAndInitSparkCatalog(
      String name, CaseInsensitiveStringMap options, Map<String, String> properties) {
    JDBCTableCatalog jdbcTableCatalog = new JDBCTableCatalog();
    Map<String, String> all =
        getPropertiesConverter().toSparkCatalogProperties(options, properties);
    applyJdbcCredential(gravitinoCatalogClient, all);
    useTimestampNtz = false;
    if (supportsPreferTimestampNtz()) {
      String value = preferTimestampNtz(options, properties);
      Preconditions.checkArgument(
          "true".equalsIgnoreCase(value) || "false".equalsIgnoreCase(value),
          "Invalid value '%s' for %s (or %s%s) in catalog %s; use true or false",
          value,
          PREFER_TIMESTAMP_NTZ,
          PropertiesConverter.SPARK_PROPERTY_PREFIX,
          PREFER_TIMESTAMP_NTZ,
          name);
      useTimestampNtz = Boolean.parseBoolean(value);
      Preconditions.checkArgument(
          !useTimestampNtz || supportsTimestampNtz(),
          "preferTimestampNTZ=true in catalog %s requires Spark 3.5 or later; use false on earlier versions",
          name);
    }
    // One value drives both the reader and converter for every provider. Explicitly disable NTZ
    // for catalogs that do not support the option, including when Spark's session timestamp type
    // defaults to NTZ.
    setPreferTimestampNtz(all, useTimestampNtz);
    jdbcTableCatalog.initialize(name, new CaseInsensitiveStringMap(all));
    jdbcOptions = new HashMap<>(all);
    return jdbcTableCatalog;
  }

  /**
   * Loads the table from Spark's JDBC catalog. Spark reports any failure to read the table schema,
   * such as a permission the catalog user lacks, as a missing table. Gravitino has already found
   * the table, so read the schema again and report the source's own error instead.
   *
   * @param ident the Spark table identifier
   * @return the Spark JDBC table
   */
  @Override
  protected Table loadSparkTable(Identifier ident) {
    try {
      return sparkCatalog.loadTable(ident);
    } catch (NoSuchTableException e) {
      String table = String.join(".", getDatabase(ident), ident.name());
      SQLException sourceError = sourceError(jdbcOptions, ident);
      if (sourceError != null) {
        throw new RuntimeException(
            String.format(
                "Failed to read table %s from the JDBC source: %s",
                table, sourceError.getMessage()),
            sourceError);
      }
      throw new RuntimeException(String.format("Failed to load the real sparkTable: %s", table), e);
    }
  }

  /**
   * Reads a table's schema the way Spark's JDBC catalog does and returns the source's error.
   *
   * @param options the catalog's Spark JDBC options
   * @param ident the Spark table identifier
   * @return the source's error, or null if the schema can be read or no SQL error explains it
   */
  @VisibleForTesting
  @Nullable
  static SQLException sourceError(Map<String, String> options, Identifier ident) {
    String url = options.get(JDBCOptions.JDBC_URL());
    if (url == null) {
      return null;
    }
    JdbcDialect dialect = JdbcDialects.get(url);
    String table =
        Stream.concat(Arrays.stream(ident.namespace()), Stream.of(ident.name()))
            .map(dialect::quoteIdentifier)
            .collect(Collectors.joining("."));
    Map<String, String> tableOptions = new HashMap<>(options);
    tableOptions.put(JDBCOptions.JDBC_TABLE_NAME(), table);
    try {
      JDBCRDD.resolveTable(new JDBCOptions(toScalaMap(tableOptions)));
      return null;
    } catch (Exception e) {
      // Scala does not declare checked exceptions, so the SQLException arrives as Exception and
      // can be wrapped. Track visited causes by identity so a cause cycle ends the walk.
      Set<Throwable> visited = Collections.newSetFromMap(new IdentityHashMap<>());
      for (Throwable t = e; t != null && visited.add(t); t = t.getCause()) {
        if (t instanceof SQLException) {
          return (SQLException) t;
        }
      }
      LOG.debug("Failed to read the schema of {} again", table, e);
      return null;
    }
  }

  /**
   * Overwrites the Spark JDBC user and password in {@code sparkProperties} with credentials
   * obtained from the server via credential vending, if available.
   *
   * @param catalog the Gravitino catalog client
   * @param sparkProperties the mutable Spark properties map to update
   */
  @VisibleForTesting
  static void applyJdbcCredential(Catalog catalog, Map<String, String> sparkProperties) {
    Credential[] credentials;
    try {
      credentials = catalog.supportsCredentials().getCredentials();
    } catch (UnsupportedOperationException e) {
      return;
    }
    for (Credential credential : credentials) {
      if (credential instanceof JdbcCredential) {
        JdbcCredential jdbcCredential = (JdbcCredential) credential;
        sparkProperties.put(JdbcPropertiesConstants.SPARK_JDBC_USER, jdbcCredential.jdbcUser());
        sparkProperties.put(
            JdbcPropertiesConstants.SPARK_JDBC_PASSWORD, jdbcCredential.jdbcPassword());
        return;
      }
    }
  }

  @Override
  protected Table createSparkTable(
      Identifier identifier,
      org.apache.gravitino.rel.Table gravitinoTable,
      Table sparkTable,
      TableCatalog sparkCatalog,
      PropertiesConverter propertiesConverter,
      SparkTransformConverter sparkTransformConverter,
      SparkTypeConverter sparkTypeConverter) {
    return new SparkJdbcTable(
        identifier,
        gravitinoTable,
        (JDBCTable) sparkTable,
        (JDBCTableCatalog) sparkCatalog,
        propertiesConverter,
        sparkTransformConverter,
        sparkTypeConverter);
  }

  @Override
  protected PropertiesConverter getPropertiesConverter() {
    return JdbcPropertiesConverter.getInstance();
  }

  @Override
  protected SparkTransformConverter getSparkTransformConverter() {
    return new SparkTransformConverter(false);
  }

  @Override
  protected SparkTypeConverter getSparkTypeConverter() {
    return new SparkJdbcTypeConverter(useTimestampNtz);
  }

  /**
   * Returns whether this version's JDBC reader preserves timestamp-without-time-zone values.
   *
   * @return true for readers preserving wall-clock values with preferTimestampNTZ
   */
  protected boolean supportsTimestampNtz() {
    return false;
  }

  /**
   * Returns whether this catalog honors the {@code preferTimestampNTZ} option. PostgreSQL catalogs
   * do. A subclass whose source and Spark JDBC dialect keep zone-free timestamps as wall-clock
   * values can opt in; every other catalog reads with the option off.
   *
   * <p>Opting in does not bypass the reader version check: enabling NTZ still requires {@link
   * #supportsTimestampNtz()} to return true, which the built-in catalogs support on Spark 3.5 or
   * later.
   *
   * @return true if the catalog reads {@code preferTimestampNTZ} from its options
   */
  protected boolean supportsPreferTimestampNtz() {
    return "jdbc-postgresql".equalsIgnoreCase(gravitinoCatalogClient.provider());
  }

  /**
   * Returns the timestamp mode selected when the catalog was initialized. A subclass that overrides
   * {@link #getSparkTypeConverter()} must use it so its converter agrees with the JDBC reader.
   *
   * @return whether zone-free timestamps use Spark TimestampNTZType
   */
  protected boolean useTimestampNtz() {
    return useTimestampNtz;
  }

  @Override
  public void createNamespace(String[] namespace, Map<String, String> metadata)
      throws NamespaceAlreadyExistsException {
    Map<String, String> properties = Maps.newHashMap();
    if (MapUtils.isNotEmpty(metadata)) {
      metadata.forEach(
          (k, v) -> {
            switch (k) {
              case SupportsNamespaces.PROP_COMMENT:
                properties.put(k, v);
                break;
              case SupportsNamespaces.PROP_OWNER:
                break;
              case SupportsNamespaces.PROP_LOCATION:
                throw new RuntimeException(
                    QueryCompilationErrors.cannotCreateJDBCNamespaceUsingProviderError());
              default:
                throw new RuntimeException(
                    QueryCompilationErrors.cannotCreateJDBCNamespaceWithPropertyError(k));
            }
          });
    }
    super.createNamespace(namespace, properties);
  }

  @VisibleForTesting
  static void setPreferTimestampNtz(Map<String, String> properties, boolean value) {
    properties.keySet().removeIf(PREFER_TIMESTAMP_NTZ::equalsIgnoreCase);
    properties.put(PREFER_TIMESTAMP_NTZ, String.valueOf(value));
  }

  // The cast is redundant on Scala 2.12 and required on Scala 2.13.
  @SuppressWarnings({"unchecked", "cast"})
  private static scala.collection.immutable.Map<String, String> toScalaMap(
      Map<String, String> map) {
    // Built entry by entry because the Java-to-Scala map converters differ between Scala 2.12
    // and 2.13.
    scala.collection.immutable.Map<String, String> result =
        scala.collection.immutable.Map$.MODULE$.empty();
    for (Map.Entry<String, String> entry : map.entrySet()) {
      result =
          (scala.collection.immutable.Map<String, String>)
              result.$plus(new Tuple2<>(entry.getKey(), entry.getValue()));
    }
    return result;
  }

  /**
   * Resolves the opt-in NTZ option. A Spark catalog option wins over a Gravitino catalog property
   * with the {@code spark.bypass.} prefix, as in {@link
   * PropertiesConverter#toSparkCatalogProperties(CaseInsensitiveStringMap, Map)}.
   */
  private static String preferTimestampNtz(
      CaseInsensitiveStringMap options, Map<String, String> properties) {
    if (options != null && options.containsKey(PREFER_TIMESTAMP_NTZ)) {
      return options.get(PREFER_TIMESTAMP_NTZ);
    }
    if (properties != null) {
      for (Map.Entry<String, String> entry : properties.entrySet()) {
        String key = entry.getKey();
        // The prefix is case-sensitive, as in PropertiesConverter; the Spark option name is not.
        if (key.startsWith(PropertiesConverter.SPARK_PROPERTY_PREFIX)
            && PREFER_TIMESTAMP_NTZ.equalsIgnoreCase(
                key.substring(PropertiesConverter.SPARK_PROPERTY_PREFIX.length()))) {
          return entry.getValue();
        }
      }
    }
    return "false";
  }
}
