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

package org.apache.gravitino.spark.connector.jdbc.doris;

import com.google.common.annotations.VisibleForTesting;
import java.util.Map;
import java.util.Set;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.credential.Credential;
import org.apache.gravitino.credential.JdbcCredential;
import org.apache.gravitino.spark.connector.PropertiesConverter;
import org.apache.gravitino.spark.connector.SparkTransformConverter;
import org.apache.gravitino.spark.connector.SparkTypeConverter;
import org.apache.gravitino.spark.connector.jdbc.GravitinoJdbcCatalogSpark35;
import org.apache.gravitino.spark.connector.jdbc.SparkJdbcTable;
import org.apache.gravitino.spark.connector.jdbc.SparkJdbcTypeConverter;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.NamespaceChange;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.catalog.TableChange;
import org.apache.spark.sql.connector.catalog.TableWritePrivilege;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCTableCatalog;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/** Spark 3.5 Gravitino-owned, read-only Doris catalog facade. */
public class GravitinoDorisCatalogSpark35 extends GravitinoJdbcCatalogSpark35 {

  private String jdbcUrl;
  private String jdbcDriver;
  private String jdbcUser;
  private String jdbcPassword;

  @Override
  protected TableCatalog createAndInitSparkCatalog(
      String name, CaseInsensitiveStringMap options, Map<String, String> properties) {
    Map<String, String> all =
        DorisPropertiesConverter35.getInstance().toSparkCatalogProperties(options, properties);
    JdbcCredential credential = requiredJdbcCredential(gravitinoCatalogClient);
    jdbcUrl = requireProperty(all, "url");
    jdbcDriver = requireProperty(all, "driver");
    jdbcUser = credential.jdbcUser();
    jdbcPassword = credential.jdbcPassword();
    all.put("user", jdbcUser);
    all.put("password", jdbcPassword);

    JDBCTableCatalog jdbcTableCatalog = new JDBCTableCatalog();
    jdbcTableCatalog.initialize(name, new CaseInsensitiveStringMap(all));
    return jdbcTableCatalog;
  }

  @Override
  @SuppressWarnings("deprecation")
  protected org.apache.spark.sql.connector.catalog.Table createSparkTable(
      Identifier identifier,
      org.apache.gravitino.rel.Table gravitinoTable,
      org.apache.spark.sql.connector.catalog.Table sparkTable,
      TableCatalog sparkCatalog,
      PropertiesConverter propertiesConverter,
      SparkTransformConverter sparkTransformConverter,
      SparkTypeConverter sparkTypeConverter) {
    if (!(sparkTable instanceof org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCTable)) {
      throw new IllegalStateException("Unexpected Spark JDBC table implementation");
    }
    DorisPhysicalSchemaValidator35.validate(
        identifier,
        gravitinoTable,
        sparkTable.schema(),
        jdbcUrl,
        jdbcDriver,
        jdbcUser,
        jdbcPassword,
        sparkTypeConverter);
    SparkJdbcTable jdbcTable =
        (SparkJdbcTable)
            super.createSparkTable(
                identifier,
                gravitinoTable,
                sparkTable,
                sparkCatalog,
                propertiesConverter,
                sparkTransformConverter,
                sparkTypeConverter);
    return new DorisReadOnlyTable35(jdbcTable);
  }

  @Override
  protected PropertiesConverter getPropertiesConverter() {
    return DorisPropertiesConverter35.getInstance();
  }

  @Override
  protected SparkTypeConverter getSparkTypeConverter() {
    return new SparkJdbcTypeConverter();
  }

  /** Rejects Spark's write-aware load path because this PR exposes batch read only. */
  @Override
  public org.apache.spark.sql.connector.catalog.Table loadTable(
      Identifier ident, Set<TableWritePrivilege> writePrivileges) throws NoSuchTableException {
    throw new UnsupportedOperationException("Apache Doris Spark support is read-only");
  }

  /** Rejects Spark catalog DDL in the specialized read-only path. */
  @Override
  public org.apache.spark.sql.connector.catalog.Table createTable(
      Identifier ident, StructType schema, Transform[] transforms, Map<String, String> properties) {
    throw unsupportedDdl("create table");
  }

  /** Rejects Spark catalog DDL in the specialized read-only path. */
  @Override
  public org.apache.spark.sql.connector.catalog.Table alterTable(
      Identifier ident, TableChange... changes) {
    throw unsupportedDdl("alter table");
  }

  /** Rejects Spark catalog DDL in the specialized read-only path. */
  @Override
  public boolean dropTable(Identifier ident) {
    throw unsupportedDdl("drop table");
  }

  /** Rejects Spark catalog DDL in the specialized read-only path. */
  @Override
  public boolean purgeTable(Identifier ident) {
    throw unsupportedDdl("purge table");
  }

  /** Rejects Spark catalog DDL in the specialized read-only path. */
  @Override
  public void renameTable(Identifier oldIdent, Identifier newIdent) {
    throw unsupportedDdl("rename table");
  }

  /** Rejects Spark catalog DDL in the specialized read-only path. */
  @Override
  public void createNamespace(String[] namespace, Map<String, String> metadata) {
    throw unsupportedDdl("create namespace");
  }

  /** Rejects Spark catalog DDL in the specialized read-only path. */
  @Override
  public void alterNamespace(String[] namespace, NamespaceChange... changes) {
    throw unsupportedDdl("alter namespace");
  }

  /** Rejects Spark catalog DDL in the specialized read-only path. */
  @Override
  public boolean dropNamespace(String[] namespace, boolean cascade) {
    throw unsupportedDdl("drop namespace");
  }

  @VisibleForTesting
  static JdbcCredential requiredJdbcCredential(Catalog catalog) {
    Credential[] credentials = catalog.supportsCredentials().getCredentials();
    for (Credential credential : credentials) {
      if (credential instanceof JdbcCredential) {
        return (JdbcCredential) credential;
      }
    }
    throw new IllegalArgumentException("A vended JDBC credential is required for Doris reads");
  }

  private static String requireProperty(Map<String, String> properties, String key) {
    String value = properties.get(key);
    if (value == null || value.trim().isEmpty()) {
      throw new IllegalArgumentException("Doris catalog property is missing: " + key);
    }
    return value;
  }

  private static UnsupportedOperationException unsupportedDdl(String operation) {
    return new UnsupportedOperationException(
        "The governed Doris read-only catalog does not support Spark catalog DDL: " + operation);
  }
}
