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

import com.google.common.collect.ImmutableSet;
import java.util.Map;
import java.util.Set;
import org.apache.gravitino.spark.connector.PropertiesConverter;
import org.apache.gravitino.spark.connector.SparkTableChangeConverter;
import org.apache.gravitino.spark.connector.SparkTransformConverter;
import org.apache.gravitino.spark.connector.SparkTypeConverter;
import org.apache.gravitino.spark.connector.jdbc.GravitinoJdbcCatalogSpark35;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.NamespaceChange;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.catalog.TableChange;
import org.apache.spark.sql.connector.catalog.TableWritePrivilege;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/** Spark 3.5 Gravitino-owned Doris catalog facade for governed batch reads and writes. */
public class GravitinoDorisCatalogSpark35 extends GravitinoJdbcCatalogSpark35 {

  private String jdbcUrl;
  private String jdbcDriver;
  private String jdbcUser;
  private String jdbcPassword;
  private DorisJdbcReadOptions35 jdbcReadOptions;
  private DorisWritePolicy35 writePolicy = DorisWritePolicy35.disabled();

  @Override
  protected TableCatalog createAndInitSparkCatalog(
      String name, CaseInsensitiveStringMap options, Map<String, String> properties) {
    writePolicy = DorisWritePolicy35.from(properties);
    Map<String, String> all =
        getDorisPropertiesConverter().toSparkCatalogProperties(options, properties);
    applyJdbcCredential(gravitinoCatalogClient, all);
    jdbcUrl = requireProperty(all, "url");
    jdbcDriver = requireProperty(all, "driver");
    jdbcUser = requireProperty(all, "user");
    jdbcPassword = requireProperty(all, "password");
    requireProperty(all, DorisConnectorConstants35.DORIS_FE_NODES);
    requireProperty(all, DorisConnectorConstants35.DORIS_QUERY_PORT);
    all.put(DorisConnectorConstants35.DORIS_USER, jdbcUser);
    all.put(DorisConnectorConstants35.DORIS_PASSWORD, jdbcPassword);
    all.putAll(writePolicy.forcedConnectorOptions());
    jdbcReadOptions = DorisJdbcReadOptions35.from(properties);

    DorisTableCatalog35 catalog = new DorisTableCatalog35();
    catalog.initialize(name, new CaseInsensitiveStringMap(all));
    return catalog;
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
    if (!(sparkCatalog instanceof DorisTableCatalog35)) {
      throw new IllegalStateException("Unexpected Doris Spark catalog implementation");
    }
    DorisTableCatalog35 catalog = (DorisTableCatalog35) sparkCatalog;
    DorisPhysicalSchema35 physicalSchema =
        catalog.loadPhysicalSchema(identifier, jdbcUrl, jdbcDriver, jdbcUser, jdbcPassword);
    DorisReadSchema35 readSchema =
        DorisSchemaCompatibility35.plan(
            identifier, gravitinoTable, physicalSchema, sparkTypeConverter);
    Table jdbcTable =
        catalog.createJdbcTable(
            identifier, readSchema, jdbcUrl, jdbcDriver, jdbcUser, jdbcPassword, jdbcReadOptions);
    return new DorisHybridTable35(
        identifier,
        gravitinoTable,
        sparkTable,
        jdbcTable,
        readSchema,
        propertiesConverter,
        sparkTransformConverter,
        sparkTypeConverter);
  }

  @Override
  protected PropertiesConverter getPropertiesConverter() {
    return getDorisPropertiesConverter();
  }

  @Override
  protected SparkTypeConverter getSparkTypeConverter() {
    return new DorisSparkTypeConverter35();
  }

  @Override
  protected SparkTableChangeConverter getSparkTableChangeConverter(
      SparkTypeConverter sparkTypeConverter) {
    return new SparkTableChangeConverter(sparkTypeConverter);
  }

  /** Loads a governed write table only after the inherited MODIFY_TABLE authorization succeeds. */
  @Override
  public Table loadTable(Identifier ident, Set<TableWritePrivilege> writePrivileges)
      throws NoSuchTableException {
    DorisCapabilityPolicy35 capabilityPolicy = DorisCapabilityPolicy35.from(writePolicy);
    if (!capabilityPolicy.allowsTableWrites()) {
      throw capabilityPolicy.reject("table writes");
    }
    validateWritePrivileges(writePolicy, writePrivileges);
    Table table = loadTableForWriting(ident);
    if (!(table instanceof DorisHybridTable35)) {
      throw new IllegalStateException("Unexpected governed Doris table implementation");
    }
    return ((DorisHybridTable35) table).withGovernedWrite(writePolicy);
  }

  /** Rejects Spark table creation because specialized Doris catalog DDL is out of scope. */
  @Override
  public Table createTable(
      Identifier ident, StructType schema, Transform[] transforms, Map<String, String> properties) {
    throw rejectSparkDdl("create table");
  }

  /** Rejects Spark table alteration because specialized Doris catalog DDL is out of scope. */
  @Override
  public Table alterTable(Identifier ident, TableChange... changes) {
    throw rejectSparkDdl("alter table");
  }

  /** Rejects Spark table deletion because specialized Doris catalog DDL is out of scope. */
  @Override
  public boolean dropTable(Identifier ident) {
    throw rejectSparkDdl("drop table");
  }

  /** Rejects Spark table purging because specialized Doris catalog DDL is out of scope. */
  @Override
  public boolean purgeTable(Identifier ident) {
    throw rejectSparkDdl("purge table");
  }

  /** Rejects Spark table renaming because specialized Doris catalog DDL is out of scope. */
  @Override
  public void renameTable(Identifier oldIdent, Identifier newIdent) {
    throw rejectSparkDdl("rename table");
  }

  /** Rejects Spark namespace creation because specialized Doris catalog DDL is out of scope. */
  @Override
  public void createNamespace(String[] namespace, Map<String, String> metadata) {
    throw rejectSparkDdl("create namespace");
  }

  /** Rejects Spark namespace alteration because specialized Doris catalog DDL is out of scope. */
  @Override
  public void alterNamespace(String[] namespace, NamespaceChange... changes) {
    throw rejectSparkDdl("alter namespace");
  }

  /** Rejects Spark namespace deletion because specialized Doris catalog DDL is out of scope. */
  @Override
  public boolean dropNamespace(String[] namespace, boolean cascade) {
    throw rejectSparkDdl("drop namespace");
  }

  static void validateWritePrivileges(
      DorisWritePolicy35 writePolicy, Set<TableWritePrivilege> writePrivileges) {
    if (writePrivileges == null || writePrivileges.isEmpty()) {
      throw DorisCapabilityPolicy35.readOnly().reject("an empty write privilege request");
    }
    if (writePrivileges.equals(ImmutableSet.of(TableWritePrivilege.INSERT))) {
      return;
    }
    if (writePolicy.allowsTruncate()
        && writePrivileges.equals(
            ImmutableSet.of(TableWritePrivilege.INSERT, TableWritePrivilege.DELETE))) {
      return;
    }
    throw DorisCapabilityPolicy35.readOnly().reject("the requested write privileges");
  }

  static String requireProperty(Map<String, String> properties, String key) {
    String value = properties.get(key);
    if (value == null || value.trim().isEmpty()) {
      throw new IllegalArgumentException("Doris catalog property is missing: " + key);
    }
    return value;
  }

  private DorisPropertiesConverter35 getDorisPropertiesConverter() {
    return DorisPropertiesConverter35.getInstance();
  }

  private static UnsupportedOperationException rejectSparkDdl(String operation) {
    return new UnsupportedOperationException(
        "The governed Doris connector does not support Spark catalog DDL: " + operation);
  }
}
