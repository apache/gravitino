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

import java.util.Map;
import java.util.Set;
import org.apache.gravitino.spark.connector.PropertiesConverter;
import org.apache.gravitino.spark.connector.SparkTableChangeConverter;
import org.apache.gravitino.spark.connector.SparkTransformConverter;
import org.apache.gravitino.spark.connector.SparkTypeConverter;
import org.apache.gravitino.spark.connector.jdbc.GravitinoJdbcCatalogSpark35;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.catalog.TableWritePrivilege;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/** Spark 3.5 Gravitino-owned Doris catalog facade for governed batch reads. */
public class GravitinoDorisCatalogSpark35 extends GravitinoJdbcCatalogSpark35 {

  private String jdbcUrl;
  private String jdbcDriver;
  private String jdbcUser;
  private String jdbcPassword;
  private DorisJdbcReadOptions35 jdbcReadOptions;

  @Override
  protected TableCatalog createAndInitSparkCatalog(
      String name, CaseInsensitiveStringMap options, Map<String, String> properties) {
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
    DorisPhysicalSchema35 physicalSchema = catalog.loadPhysicalSchema(identifier);
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

  /** Rejects Spark's write-aware load path because this PR exposes batch read only. */
  @Override
  public Table loadTable(Identifier ident, Set<TableWritePrivilege> writePrivileges)
      throws NoSuchTableException {
    throw new UnsupportedOperationException("Apache Doris Spark support is read-only");
  }

  private DorisPropertiesConverter35 getDorisPropertiesConverter() {
    return DorisPropertiesConverter35.getInstance();
  }

  static String requireProperty(Map<String, String> properties, String key) {
    String value = properties.get(key);
    if (value == null || value.trim().isEmpty()) {
      throw new IllegalArgumentException("Doris catalog property is missing: " + key);
    }
    return value;
  }
}
