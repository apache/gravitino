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

import com.google.common.collect.Maps;
import java.util.Map;
import org.apache.commons.collections4.MapUtils;
import org.apache.gravitino.spark.connector.PropertiesConverter;
import org.apache.gravitino.spark.connector.SparkTransformConverter;
import org.apache.gravitino.spark.connector.SparkTypeConverter;
import org.apache.gravitino.spark.connector.catalog.BaseCatalog;
import org.apache.spark.sql.catalyst.analysis.NamespaceAlreadyExistsException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.SupportsNamespaces;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.errors.QueryCompilationErrors;
import org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCTable;
import org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCTableCatalog;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

public class GravitinoJdbcCatalog extends BaseCatalog {

  @Override
  protected TableCatalog createAndInitSparkCatalog(
      String name, CaseInsensitiveStringMap options, Map<String, String> properties) {
    JDBCTableCatalog jdbcTableCatalog = new JDBCTableCatalog();
    Map<String, String> all =
        getPropertiesConverter().toSparkCatalogProperties(options, properties);
    jdbcTableCatalog.initialize(name, new CaseInsensitiveStringMap(all));
    return jdbcTableCatalog;
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
    return new SparkJdbcTypeConverter();
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
}
