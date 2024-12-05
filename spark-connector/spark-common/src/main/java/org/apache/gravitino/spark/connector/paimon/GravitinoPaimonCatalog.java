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

package org.apache.gravitino.spark.connector.paimon;

import java.util.Map;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.catalog.lakehouse.paimon.PaimonPropertiesUtils;
import org.apache.gravitino.spark.connector.PropertiesConverter;
import org.apache.gravitino.spark.connector.SparkTransformConverter;
import org.apache.gravitino.spark.connector.SparkTypeConverter;
import org.apache.gravitino.spark.connector.catalog.BaseCatalog;
import org.apache.paimon.spark.SparkCatalog;
import org.apache.paimon.spark.SparkTable;
import org.apache.spark.sql.catalyst.analysis.NoSuchFunctionException;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.connector.catalog.FunctionCatalog;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.catalog.functions.UnboundFunction;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

public class GravitinoPaimonCatalog extends BaseCatalog implements FunctionCatalog {

  @Override
  protected TableCatalog createAndInitSparkCatalog(
      String name, CaseInsensitiveStringMap options, Map<String, String> properties) {
    String catalogBackendName = PaimonPropertiesUtils.getCatalogBackendName(properties);
    TableCatalog paimonCatalog = new SparkCatalog();
    Map<String, String> all =
        getPropertiesConverter().toSparkCatalogProperties(options, properties);
    paimonCatalog.initialize(catalogBackendName, new CaseInsensitiveStringMap(all));
    return paimonCatalog;
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
    return new SparkPaimonTable(
        identifier,
        gravitinoTable,
        (SparkTable) sparkTable,
        propertiesConverter,
        sparkTransformConverter,
        sparkTypeConverter);
  }

  @Override
  protected PropertiesConverter getPropertiesConverter() {
    return PaimonPropertiesConverter.getInstance();
  }

  @Override
  protected SparkTransformConverter getSparkTransformConverter() {
    return new SparkTransformConverter(true);
  }

  @Override
  public boolean dropTable(Identifier ident) {
    sparkCatalog.invalidateTable(ident);
    return gravitinoCatalogClient
        .asTableCatalog()
        .purgeTable(NameIdentifier.of(getDatabase(ident), ident.name()));
  }

  @Override
  public Identifier[] listFunctions(String[] namespace) throws NoSuchNamespaceException {
    return ((SparkCatalog) sparkCatalog).listFunctions(namespace);
  }

  @Override
  public UnboundFunction loadFunction(Identifier ident) throws NoSuchFunctionException {
    return ((SparkCatalog) sparkCatalog).loadFunction(ident);
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
}
