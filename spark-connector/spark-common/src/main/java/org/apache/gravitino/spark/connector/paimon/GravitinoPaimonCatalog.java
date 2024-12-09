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
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

public class GravitinoPaimonCatalog extends BaseCatalog {

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
}
