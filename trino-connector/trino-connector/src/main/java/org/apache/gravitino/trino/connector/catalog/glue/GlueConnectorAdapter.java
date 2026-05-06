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
package org.apache.gravitino.trino.connector.catalog.glue;

import io.trino.spi.session.PropertyMetadata;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.trino.connector.catalog.CatalogConnectorAdapter;
import org.apache.gravitino.trino.connector.catalog.CatalogConnectorMetadataAdapter;
import org.apache.gravitino.trino.connector.catalog.hive.HiveDataTypeTransformer;
import org.apache.gravitino.trino.connector.metadata.GravitinoCatalog;

/**
 * Transforming Apache Gravitino Glue catalog configuration and components into Apache Gravitino
 * connector.
 */
public class GlueConnectorAdapter implements CatalogConnectorAdapter {

  private static final String CONNECTOR_LAKEHOUSE = "lakehouse";

  @Override
  public Map<String, String> buildInternalConnectorConfig(GravitinoCatalog catalog)
      throws Exception {
    Map<String, String> config = new HashMap<>();
    config.put("connector.name", CONNECTOR_LAKEHOUSE);
    config.put("hive.metastore", "glue");
    config.put("hive.metastore.glue.region", catalog.getRequiredProperty("aws-region"));
    config.put("hive.security", "allow-all");
    config.put("fs.hadoop.enabled", "true");

    String catalogId = catalog.getProperty("aws-glue-catalog-id", null);
    if (catalogId != null) {
      config.put("hive.metastore.glue.catalog-id", catalogId);
    }

    String accessKey = catalog.getProperty("aws-access-key-id", null);
    String secretKey = catalog.getProperty("aws-secret-access-key", null);
    if (accessKey != null && secretKey != null) {
      config.put("hive.metastore.glue.aws-access-key", accessKey);
      config.put("hive.metastore.glue.aws-secret-key", secretKey);
    }

    String endpoint = catalog.getProperty("aws-glue-endpoint", null);
    if (endpoint != null) {
      config.put("hive.metastore.glue.endpoint-url", endpoint);
    }

    return config;
  }

  @Override
  public String internalConnectorName() {
    return CONNECTOR_LAKEHOUSE;
  }

  @Override
  public CatalogConnectorMetadataAdapter getMetadataAdapter() {
    return new CatalogConnectorMetadataAdapter(
        Collections.emptyList(),
        Collections.emptyList(),
        Collections.emptyList(),
        new HiveDataTypeTransformer());
  }

  @Override
  public List<PropertyMetadata<?>> getTableProperties() {
    return Collections.emptyList();
  }

  @Override
  public List<PropertyMetadata<?>> getSchemaProperties() {
    return Collections.emptyList();
  }

  @Override
  public List<PropertyMetadata<?>> getColumnProperties() {
    return Collections.emptyList();
  }
}
