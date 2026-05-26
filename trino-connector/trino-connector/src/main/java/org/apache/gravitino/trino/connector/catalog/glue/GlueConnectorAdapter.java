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
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.trino.connector.catalog.CatalogConnectorAdapter;
import org.apache.gravitino.trino.connector.catalog.CatalogConnectorMetadataAdapter;
import org.apache.gravitino.trino.connector.catalog.HasPropertyMeta;
import org.apache.gravitino.trino.connector.catalog.hive.HivePropertyMeta;
import org.apache.gravitino.trino.connector.metadata.GravitinoCatalog;

/**
 * Transforming Apache Gravitino Glue catalog configuration and components into Apache Gravitino
 * connector.
 */
public class GlueConnectorAdapter implements CatalogConnectorAdapter {

  private static final String CONNECTOR_LAKEHOUSE = "lakehouse";

  // Gravitino catalog property keys for AWS Glue
  private static final String PROP_AWS_REGION = "aws-region";
  private static final String PROP_AWS_GLUE_CATALOG_ID = "aws-glue-catalog-id";
  private static final String PROP_AWS_ACCESS_KEY_ID = "aws-access-key-id";
  private static final String PROP_AWS_SECRET_ACCESS_KEY = "aws-secret-access-key";
  private static final String PROP_AWS_GLUE_ENDPOINT = "aws-glue-endpoint";

  // Trino Hive connector configuration keys for Glue
  private static final String HIVE_METASTORE = "hive.metastore";
  private static final String HIVE_METASTORE_GLUE_REGION = "hive.metastore.glue.region";
  private static final String HIVE_METASTORE_GLUE_CATALOG_ID = "hive.metastore.glue.catalogid";
  private static final String HIVE_METASTORE_GLUE_ACCESS_KEY = "hive.metastore.glue.aws-access-key";
  private static final String HIVE_METASTORE_GLUE_SECRET_KEY = "hive.metastore.glue.aws-secret-key";
  private static final String HIVE_METASTORE_GLUE_ENDPOINT = "hive.metastore.glue.endpoint-url";
  private static final String HIVE_S3_ACCESS_KEY = "hive.s3.aws-access-key";
  private static final String HIVE_S3_SECRET_KEY = "hive.s3.aws-secret-key";

  private final HasPropertyMeta propertyMetadata;

  /** Constructs a new GlueConnectorAdapter. */
  public GlueConnectorAdapter() {
    this.propertyMetadata = new HivePropertyMeta();
  }

  @Override
  public Map<String, String> buildInternalConnectorConfig(GravitinoCatalog catalog)
      throws Exception {
    Map<String, String> config = new HashMap<>();

    // Glue-specific metastore configuration.
    config.put(HIVE_METASTORE, "glue");
    config.put(HIVE_METASTORE_GLUE_REGION, catalog.getRequiredProperty(PROP_AWS_REGION));

    // Passed through to the underlying Trino Hive/Iceberg connector.
    config.put("hive.security", "allow-all");
    config.put("fs.hadoop.enabled", "true");
    config.put("hive.non-managed-table-writes-enabled", "true");

    String catalogId = catalog.getProperty(PROP_AWS_GLUE_CATALOG_ID, null);
    if (catalogId != null) {
      config.put(HIVE_METASTORE_GLUE_CATALOG_ID, catalogId);
    }

    String accessKey = catalog.getProperty(PROP_AWS_ACCESS_KEY_ID, null);
    String secretKey = catalog.getProperty(PROP_AWS_SECRET_ACCESS_KEY, null);
    if (accessKey != null && secretKey != null) {
      // Glue metastore credentials
      config.put(HIVE_METASTORE_GLUE_ACCESS_KEY, accessKey);
      config.put(HIVE_METASTORE_GLUE_SECRET_KEY, secretKey);
      // S3 credentials for data access
      config.put(HIVE_S3_ACCESS_KEY, accessKey);
      config.put(HIVE_S3_SECRET_KEY, secretKey);
    }

    String endpoint = catalog.getProperty(PROP_AWS_GLUE_ENDPOINT, null);
    if (StringUtils.isNotBlank(endpoint)) {
      config.put(HIVE_METASTORE_GLUE_ENDPOINT, endpoint);
    }

    return config;
  }

  @Override
  public String internalConnectorName() {
    return CONNECTOR_LAKEHOUSE;
  }

  @Override
  public CatalogConnectorMetadataAdapter getMetadataAdapter() {
    return new GlueMetadataAdapter(propertyMetadata.getSchemaPropertyMetadata());
  }

  @Override
  public List<PropertyMetadata<?>> getTableProperties() {
    return GlueMetadataAdapter.getTablePropertyMetadata();
  }

  @Override
  public List<PropertyMetadata<?>> getSchemaProperties() {
    return propertyMetadata.getSchemaPropertyMetadata();
  }

  @Override
  public List<PropertyMetadata<?>> getColumnProperties() {
    return Collections.emptyList();
  }
}
