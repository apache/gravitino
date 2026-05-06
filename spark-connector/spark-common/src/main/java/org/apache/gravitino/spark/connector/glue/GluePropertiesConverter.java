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

package org.apache.gravitino.spark.connector.glue;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.spark.connector.PropertiesConverter;
import org.apache.iceberg.CatalogProperties;

/**
 * Transform AWS Glue catalog properties between Apache Spark and Apache Gravitino.
 *
 * <p>This converter handles the property mapping for:
 *
 * <ul>
 *   <li>Non-Iceberg tables: pass through AWS credentials and region to HiveTableCatalog
 *   <li>Iceberg tables: map Gravitino Glue properties to Iceberg's GlueCatalog configuration
 * </ul>
 */
public class GluePropertiesConverter implements PropertiesConverter {

  public static final String GLUE_CATALOG_IMPL = "org.apache.iceberg.aws.glue.GlueCatalog";
  public static final String GLUE_ID = "glue.id";
  public static final String GLUE_ENDPOINT = "glue.endpoint";
  public static final String CLIENT_REGION = "client.region";
  public static final String CLIENT_CREDENTIALS_PROVIDER = "client.credentials-provider";
  public static final String STATIC_CREDENTIALS_PROVIDER =
      "org.apache.iceberg.aws.static.SessionCredentialsProvider";
  public static final String AWS_ACCESS_KEY_ID = "aws-access-key-id";
  public static final String AWS_SECRET_ACCESS_KEY = "aws-secret-access-key";
  public static final String AWS_REGION = "aws-region";
  public static final String AWS_GLUE_CATALOG_ID = "aws-glue-catalog-id";
  public static final String AWS_GLUE_ENDPOINT = "aws-glue-endpoint";

  public static class GluePropertiesConverterHolder {
    private static final GluePropertiesConverter INSTANCE = new GluePropertiesConverter();
  }

  private GluePropertiesConverter() {}

  public static GluePropertiesConverter getInstance() {
    return GluePropertiesConverterHolder.INSTANCE;
  }

  /**
   * Transform Gravitino Glue catalog properties to Spark catalog properties for HiveTableCatalog.
   *
   * <p>For Glue-backed Hive tables, Spark uses the AWS SDK directly through the Hive metastore
   * compatibility layer. The properties are passed through to enable Glue API access.
   */
  @Override
  public Map<String, String> toSparkCatalogProperties(Map<String, String> properties) {
    Preconditions.checkArgument(properties != null, "Glue Catalog properties should not be null");
    HashMap<String, String> all = new HashMap<>();
    String region = properties.get(AWS_REGION);
    if (StringUtils.isNotBlank(region)) {
      all.put(CLIENT_REGION, region);
    }
    String accessKey = properties.get(AWS_ACCESS_KEY_ID);
    String secretKey = properties.get(AWS_SECRET_ACCESS_KEY);
    if (StringUtils.isNotBlank(accessKey) && StringUtils.isNotBlank(secretKey)) {
      all.put(CLIENT_CREDENTIALS_PROVIDER, STATIC_CREDENTIALS_PROVIDER);
      all.put("client.access-key-id", accessKey);
      all.put("client.secret-key", secretKey);
    }
    return all;
  }

  /**
   * Transform Gravitino Glue catalog properties to Iceberg SparkCatalog properties for Iceberg
   * tables stored in Glue.
   *
   * <p>This maps Gravitino's AWS Glue properties to Iceberg's GlueCatalog configuration:
   *
   * <ul>
   *   <li>{@code aws-region} → {@code client.region}
   *   <li>{@code aws-glue-catalog-id} → {@code glue.id}
   *   <li>{@code aws-glue-endpoint} → {@code glue.endpoint} (optional)
   *   <li>{@code aws-access-key-id} + {@code aws-secret-access-key} → {@code
   *       client.credentials-provider=StaticCredentialsProvider}
   * </ul>
   */
  @VisibleForTesting
  Map<String, String> toIcebergCatalogProperties(Map<String, String> properties) {
    Preconditions.checkArgument(properties != null, "Glue Catalog properties should not be null");
    HashMap<String, String> all = new HashMap<>();
    all.put(CatalogProperties.CATALOG_IMPL, GLUE_CATALOG_IMPL);
    String region = properties.get(AWS_REGION);
    Preconditions.checkArgument(
        StringUtils.isNotBlank(region), "AWS region must be specified for Glue Iceberg catalog");
    all.put(CLIENT_REGION, region);
    String catalogId = properties.get(AWS_GLUE_CATALOG_ID);
    if (StringUtils.isNotBlank(catalogId)) {
      all.put(GLUE_ID, catalogId);
    }
    String endpoint = properties.get(AWS_GLUE_ENDPOINT);
    if (StringUtils.isNotBlank(endpoint)) {
      all.put(GLUE_ENDPOINT, endpoint);
    }
    String accessKey = properties.get(AWS_ACCESS_KEY_ID);
    String secretKey = properties.get(AWS_SECRET_ACCESS_KEY);
    if (StringUtils.isNotBlank(accessKey) && StringUtils.isNotBlank(secretKey)) {
      all.put(CLIENT_CREDENTIALS_PROVIDER, STATIC_CREDENTIALS_PROVIDER);
      all.put("client.access-key-id", accessKey);
      all.put("client.secret-key", secretKey);
    }
    return all;
  }

  @Override
  public Map<String, String> toGravitinoTableProperties(Map<String, String> properties) {
    return new HashMap<>(properties);
  }

  @Override
  public Map<String, String> toSparkTableProperties(Map<String, String> properties) {
    return new HashMap<>(properties);
  }
}
