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
import com.google.common.collect.ImmutableMap;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.catalog.glue.GlueConstants;
import org.apache.gravitino.spark.connector.PropertiesConverter;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.aws.glue.GlueCatalog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

  private static final Logger LOG = LoggerFactory.getLogger(GluePropertiesConverter.class);

  /** Iceberg GlueCatalog implementation class name, set as {@code catalog-impl}. */
  public static final String GLUE_CATALOG_IMPL = GlueCatalog.class.getName();

  /** Iceberg property key for the AWS Glue catalog ID (account-level catalog). */
  public static final String GLUE_ID = "glue.id";

  /** Iceberg property key for a custom AWS Glue service endpoint URL. */
  public static final String GLUE_ENDPOINT = "glue.endpoint";

  /** Iceberg property key for the AWS region used by the Glue client. */
  public static final String CLIENT_REGION = "client.region";

  /**
   * Iceberg property key for the credentials provider class. Set to {@link
   * #GRAVITINO_GLUE_CREDENTIALS_PROVIDER} when static credentials are configured.
   */
  public static final String CLIENT_CREDENTIALS_PROVIDER = "client.credentials-provider";

  /**
   * Fully-qualified class name of {@link GravitinoGlueCredentialsProvider}. Implements {@code
   * AwsCredentialsProvider} with a {@code create(Map)} factory so that Iceberg's {@code
   * AwsClientProperties} can instantiate it dynamically via {@code client.credentials-provider}.
   */
  public static final String GRAVITINO_GLUE_CREDENTIALS_PROVIDER =
      "org.apache.gravitino.spark.connector.glue.GravitinoGlueCredentialsProvider";

  /** Gravitino catalog property key for the AWS access key ID. */
  public static final String AWS_ACCESS_KEY_ID = "aws-access-key-id";

  /** Gravitino catalog property key for the AWS secret access key. */
  public static final String AWS_SECRET_ACCESS_KEY = "aws-secret-access-key";

  /** Gravitino catalog property key for the AWS region. */
  public static final String AWS_REGION = "aws-region";

  /** Gravitino catalog property key for the AWS Glue catalog ID. */
  public static final String AWS_GLUE_CATALOG_ID = "aws-glue-catalog-id";

  /** Gravitino catalog property key for a custom AWS Glue endpoint URL. */
  public static final String AWS_GLUE_ENDPOINT = "aws-glue-endpoint";

  /** Maps Gravitino property keys to Spark HiveTableCatalog property keys. */
  private static final Map<String, String> GRAVITINO_TO_SPARK_KEYS =
      ImmutableMap.of(
          AWS_REGION, "aws.region",
          AWS_GLUE_CATALOG_ID, "aws.glue.catalog.id",
          AWS_GLUE_ENDPOINT, "aws.glue.endpoint");

  /** Maps Gravitino property keys to Iceberg GlueCatalog property keys. */
  private static final Map<String, String> GRAVITINO_TO_ICEBERG_KEYS =
      ImmutableMap.of(
          AWS_REGION, CLIENT_REGION,
          AWS_GLUE_CATALOG_ID, GLUE_ID,
          AWS_GLUE_ENDPOINT, GLUE_ENDPOINT);

  private static class GluePropertiesConverterHolder {
    private static final GluePropertiesConverter INSTANCE = new GluePropertiesConverter();
  }

  private GluePropertiesConverter() {}

  /**
   * Returns the singleton instance of {@link GluePropertiesConverter}.
   *
   * @return the singleton instance
   */
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
    GRAVITINO_TO_SPARK_KEYS.forEach(
        (gravitinoKey, sparkKey) -> {
          String value = properties.get(gravitinoKey);
          if (StringUtils.isNotBlank(value)) {
            all.put(sparkKey, value);
          }
        });
    return all;
  }

  /**
   * Transform Gravitino Glue catalog properties to Iceberg SparkCatalog properties for Iceberg
   * tables stored in Glue.
   *
   * <p>This maps Gravitino's AWS Glue properties to Iceberg's GlueCatalog configuration:
   *
   * <ul>
   *   <li>{@code aws-region} → {@code client.region} (optional; falls back to SDK default chain)
   *   <li>{@code aws-glue-catalog-id} → {@code glue.id}
   *   <li>{@code aws-glue-endpoint} → {@code glue.endpoint} (optional)
   *   <li>{@code aws-access-key-id} + {@code aws-secret-access-key} → {@code
   *       client.credentials-provider=GravitinoGlueCredentialsProvider} with credentials passed via
   *       {@code client.credentials-provider.*} properties
   * </ul>
   */
  @VisibleForTesting
  Map<String, String> toIcebergCatalogProperties(Map<String, String> properties) {
    Preconditions.checkArgument(properties != null, "Glue Catalog properties should not be null");
    HashMap<String, String> all = new HashMap<>();
    all.put(CatalogProperties.CATALOG_IMPL, GLUE_CATALOG_IMPL);
    GRAVITINO_TO_ICEBERG_KEYS.forEach(
        (gravitinoKey, icebergKey) -> {
          String value = properties.get(gravitinoKey);
          if (StringUtils.isNotBlank(value)) {
            all.put(icebergKey, value);
          }
        });
    String accessKey = properties.get(AWS_ACCESS_KEY_ID);
    String secretKey = properties.get(AWS_SECRET_ACCESS_KEY);
    if (StringUtils.isNotBlank(accessKey) && StringUtils.isNotBlank(secretKey)) {
      // Iceberg 1.10+ reads credentials via client.credentials-provider (a class with create(Map)).
      // The client.credentials-provider.* properties are stripped of their prefix and passed to
      // GravitinoGlueCredentialsProvider.create(Map) as {"access-key-id": ..., "secret-access-key":
      // ...}.
      all.put(CLIENT_CREDENTIALS_PROVIDER, GRAVITINO_GLUE_CREDENTIALS_PROVIDER);
      all.put("client.credentials-provider.access-key-id", accessKey);
      all.put("client.credentials-provider.secret-access-key", secretKey);
    } else {
      LOG.debug(
          "aws-access-key-id or aws-secret-access-key not configured; "
              + "falling back to the default AWS credential chain (instance profile, env vars, etc.).");
    }
    return all;
  }

  @Override
  public Map<String, String> toGravitinoTableProperties(Map<String, String> properties) {
    HashMap<String, String> all = new HashMap<>(properties);
    String provider = all.remove("provider");
    if ("iceberg".equalsIgnoreCase(provider)) {
      all.put(GlueConstants.TABLE_FORMAT, GlueConstants.TABLE_FORMAT_ICEBERG);
    }
    return all;
  }

  @Override
  public Map<String, String> toSparkTableProperties(Map<String, String> properties) {
    return new HashMap<>(properties);
  }
}
