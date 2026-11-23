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
package org.apache.gravitino.catalog.glue;

import java.util.HashMap;
import java.util.Map;
import org.apache.iceberg.aws.AwsClientFactories;
import org.apache.iceberg.aws.AwsClientFactory;
import org.apache.iceberg.aws.AwsProperties;
import org.apache.iceberg.util.PropertyUtil;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.kms.KmsClient;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;

/**
 * AWS client factory of the Iceberg Glue catalog used for Iceberg tables. The Glue client is built
 * the same way as the catalog's own Glue client, so the call timeout, attempt timeout and maximum
 * attempts of the catalog also apply to Iceberg tables. The other clients come from the Iceberg
 * default factory.
 *
 * <p>The catalog properties the Glue client needs are passed under {@link #GLUE_CONFIG_PREFIX}.
 */
public class GlueIcebergAwsClientFactory implements AwsClientFactory {

  /** Prefix of the Iceberg catalog properties that carry the Gravitino Glue catalog properties. */
  static final String GLUE_CONFIG_PREFIX = "gravitino.glue.";

  private static final long serialVersionUID = 1L;

  private HashMap<String, String> glueConfig;

  private AwsClientFactory delegate;

  /** Creates a factory; Iceberg calls {@link #initialize(Map)} before using it. */
  public GlueIcebergAwsClientFactory() {}

  @Override
  public void initialize(Map<String, String> properties) {
    this.glueConfig =
        new HashMap<>(PropertyUtil.propertiesWithPrefix(properties, GLUE_CONFIG_PREFIX));
    Map<String, String> delegateProperties = new HashMap<>(properties);
    delegateProperties.remove(AwsProperties.CLIENT_FACTORY);
    this.delegate = AwsClientFactories.from(delegateProperties);
  }

  @Override
  public GlueClient glue() {
    // The catalog's own Glue client has already checked that credentials can be resolved.
    return GlueClientProvider.buildClient(glueConfig, false);
  }

  @Override
  public S3Client s3() {
    return delegate.s3();
  }

  @Override
  public S3AsyncClient s3Async() {
    return delegate.s3Async();
  }

  @Override
  public KmsClient kms() {
    return delegate.kms();
  }

  @Override
  public DynamoDbClient dynamo() {
    return delegate.dynamo();
  }
}
