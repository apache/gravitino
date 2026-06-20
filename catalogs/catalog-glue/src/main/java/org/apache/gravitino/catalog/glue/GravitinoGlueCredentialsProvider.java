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

import java.util.Map;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

/**
 * AWS credentials provider for Iceberg {@code GlueCatalog} that reads static credentials from a
 * properties map.
 *
 * <p>Iceberg 1.10+ no longer supports {@code client.access-key-id} directly; credentials must be
 * supplied via {@code client.credentials-provider}. This class is configured in {@link
 * GlueIcebergTableHelper#createGlueCatalog} when explicit credentials are provided.
 *
 * <p>Adapted from Apache Doris's {@code CustomAwsCredentialsProvider} ({@code
 * org.apache.doris.datasource.iceberg.s3tables.CustomAwsCredentialsProvider}).
 */
class GravitinoGlueCredentialsProvider implements AwsCredentialsProvider {

  private static final String ACCESS_KEY_ID = "access-key-id";
  private static final String SECRET_ACCESS_KEY = "secret-access-key";

  private final String accessKeyId;
  private final String secretAccessKey;

  /**
   * Creates a credentials provider from the given properties map.
   *
   * @param properties map containing {@code access-key-id} and {@code secret-access-key}
   * @return a new {@link AwsCredentialsProvider} instance
   */
  public static AwsCredentialsProvider create(Map<String, String> properties) {
    return new GravitinoGlueCredentialsProvider(
        properties.get(ACCESS_KEY_ID), properties.get(SECRET_ACCESS_KEY));
  }

  GravitinoGlueCredentialsProvider(String accessKeyId, String secretAccessKey) {
    this.accessKeyId = accessKeyId;
    this.secretAccessKey = secretAccessKey;
  }

  @Override
  public AwsCredentials resolveCredentials() {
    if (accessKeyId == null || accessKeyId.isEmpty()) {
      throw new IllegalStateException("Access key ID is not set");
    }
    if (secretAccessKey == null || secretAccessKey.isEmpty()) {
      throw new IllegalStateException("Secret access key is not set");
    }
    return AwsBasicCredentials.create(accessKeyId, secretAccessKey);
  }
}
