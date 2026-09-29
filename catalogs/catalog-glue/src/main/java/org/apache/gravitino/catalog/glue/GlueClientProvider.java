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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.net.URI;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.GlueClientBuilder;

/**
 * Factory for creating AWS {@link GlueClient} instances from Gravitino catalog configuration.
 *
 * <p>Authentication priority:
 *
 * <ol>
 *   <li>Static credentials ({@code aws-access-key-id} + {@code aws-secret-access-key})
 *   <li>Default credential chain (environment variables, instance profile, container credentials)
 * </ol>
 *
 * <p>An optional endpoint override ({@code aws-glue-endpoint}) enables connectivity to VPC
 * endpoints and LocalStack for integration testing.
 */
public final class GlueClientProvider {

  private GlueClientProvider() {}

  /**
   * Builds a {@link GlueClient} from the given catalog configuration map.
   *
   * @param config Catalog configuration properties.
   * @return A configured and ready-to-use {@link GlueClient}.
   * @throws IllegalArgumentException if {@code aws-region} is missing or blank, if only one of the
   *     credential keys is provided, if {@code aws-glue-endpoint} is not a valid URI, or if no
   *     usable AWS credential source can be resolved.
   */
  public static GlueClient buildClient(Map<String, String> config) {
    String region = config.get(GlueConstants.AWS_REGION);
    Preconditions.checkArgument(
        StringUtils.isNotBlank(region),
        "Property '%s' is required to create a Glue client",
        GlueConstants.AWS_REGION);

    GlueClientBuilder builder = GlueClient.builder().region(Region.of(region));

    // Static credentials take priority over the default credential chain.
    // Both keys must be provided together — a partial pair is always a misconfiguration.
    // Default credential chain order (when both keys are omitted):
    //   1. Java system properties (aws.accessKeyId / aws.secretAccessKey)
    //   2. Environment variables (AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY)
    //   3. Web identity token (EKS / Kubernetes)
    //   4. ~/.aws/credentials profile file
    //   5. ECS container task role
    //   6. EC2 instance profile (IMDSv2)
    String accessKey = config.get(GlueConstants.AWS_ACCESS_KEY_ID);
    String secretKey = config.get(GlueConstants.AWS_SECRET_ACCESS_KEY);
    boolean hasStaticCredentials = hasAwsStaticCredentials(accessKey, secretKey);

    AwsCredentialsProvider credentialsProvider =
        hasStaticCredentials
            ? StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey))
            : DefaultCredentialsProvider.builder().build();
    validateCredentials(credentialsProvider);
    builder.credentialsProvider(credentialsProvider);

    // Optional custom endpoint override for VPC endpoints or LocalStack testing.
    String endpoint = config.get(GlueConstants.AWS_GLUE_ENDPOINT);
    if (StringUtils.isNotBlank(endpoint)) {
      builder.endpointOverride(URI.create(endpoint));
    }

    return builder.build();
  }

  /**
   * Eagerly resolves {@code credentialsProvider} to confirm a usable credential source exists,
   * instead of leaving resolution to the first real Glue API call. Without this check, a catalog
   * created with no static credentials and no usable default-chain source (env vars, instance
   * profile, etc.) is stored successfully and then fails on every operation with a raw AWS SDK
   * error that never mentions this connector's own credential properties.
   *
   * @throws IllegalArgumentException if no credentials can be resolved
   */
  @VisibleForTesting
  static void validateCredentials(AwsCredentialsProvider credentialsProvider) {
    try {
      credentialsProvider.resolveCredentials();
    } catch (SdkClientException e) {
      if (!GlueExceptionConverter.isCredentialFailure(e)) {
        throw new IllegalArgumentException(
            "Failed to resolve AWS credentials for the Glue catalog: " + e.getMessage(), e);
      }
      throw new IllegalArgumentException(
          String.format(
              "No usable AWS credentials found for the Glue catalog. Set both '%s' and '%s' "
                  + "catalog properties for static authentication, or ensure the default AWS "
                  + "credential chain (environment variables, instance profile, web identity "
                  + "token, etc.) can resolve credentials.",
              GlueConstants.AWS_ACCESS_KEY_ID, GlueConstants.AWS_SECRET_ACCESS_KEY),
          e);
    }
  }

  static boolean hasAwsStaticCredentials(String accessKey, String secretKey) {
    boolean hasAccessKey = StringUtils.isNotBlank(accessKey);
    boolean hasSecretKey = StringUtils.isNotBlank(secretKey);
    Preconditions.checkArgument(
        hasAccessKey == hasSecretKey,
        "Both '%s' and '%s' must be set together. "
            + "Either provide both keys for static authentication, "
            + "or omit both to use the default credential chain.",
        GlueConstants.AWS_ACCESS_KEY_ID,
        GlueConstants.AWS_SECRET_ACCESS_KEY);

    return hasAccessKey;
  }
}
