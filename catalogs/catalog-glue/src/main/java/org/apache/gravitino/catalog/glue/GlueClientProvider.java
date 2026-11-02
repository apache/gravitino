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
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.exceptions.ConnectionFailedException;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.GlueClientBuilder;
import software.amazon.awssdk.services.glue.GlueServiceClientConfiguration;

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

  private static final String TEMPORARY_ACCESS_KEY_PREFIX = "ASIA";

  private GlueClientProvider() {}

  /**
   * Builds a {@link GlueClient} from the given catalog configuration map.
   *
   * @param config Catalog configuration properties.
   * @return A configured and ready-to-use {@link GlueClient}.
   * @throws IllegalArgumentException if the configuration fails {@link #validateConfig(Map)}
   * @throws ConnectionFailedException if the configured credential provider cannot resolve
   *     credentials
   */
  public static GlueClient buildClient(Map<String, String> config) {
    validateConfig(config);
    String region = config.get(GlueConstants.AWS_REGION);
    GlueClientBuilder builder = GlueClient.builder().region(Region.of(region));

    // Static credentials take priority over the default credential chain.
    // Default credential chain order (when both keys are omitted):
    //   1. Java system properties (aws.accessKeyId / aws.secretAccessKey)
    //   2. Environment variables (AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY)
    //   3. Web identity token (EKS / Kubernetes)
    //   4. ~/.aws/credentials profile file
    //   5. ECS container task role
    //   6. EC2 instance profile (IMDSv2)
    String accessKey = config.get(GlueConstants.AWS_ACCESS_KEY_ID);
    String secretKey = config.get(GlueConstants.AWS_SECRET_ACCESS_KEY);
    AwsCredentialsProvider credentialsProvider =
        hasAwsStaticCredentials(accessKey, secretKey)
            ? StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey))
            : DefaultCredentialsProvider.builder().build();
    validateCredentials(credentialsProvider);
    builder.credentialsProvider(credentialsProvider);

    // Bound every call so an endpoint that does not answer cannot hold a request thread for the
    // SDK default (connect timeout x resolved addresses x attempts).
    int maxAttempts =
        intProperty(
            config,
            GlueConstants.AWS_GLUE_MAX_ATTEMPTS,
            GlueConstants.DEFAULT_AWS_GLUE_MAX_ATTEMPTS);
    builder.overrideConfiguration(
        ClientOverrideConfiguration.builder()
            .apiCallTimeout(Duration.ofMillis(apiCallTimeoutMs(config)))
            .apiCallAttemptTimeout(Duration.ofMillis(apiCallAttemptTimeoutMs(config)))
            .retryStrategy(b -> b.maxAttempts(maxAttempts))
            .build());

    // Optional custom endpoint override for VPC endpoints or LocalStack testing.
    String endpoint = config.get(GlueConstants.AWS_GLUE_ENDPOINT);
    if (StringUtils.isNotBlank(endpoint)) {
      builder.endpointOverride(GlueCatalogPropertiesMetadata.parseEndpoint(endpoint));
    }

    return builder.build();
  }

  /**
   * Runs the local configuration checks that need no AWS call: {@code aws-region} is set, the
   * static key pair is complete and is a long-term key, and {@code aws-glue-endpoint} is a valid
   * URL.
   *
   * @param config Catalog configuration properties.
   * @throws IllegalArgumentException if any check fails
   */
  public static void validateConfig(Map<String, String> config) {
    Preconditions.checkArgument(
        StringUtils.isNotBlank(config.get(GlueConstants.AWS_REGION)),
        "Property '%s' is required to create a Glue client",
        GlueConstants.AWS_REGION);

    String accessKey = config.get(GlueConstants.AWS_ACCESS_KEY_ID);
    if (hasAwsStaticCredentials(accessKey, config.get(GlueConstants.AWS_SECRET_ACCESS_KEY))) {
      // Temporary keys (ASIA...) are only valid together with a session token, which this
      // connector cannot be given, so AWS would reject every request signed with them.
      Preconditions.checkArgument(
          !accessKey.trim().startsWith(TEMPORARY_ACCESS_KEY_PREFIX),
          "Property '%s' holds a temporary AWS access key (prefix %s). Temporary credentials need"
              + " a session token and are not supported. Use a long-term key pair, or omit both"
              + " '%s' and '%s' to use the default AWS credential chain.",
          GlueConstants.AWS_ACCESS_KEY_ID,
          TEMPORARY_ACCESS_KEY_PREFIX,
          GlueConstants.AWS_ACCESS_KEY_ID,
          GlueConstants.AWS_SECRET_ACCESS_KEY);
    }

    String endpoint = config.get(GlueConstants.AWS_GLUE_ENDPOINT);
    if (StringUtils.isNotBlank(endpoint)) {
      GlueCatalogPropertiesMetadata.parseEndpoint(endpoint);
    }
  }

  /**
   * Returns the configured upper bound in milliseconds for one Glue API call.
   *
   * @param config Catalog configuration properties.
   * @return the total call timeout in milliseconds
   */
  static long apiCallTimeoutMs(Map<String, String> config) {
    return longProperty(
        config,
        GlueConstants.AWS_GLUE_API_CALL_TIMEOUT_MS,
        GlueConstants.DEFAULT_AWS_GLUE_API_CALL_TIMEOUT_MS);
  }

  /**
   * Returns the configured upper bound in milliseconds for one attempt of a Glue API call.
   *
   * @param config Catalog configuration properties.
   * @return the per-attempt timeout in milliseconds
   */
  static long apiCallAttemptTimeoutMs(Map<String, String> config) {
    return longProperty(
        config,
        GlueConstants.AWS_GLUE_API_CALL_ATTEMPT_TIMEOUT_MS,
        GlueConstants.DEFAULT_AWS_GLUE_API_CALL_ATTEMPT_TIMEOUT_MS);
  }

  /**
   * Describes the Glue endpoint a client calls, for error messages: the endpoint override when one
   * is configured, otherwise the region whose default endpoint is used.
   *
   * @param client the Glue client, may be null
   * @return a human-readable endpoint description
   */
  static String describeEndpoint(GlueClient client) {
    GlueServiceClientConfiguration conf =
        client == null ? null : client.serviceClientConfiguration();
    if (conf == null) {
      return "the configured endpoint";
    }
    Optional<URI> override = conf.endpointOverride();
    if (override != null && override.isPresent()) {
      return String.format("'%s' (%s)", override.get(), GlueConstants.AWS_GLUE_ENDPOINT);
    }
    return String.format(
        "the default endpoint of region '%s' (%s)", conf.region(), GlueConstants.AWS_REGION);
  }

  /**
   * Eagerly resolves {@code credentialsProvider} when Glue operations are initialized, instead of
   * leaving resolution to the first real Glue API call. This makes an explicit connection test or
   * the first operation fail with an actionable connection error when no credential source is
   * available. It does not authenticate static credentials; only an AWS API request can do that.
   *
   * @throws ConnectionFailedException if no credentials can be resolved
   */
  @VisibleForTesting
  static void validateCredentials(AwsCredentialsProvider credentialsProvider) {
    try {
      credentialsProvider.resolveCredentials();
    } catch (SdkClientException e) {
      throw GlueExceptionConverter.toConnectionException(e);
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

  private static long longProperty(Map<String, String> config, String key, long defaultValue) {
    String value = config.get(key);
    return StringUtils.isBlank(value) ? defaultValue : Long.parseLong(value.trim());
  }

  private static int intProperty(Map<String, String> config, String key, int defaultValue) {
    String value = config.get(key);
    return StringUtils.isBlank(value) ? defaultValue : Integer.parseInt(value.trim());
  }
}
