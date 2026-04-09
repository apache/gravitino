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

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import java.net.URI;
import java.util.Map;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
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
   *     credential keys is provided, or if {@code aws-glue-endpoint} is not a valid URI.
   */
  public static GlueClient buildClient(Map<String, String> config) {
    String region = config.get(GlueConstants.AWS_REGION);
    Preconditions.checkArgument(
        !Strings.isNullOrEmpty(region) && !region.isBlank(),
        "Property '%s' is required to create a Glue client",
        GlueConstants.AWS_REGION);

    GlueClientBuilder builder = GlueClient.builder().region(Region.of(region));

    // Static credentials take priority over the default credential chain.
    // Both keys must be provided together — a partial pair is always a misconfiguration.
    String accessKey = config.get(GlueConstants.AWS_ACCESS_KEY_ID);
    String secretKey = config.get(GlueConstants.AWS_SECRET_ACCESS_KEY);
    boolean hasAccessKey = !Strings.isNullOrEmpty(accessKey) && !accessKey.isBlank();
    boolean hasSecretKey = !Strings.isNullOrEmpty(secretKey) && !secretKey.isBlank();
    Preconditions.checkArgument(
        hasAccessKey == hasSecretKey,
        "Incomplete static credentials: '%s' requires '%s'. "
            + "Either provide both keys for static authentication, "
            + "or omit both to use the default credential chain.",
        hasAccessKey ? GlueConstants.AWS_ACCESS_KEY_ID : GlueConstants.AWS_SECRET_ACCESS_KEY,
        hasAccessKey ? GlueConstants.AWS_SECRET_ACCESS_KEY : GlueConstants.AWS_ACCESS_KEY_ID);

    if (hasAccessKey) {
      builder.credentialsProvider(
          StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey)));
    } else {
      builder.credentialsProvider(DefaultCredentialsProvider.create());
    }

    // Optional custom endpoint override for VPC endpoints or LocalStack testing.
    String endpoint = config.get(GlueConstants.AWS_GLUE_ENDPOINT);
    if (!Strings.isNullOrEmpty(endpoint) && !endpoint.isBlank()) {
      try {
        builder.endpointOverride(URI.create(endpoint));
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException(
            String.format(
                "Property '%s' contains an invalid URI: '%s'. "
                    + "Expected a valid URL, e.g. 'http://localhost:4566'. Cause: %s",
                GlueConstants.AWS_GLUE_ENDPOINT, endpoint, e.getMessage()),
            e);
      }
    }

    return builder.build();
  }
}
