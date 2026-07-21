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
package org.apache.gravitino.kms.aws;

import com.google.common.collect.ImmutableSet;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.Set;
import org.apache.gravitino.encryption.kms.KmsApi;
import org.apache.gravitino.encryption.kms.KmsClient;
import org.apache.gravitino.encryption.kms.KmsClientFactory;
import org.apache.gravitino.encryption.kms.KmsConfigurationException;
import org.apache.gravitino.exceptions.ConnectionFailedException;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kms.KmsClientBuilder;

/** Creates AWS KMS clients using the AWS SDK default credential provider chain. */
public final class AwsKmsClientFactory implements KmsClientFactory {

  /** Required AWS region property. */
  public static final String REGION = "endpoint.region";

  /** Optional HTTP(S) AWS KMS endpoint override property. */
  public static final String SERVICE_ADDRESS = "endpoint.serviceAddress";

  /** Credential method property. */
  public static final String CREDENTIAL_METHOD = "credential.method";

  private static final Set<String> SUPPORTED_PROPERTIES =
      ImmutableSet.of(REGION, SERVICE_ADDRESS, CREDENTIAL_METHOD);

  /** Creates an AWS KMS client factory. */
  public AwsKmsClientFactory() {}

  /**
   * Returns the AWS KMS API.
   *
   * @return the AWS KMS API
   */
  @Override
  public KmsApi api() {
    return KmsApi.AWS_KMS;
  }

  /**
   * Creates an AWS KMS client for a configured source.
   *
   * <p>Credentials are intentionally not accepted as source properties. The AWS SDK resolves
   * credentials through its default credential provider chain when a request is made.
   *
   * @param source logical name of the configured AWS KMS instance
   * @param properties AWS KMS properties
   * @return the configured AWS KMS client
   * @throws IllegalArgumentException if the source or properties are invalid
   * @throws ConnectionFailedException if the AWS SDK client cannot be initialized
   */
  @Override
  public KmsClient create(String source, Map<String, String> properties) {
    validateSource(source);
    if (properties == null) {
      throw new KmsConfigurationException("AWS KMS properties cannot be null");
    }
    validatePropertyNames(properties);

    String region = requiredProperty(properties, REGION);
    String credentialMethod = requiredProperty(properties, CREDENTIAL_METHOD);
    if (!"default".equals(credentialMethod)) {
      throw new KmsConfigurationException(
          "Unsupported AWS KMS credential method: %s", credentialMethod);
    }
    KmsClientBuilder builder =
        software.amazon.awssdk.services.kms.KmsClient.builder().region(Region.of(region));
    if (properties.containsKey(SERVICE_ADDRESS)) {
      builder.endpointOverride(parseServiceAddress(requiredProperty(properties, SERVICE_ADDRESS)));
    }

    try {
      return new AwsKmsClient(source, builder.build());
    } catch (SdkClientException e) {
      throw new ConnectionFailedException(
          e, "Failed to initialize AWS KMS client for source '%s'", source);
    }
  }

  private static void validateSource(String source) {
    if (source == null || source.trim().isEmpty()) {
      throw new KmsConfigurationException("AWS KMS source cannot be blank");
    }
  }

  private static void validatePropertyNames(Map<String, String> properties) {
    for (String property : properties.keySet()) {
      if (!SUPPORTED_PROPERTIES.contains(property)) {
        throw new KmsConfigurationException("Unsupported AWS KMS property '%s'", property);
      }
    }
  }

  private static String requiredProperty(Map<String, String> properties, String property) {
    String value = properties.get(property);
    if (value == null || value.trim().isEmpty()) {
      throw new KmsConfigurationException("AWS KMS property '%s' cannot be blank", property);
    }
    return value.trim();
  }

  private static URI parseServiceAddress(String serviceAddress) {
    URI uri;
    try {
      uri = new URI(serviceAddress);
    } catch (URISyntaxException e) {
      throw new KmsConfigurationException(
          e, "Invalid AWS KMS service address '%s'", serviceAddress);
    }

    boolean http = "http".equalsIgnoreCase(uri.getScheme());
    boolean https = "https".equalsIgnoreCase(uri.getScheme());
    boolean rootPath =
        uri.getPath() == null || uri.getPath().isEmpty() || "/".equals(uri.getPath());
    if ((!http && !https)
        || uri.getHost() == null
        || uri.getUserInfo() != null
        || !rootPath
        || uri.getQuery() != null
        || uri.getFragment() != null) {
      throw new KmsConfigurationException(
          "Invalid AWS KMS service address '%s'; expected an HTTP(S) origin", serviceAddress);
    }
    return uri;
  }
}
