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
package org.apache.gravitino.kms.gcp;

import com.google.cloud.kms.v1.KeyManagementServiceClient;
import java.io.IOException;
import java.util.Map;
import org.apache.gravitino.annotation.DeveloperApi;
import org.apache.gravitino.encryption.kms.KmsApi;
import org.apache.gravitino.encryption.kms.KmsClient;
import org.apache.gravitino.encryption.kms.KmsClientFactory;
import org.apache.gravitino.encryption.kms.KmsConfigurationException;
import org.apache.gravitino.exceptions.ConnectionFailedException;

/**
 * Creates metadata-only Google Cloud KMS clients using Application Default Credentials.
 *
 * <p>Google Cloud project, location, key ring, and key are supplied in each full CryptoKey resource
 * name. Credentials are resolved by the Google Cloud client library and are not accepted as source
 * properties.
 */
@DeveloperApi
public final class GoogleCloudKmsClientFactory implements KmsClientFactory {

  /** Google Cloud project property. */
  public static final String PROJECT_ID = "endpoint.projectId";

  /** Google Cloud location property. */
  public static final String LOCATION = "endpoint.location";

  /** Credential method property. */
  public static final String CREDENTIAL_METHOD = "credential.method";

  private final MetadataServiceFactory metadataServiceFactory;

  /** Creates a factory that uses the default Google Cloud KMS endpoint and credentials. */
  public GoogleCloudKmsClientFactory() {
    this(GoogleCloudKmsClientFactory::createDefaultMetadataService);
  }

  GoogleCloudKmsClientFactory(MetadataServiceFactory metadataServiceFactory) {
    if (metadataServiceFactory == null) {
      throw new IllegalArgumentException(
          "Google Cloud KMS metadata service factory cannot be null");
    }
    this.metadataServiceFactory = metadataServiceFactory;
  }

  /** {@inheritDoc} */
  @Override
  public KmsApi api() {
    return KmsApi.GOOGLE_CLOUD_KMS;
  }

  /** {@inheritDoc} */
  @Override
  public KmsClient create(String source, Map<String, String> properties) {
    if (source == null || source.trim().isEmpty()) {
      throw new KmsConfigurationException("Google Cloud KMS source cannot be blank");
    }
    if (properties == null) {
      throw new KmsConfigurationException("Google Cloud KMS properties cannot be null");
    }
    for (String property : properties.keySet()) {
      if (!PROJECT_ID.equals(property)
          && !LOCATION.equals(property)
          && !CREDENTIAL_METHOD.equals(property)) {
        throw new KmsConfigurationException("Unsupported Google Cloud KMS property: %s", property);
      }
    }

    String projectId = requiredProperty(properties, PROJECT_ID);
    String location = requiredProperty(properties, LOCATION);
    String credentialMethod = requiredProperty(properties, CREDENTIAL_METHOD);
    if (!"default".equals(credentialMethod)) {
      throw new KmsConfigurationException(
          "Unsupported Google Cloud KMS credential method: %s", credentialMethod);
    }

    GoogleCloudKmsMetadataService metadataService = metadataServiceFactory.create(source);
    if (metadataService == null) {
      throw new IllegalStateException("Google Cloud KMS metadata service factory returned null");
    }
    return new GoogleCloudKmsClient(source, projectId, location, metadataService);
  }

  private static String requiredProperty(Map<String, String> properties, String name) {
    String value = properties.get(name);
    if (value == null || value.trim().isEmpty()) {
      throw new KmsConfigurationException("Google Cloud KMS property %s cannot be blank", name);
    }
    return value.trim();
  }

  private static GoogleCloudKmsMetadataService createDefaultMetadataService(String source) {
    try {
      return new GoogleCloudKmsSdkMetadataService(source, KeyManagementServiceClient.create());
    } catch (IOException | RuntimeException e) {
      throw new ConnectionFailedException(
          e, "Google Cloud KMS client initialization failed for source %s", source);
    }
  }

  @FunctionalInterface
  interface MetadataServiceFactory {
    GoogleCloudKmsMetadataService create(String source);
  }
}
