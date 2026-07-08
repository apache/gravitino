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
package org.apache.gravitino.iceberg.common.credential;

import com.google.common.base.Preconditions;
import java.io.Closeable;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergConstants;
import org.apache.gravitino.client.DefaultOAuth2TokenProvider;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.credential.AwsIrsaCredential;
import org.apache.gravitino.credential.CatalogCredentialContext;
import org.apache.gravitino.credential.CatalogCredentialManager;
import org.apache.gravitino.credential.Credential;
import org.apache.gravitino.credential.CredentialPropertyUtils;
import org.apache.gravitino.credential.S3SecretKeyCredential;
import org.apache.gravitino.credential.S3TokenCredential;
import org.apache.gravitino.credential.config.CredentialConfig;
import org.apache.gravitino.utils.PrincipalUtils;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.utils.SdkAutoCloseable;

/**
 * AWS SDK v2 credentials provider for Iceberg server-side S3 FileIO.
 *
 * <p>Iceberg loads this class through {@code client.credentials-provider}. The provider refreshes
 * Gravitino-vended S3 credentials before they expire instead of pinning a static session token in
 * the long-lived Iceberg catalog configuration.
 */
public class GravitinoIcebergAwsCredentialsProvider
    implements AwsCredentialsProvider, SdkAutoCloseable {

  /** Property that selects the backing credential source. */
  public static final String SOURCE = "credential-source";

  /** Source that generates credentials in the current JVM with {@link CatalogCredentialManager}. */
  public static final String SOURCE_LOCAL = "local";

  /** Source that refreshes credentials through the Gravitino REST client. */
  public static final String SOURCE_REMOTE = "remote";

  /** Catalog name used for credential generation. */
  public static final String CATALOG_NAME = "catalog-name";

  private static final double REFRESH_TIME_FACTOR = 0.5D;
  private static final String SIMPLE_AUTH_TYPE = "simple";
  private static final String OAUTH2_AUTH_TYPE = "oauth2";

  private final CredentialSupplier credentialSupplier;

  private volatile AwsCredentials cachedCredentials;
  private volatile long refreshTimeInMs;

  private GravitinoIcebergAwsCredentialsProvider(CredentialSupplier credentialSupplier) {
    this.credentialSupplier = credentialSupplier;
  }

  /**
   * Creates a Gravitino Iceberg AWS credentials provider.
   *
   * @param properties Iceberg {@code client.credentials-provider.*} properties with the prefix
   *     stripped
   * @return AWS credentials provider
   */
  public static AwsCredentialsProvider create(Map<String, String> properties) {
    Preconditions.checkArgument(
        properties != null, "Credential provider properties must not be null");
    String source = properties.getOrDefault(SOURCE, SOURCE_LOCAL);
    if (SOURCE_LOCAL.equalsIgnoreCase(source)) {
      return new GravitinoIcebergAwsCredentialsProvider(new LocalCredentialSupplier(properties));
    } else if (SOURCE_REMOTE.equalsIgnoreCase(source)) {
      return new GravitinoIcebergAwsCredentialsProvider(new RemoteCredentialSupplier(properties));
    }

    throw new IllegalArgumentException("Unsupported Gravitino credential source: " + source);
  }

  @Override
  public AwsCredentials resolveCredentials() {
    long now = System.currentTimeMillis();
    AwsCredentials credentials = cachedCredentials;
    if (credentials != null && now < refreshTimeInMs) {
      return credentials;
    }

    synchronized (this) {
      now = System.currentTimeMillis();
      credentials = cachedCredentials;
      if (credentials != null && now < refreshTimeInMs) {
        return credentials;
      }

      Credential gravitinoCredential = selectS3Credential(credentialSupplier.getCredentials());
      credentials = toAwsCredentials(gravitinoCredential);
      cachedCredentials = credentials;
      refreshTimeInMs = calculateRefreshTimeInMs(gravitinoCredential.expireTimeInMs(), now);
      return credentials;
    }
  }

  @Override
  public void close() {
    try {
      credentialSupplier.close();
    } catch (IOException e) {
      throw new RuntimeException("Failed to close Gravitino Iceberg AWS credentials provider", e);
    }
  }

  private static long calculateRefreshTimeInMs(long expireTimeInMs, long now) {
    if (expireTimeInMs <= 0) {
      return Long.MAX_VALUE;
    }

    long timeToExpire = expireTimeInMs - now;
    if (timeToExpire <= 0) {
      return now;
    }

    return now + (long) (timeToExpire * REFRESH_TIME_FACTOR);
  }

  private static Credential selectS3Credential(Credential[] credentials) {
    Credential selected = null;
    for (Credential credential : credentials) {
      if (credential instanceof S3TokenCredential || credential instanceof AwsIrsaCredential) {
        selected = credential;
      } else if (selected == null && credential instanceof S3SecretKeyCredential) {
        selected = credential;
      }
    }

    if (selected == null) {
      throw new IllegalStateException("No S3 credential found for Iceberg server-side FileIO");
    }
    return selected;
  }

  private static AwsCredentials toAwsCredentials(Credential credential) {
    if (credential instanceof S3TokenCredential) {
      S3TokenCredential s3 = (S3TokenCredential) credential;
      return AwsSessionCredentials.builder()
          .accessKeyId(s3.accessKeyId())
          .secretAccessKey(s3.secretAccessKey())
          .sessionToken(s3.sessionToken())
          .expirationTime(Instant.ofEpochMilli(s3.expireTimeInMs()))
          .build();
    } else if (credential instanceof AwsIrsaCredential) {
      AwsIrsaCredential irsa = (AwsIrsaCredential) credential;
      return AwsSessionCredentials.builder()
          .accessKeyId(irsa.accessKeyId())
          .secretAccessKey(irsa.secretAccessKey())
          .sessionToken(irsa.sessionToken())
          .expirationTime(Instant.ofEpochMilli(irsa.expireTimeInMs()))
          .build();
    } else if (credential instanceof S3SecretKeyCredential) {
      S3SecretKeyCredential s3 = (S3SecretKeyCredential) credential;
      return AwsBasicCredentials.create(s3.accessKeyId(), s3.secretAccessKey());
    }

    throw new IllegalArgumentException("Unsupported S3 credential: " + credential.credentialType());
  }

  private interface CredentialSupplier extends Closeable {
    Credential[] getCredentials();
  }

  private static class LocalCredentialSupplier implements CredentialSupplier {
    private final CatalogCredentialManager credentialManager;
    private final List<String> credentialProviders;

    LocalCredentialSupplier(Map<String, String> properties) {
      String catalogName = required(properties, CATALOG_NAME);
      this.credentialManager = new CatalogCredentialManager(catalogName, properties);
      this.credentialProviders =
          new CredentialConfig(properties).get(CredentialConfig.CREDENTIAL_PROVIDERS);
    }

    @Override
    public Credential[] getCredentials() {
      CatalogCredentialContext context =
          new CatalogCredentialContext(PrincipalUtils.getCurrentUserName());
      List<Credential> credentials = new ArrayList<>();
      for (String credentialProvider : credentialProviders) {
        credentialManager.getCredential(credentialProvider, context).ifPresent(credentials::add);
      }
      return credentials.toArray(new Credential[0]);
    }

    @Override
    public void close() {
      credentialManager.close();
    }
  }

  private static class RemoteCredentialSupplier implements CredentialSupplier {
    private final GravitinoClient client;
    private final String catalogName;

    RemoteCredentialSupplier(Map<String, String> properties) {
      this.catalogName = required(properties, CATALOG_NAME);
      this.client = createGravitinoClient(properties);
    }

    @Override
    public Credential[] getCredentials() {
      Catalog catalog = client.loadCatalog(catalogName);
      return CredentialPropertyUtils.getCredentials(catalog);
    }

    @Override
    public void close() {
      client.close();
    }
  }

  private static GravitinoClient createGravitinoClient(Map<String, String> properties) {
    GravitinoClient.ClientBuilder builder =
        GravitinoClient.builder(required(properties, IcebergConstants.GRAVITINO_URI))
            .withMetalake(required(properties, IcebergConstants.GRAVITINO_METALAKE));
    String authType =
        properties.getOrDefault(IcebergConstants.GRAVITINO_AUTH_TYPE, SIMPLE_AUTH_TYPE);
    if (SIMPLE_AUTH_TYPE.equalsIgnoreCase(authType)) {
      builder.withSimpleAuth(
          properties.getOrDefault(
              IcebergConstants.GRAVITINO_SIMPLE_USERNAME, "iceberg-rest-server"));
    } else if (OAUTH2_AUTH_TYPE.equalsIgnoreCase(authType)) {
      DefaultOAuth2TokenProvider oAuth2TokenProvider =
          DefaultOAuth2TokenProvider.builder()
              .withUri(required(properties, IcebergConstants.GRAVITINO_OAUTH2_SERVER_URI))
              .withCredential(required(properties, IcebergConstants.GRAVITINO_OAUTH2_CREDENTIAL))
              .withPath(required(properties, IcebergConstants.GRAVITINO_OAUTH2_TOKEN_PATH))
              .withScope(required(properties, IcebergConstants.GRAVITINO_OAUTH2_SCOPE))
              .build();
      builder.withOAuth(oAuth2TokenProvider);
    } else {
      throw new UnsupportedOperationException("Unsupported auth type: " + authType);
    }
    return builder.build();
  }

  private static String required(Map<String, String> properties, String key) {
    String value = properties.get(key);
    Preconditions.checkArgument(StringUtils.isNotBlank(value), key + " should not be empty");
    return value;
  }
}
