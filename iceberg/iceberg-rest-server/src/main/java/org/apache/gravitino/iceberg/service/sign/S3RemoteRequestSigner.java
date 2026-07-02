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
package org.apache.gravitino.iceberg.service.sign;

import java.net.URI;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergConstants;
import org.apache.gravitino.credential.AwsIrsaCredential;
import org.apache.gravitino.credential.Credential;
import org.apache.gravitino.credential.S3SecretKeyCredential;
import org.apache.gravitino.credential.S3TokenCredential;
import org.apache.gravitino.iceberg.common.IcebergConfig;
import org.apache.gravitino.iceberg.service.sign.RemoteSignSupport.Provider;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.apache.iceberg.rest.RESTCatalogProperties;
import org.apache.iceberg.rest.requests.RemoteSignRequest;
import org.apache.iceberg.rest.responses.ImmutableRemoteSignResponse;
import org.apache.iceberg.rest.responses.RemoteSignResponse;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;

/** Signs S3 HTTP requests for Iceberg REST remote-signing. */
public class S3RemoteRequestSigner implements RemoteRequestSigner {

  private final String endpoint;
  private final boolean pathStyleAccess;
  private final Duration signatureDuration;

  /**
   * Creates an S3 remote request signer.
   *
   * @param endpoint optional S3 endpoint override for S3-compatible storage
   * @param pathStyleAccess whether path-style access is enabled
   * @param signatureDuration duration for generated pre-signed URLs
   */
  public S3RemoteRequestSigner(
      String endpoint, boolean pathStyleAccess, Duration signatureDuration) {
    this.endpoint = endpoint;
    this.pathStyleAccess = pathStyleAccess;
    this.signatureDuration = signatureDuration;
  }

  @Override
  public Provider provider() {
    return Provider.S3;
  }

  @Override
  public Map<String, String> clientConfig(IcebergConfig icebergConfig, String signerEndpoint) {
    Map<String, String> config = new HashMap<>();
    config.put(RESTCatalogProperties.SIGNER_ENDPOINT, signerEndpoint);
    config.put(S3FileIOProperties.REMOTE_SIGNING_ENABLED, "true");
    String region = icebergConfig.getRawString(IcebergConfig.S3_REGION.getKey());
    if (StringUtils.isNotBlank(region)) {
      config.put(IcebergConstants.AWS_S3_REGION, region);
    }
    return config;
  }

  @Override
  public RemoteSignResponse sign(RemoteSignRequest request, Credential credential) {
    Provider provider = Provider.fromRequest(request.provider(), request.uri());
    if (provider != Provider.S3) {
      throw new UnsupportedOperationException(
          "S3 signer cannot sign provider: " + provider.providerName());
    }

    String location = RemoteSignSupport.normalize(provider, request.uri());
    RemoteSignSupport.BucketKey bucketKey = RemoteSignSupport.parseBucketKey(provider, location);
    AwsCredentials awsCredentials = toAwsCredentials(credential);
    Region region = Region.of(request.region());
    String method = request.method().toUpperCase(Locale.ROOT);

    try (S3Presigner presigner = createPresigner(awsCredentials, region)) {
      return presign(presigner, method, bucketKey);
    }
  }

  private RemoteSignResponse presign(
      S3Presigner presigner, String method, RemoteSignSupport.BucketKey bucketKey) {
    String bucket = bucketKey.bucket();
    String key = bucketKey.key();
    switch (method) {
      case "PUT": {
        var presigned =
            presigner.presignPutObject(
                builder ->
                    builder
                        .signatureDuration(signatureDuration)
                        .putObjectRequest(
                            PutObjectRequest.builder().bucket(bucket).key(key).build()));
        return toResponse(presigned.url().toString(), presigned.signedHeaders());
      }
      case "GET":
      case "HEAD": {
        var presigned =
            presigner.presignGetObject(
                builder ->
                    builder
                        .signatureDuration(signatureDuration)
                        .getObjectRequest(
                            GetObjectRequest.builder().bucket(bucket).key(key).build()));
        return toResponse(presigned.url().toString(), presigned.signedHeaders());
      }
      case "DELETE": {
        var presigned =
            presigner.presignDeleteObject(
                builder ->
                    builder
                        .signatureDuration(signatureDuration)
                        .deleteObjectRequest(
                            DeleteObjectRequest.builder().bucket(bucket).key(key).build()));
        return toResponse(presigned.url().toString(), presigned.signedHeaders());
      }
      default:
        throw new UnsupportedOperationException(
            "Remote signing is not supported for HTTP method: " + method);
    }
  }

  private S3Presigner createPresigner(AwsCredentials awsCredentials, Region region) {
    S3Configuration s3Configuration =
        S3Configuration.builder().pathStyleAccessEnabled(pathStyleAccess).build();
    S3Presigner.Builder builder =
        S3Presigner.builder()
            .region(region)
            .credentialsProvider(StaticCredentialsProvider.create(awsCredentials))
            .serviceConfiguration(s3Configuration);
    if (StringUtils.isNotBlank(endpoint)) {
      builder.endpointOverride(URI.create(endpoint));
    }
    return builder.build();
  }

  private static RemoteSignResponse toResponse(
      String uri, Map<String, List<String>> signedHeaders) {
    return ImmutableRemoteSignResponse.builder()
        .uri(URI.create(uri))
        .headers(signedHeaders)
        .build();
  }

  private static AwsCredentials toAwsCredentials(Credential credential) {
    if (credential instanceof S3SecretKeyCredential) {
      S3SecretKeyCredential secretKeyCredential = (S3SecretKeyCredential) credential;
      return AwsBasicCredentials.create(
          secretKeyCredential.accessKeyId(), secretKeyCredential.secretAccessKey());
    }
    if (credential instanceof S3TokenCredential) {
      S3TokenCredential tokenCredential = (S3TokenCredential) credential;
      return AwsSessionCredentials.create(
          tokenCredential.accessKeyId(),
          tokenCredential.secretAccessKey(),
          tokenCredential.sessionToken());
    }
    if (credential instanceof AwsIrsaCredential) {
      AwsIrsaCredential irsaCredential = (AwsIrsaCredential) credential;
      return AwsSessionCredentials.create(
          irsaCredential.accessKeyId(),
          irsaCredential.secretAccessKey(),
          irsaCredential.sessionToken());
    }
    throw new UnsupportedOperationException(
        "Credential type does not support S3 remote signing: " + credential.credentialType());
  }
}
