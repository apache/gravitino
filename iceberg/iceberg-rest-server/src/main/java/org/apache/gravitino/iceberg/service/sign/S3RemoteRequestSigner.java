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
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.credential.Credential;
import org.apache.iceberg.rest.requests.RemoteSignRequest;
import org.apache.iceberg.rest.responses.ImmutableRemoteSignResponse;
import org.apache.iceberg.rest.responses.RemoteSignResponse;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;
import software.amazon.awssdk.services.s3.presigner.model.PresignedDeleteObjectRequest;
import software.amazon.awssdk.services.s3.presigner.model.PresignedGetObjectRequest;
import software.amazon.awssdk.services.s3.presigner.model.PresignedPutObjectRequest;

/** Signs S3 HTTP requests for Iceberg REST remote-signing. */
public class S3RemoteRequestSigner {

  /** Default signature duration for pre-signed S3 URLs. */
  public static final Duration DEFAULT_SIGNATURE_DURATION = Duration.ofHours(1);

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

  /**
   * Signs an S3 request described by the Iceberg REST {@link RemoteSignRequest}.
   *
   * @param request remote sign request from the Iceberg REST client
   * @param credential Gravitino credential used to sign the request
   * @return signed URI and headers for the client to call S3 directly
   */
  public RemoteSignResponse sign(RemoteSignRequest request, Credential credential) {
    String provider = request.provider() == null ? "s3" : request.provider();
    if (!"s3".equalsIgnoreCase(provider)) {
      throw new UnsupportedOperationException(
          "Remote signing is not supported for provider: " + provider);
    }

    AwsCredentials awsCredentials = AwsSigningCredentials.toAwsCredentials(credential);
    Region region = Region.of(request.region());
    S3Location location = S3Location.from(request.uri());
    String method = request.method().toUpperCase(Locale.ROOT);

    try (S3Presigner presigner = createPresigner(awsCredentials, region)) {
      switch (method) {
        case "PUT":
          return signPut(presigner, location);
        case "GET":
        case "HEAD":
          return signGet(presigner, location);
        case "DELETE":
          return signDelete(presigner, location);
        default:
          throw new UnsupportedOperationException(
              "Remote signing is not supported for HTTP method: " + method);
      }
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

  private RemoteSignResponse signPut(S3Presigner presigner, S3Location location) {
    PresignedPutObjectRequest presigned =
        presigner.presignPutObject(
            b ->
                b.signatureDuration(signatureDuration)
                    .putObjectRequest(
                        PutObjectRequest.builder()
                            .bucket(location.bucket())
                            .key(location.key())
                            .build()));
    return toResponse(presigned.url().toString(), presigned.signedHeaders());
  }

  private RemoteSignResponse signGet(S3Presigner presigner, S3Location location) {
    PresignedGetObjectRequest presigned =
        presigner.presignGetObject(
            b ->
                b.signatureDuration(signatureDuration)
                    .getObjectRequest(
                        GetObjectRequest.builder()
                            .bucket(location.bucket())
                            .key(location.key())
                            .build()));
    return toResponse(presigned.url().toString(), presigned.signedHeaders());
  }

  private RemoteSignResponse signDelete(S3Presigner presigner, S3Location location) {
    PresignedDeleteObjectRequest presigned =
        presigner.presignDeleteObject(
            b ->
                b.signatureDuration(signatureDuration)
                    .deleteObjectRequest(
                        DeleteObjectRequest.builder()
                            .bucket(location.bucket())
                            .key(location.key())
                            .build()));
    return toResponse(presigned.url().toString(), presigned.signedHeaders());
  }

  private static RemoteSignResponse toResponse(
      String uri, Map<String, List<String>> signedHeaders) {
    return ImmutableRemoteSignResponse.builder()
        .uri(URI.create(uri))
        .headers(signedHeaders)
        .build();
  }

  /** Parsed S3 bucket and object key. */
  static final class S3Location {
    private final String bucket;
    private final String key;

    private S3Location(String bucket, String key) {
      this.bucket = bucket;
      this.key = key;
    }

    String bucket() {
      return bucket;
    }

    String key() {
      return key;
    }

    static S3Location from(URI uri) {
      String location = RemoteSignPathValidator.toStorageLocation(uri);
      if (!location.startsWith("s3://")) {
        throw new IllegalArgumentException("Expected s3 URI after normalization: " + location);
      }
      String withoutScheme = location.substring("s3://".length());
      int slash = withoutScheme.indexOf('/');
      if (slash < 0) {
        throw new IllegalArgumentException("S3 object key is required: " + uri);
      }
      String bucket = withoutScheme.substring(0, slash);
      String key = withoutScheme.substring(slash + 1);
      if (StringUtils.isBlank(bucket) || StringUtils.isBlank(key)) {
        throw new IllegalArgumentException("S3 bucket and key are required: " + uri);
      }
      return new S3Location(bucket, key);
    }
  }
}
