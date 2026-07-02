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

import com.google.common.collect.ImmutableList;
import java.net.URI;
import java.time.Duration;
import java.util.EnumMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.credential.Credential;
import org.apache.gravitino.iceberg.common.IcebergConfig;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.iceberg.rest.requests.RemoteSignRequest;
import org.apache.iceberg.rest.responses.RemoteSignResponse;

/** Registry and entry point for Iceberg REST remote signing. */
public class RemoteSignSupport {

  /** Default signature duration for pre-signed object-storage URLs. */
  public static final Duration DEFAULT_SIGNATURE_DURATION = Duration.ofHours(1);

  /** Object-storage providers supported for Iceberg REST remote signing. */
  public enum Provider {
    S3("s3", "s3", "s3a"),
    GCS("gcs", "gs"),
    OSS("oss", "oss"),
    ADLS("adls", "abfs", "abfss", "wasb", "wasbs");

    private final String providerName;
    private final ImmutableList<String> schemes;

    Provider(String providerName, String... schemes) {
      this.providerName = providerName;
      this.schemes = ImmutableList.copyOf(schemes);
    }

    /**
     * Returns the provider name used in Iceberg REST {@code RemoteSignRequest#provider()}.
     *
     * @return provider name
     */
    public String providerName() {
      return providerName;
    }

    /**
     * Returns the canonical location prefix for bucket/object URIs, such as {@code s3://}.
     *
     * @return location prefix including {@code ://}
     */
    String locationPrefix() {
      switch (this) {
        case S3:
          return "s3://";
        case GCS:
          return "gs://";
        case OSS:
          return "oss://";
        default:
          throw new IllegalArgumentException(
              "Provider does not use bucket/object location prefixes: " + providerName);
      }
    }

    /**
     * Resolves a provider from a table or object-storage location URI.
     *
     * @param location storage location URI
     * @return matching provider
     */
    public static Provider fromLocation(String location) {
      if (StringUtils.isBlank(location)) {
        throw new IllegalArgumentException("Storage location is required");
      }
      return fromScheme(URI.create(location).getScheme());
    }

    /**
     * Resolves a provider from a request provider name, falling back to the URI scheme.
     *
     * @param provider provider from {@code RemoteSignRequest}
     * @param uri request URI used when provider is absent
     * @return matching provider
     */
    public static Provider fromRequest(String provider, URI uri) {
      if (StringUtils.isNotBlank(provider)) {
        for (Provider candidate : values()) {
          if (candidate.providerName.equalsIgnoreCase(provider.trim())) {
            return candidate;
          }
        }
        throw new UnsupportedOperationException(
            "Remote signing is not supported for provider: " + provider);
      }
      if (uri == null || uri.getScheme() == null) {
        throw new IllegalArgumentException("Remote sign provider or URI scheme is required");
      }
      return fromScheme(uri.getScheme());
    }

    private static Provider fromScheme(String scheme) {
      if (scheme == null) {
        throw new IllegalArgumentException("URI scheme is required");
      }
      String normalized = scheme.toLowerCase(Locale.ROOT);
      for (Provider candidate : values()) {
        for (String supportedScheme : candidate.schemes) {
          if (supportedScheme.equals(normalized)) {
            return candidate;
          }
        }
      }
      throw new IllegalArgumentException("Unsupported URI scheme for remote signing: " + scheme);
    }
  }

  private final Map<Provider, RemoteRequestSigner> signers;

  /**
   * Creates remote-sign support from catalog configuration.
   *
   * <p>To add a new object-storage provider, implement {@link RemoteRequestSigner} and register it
   * here.
   *
   * @param icebergConfig catalog configuration
   */
  public RemoteSignSupport(IcebergConfig icebergConfig) {
    Map<Provider, RemoteRequestSigner> signerMap = new EnumMap<>(Provider.class);
    signerMap.put(
        Provider.S3,
        new S3RemoteRequestSigner(
            icebergConfig.get(IcebergConfig.S3_ENDPOINT),
            icebergConfig.get(IcebergConfig.S3_PATH_STYLE_ACCESS),
            DEFAULT_SIGNATURE_DURATION));
    this.signers = Map.copyOf(signerMap);
  }

  /**
   * Returns whether remote signing is available for the provider.
   *
   * @param provider object-storage provider
   * @return true when a signer is registered
   */
  public boolean supports(Provider provider) {
    return signers.containsKey(provider);
  }

  /**
   * Builds remote-signing client config for load/create/register responses.
   *
   * @param provider object-storage provider inferred from the table location
   * @param signerEndpoint IRC signer endpoint for the table
   * @param icebergConfig catalog configuration
   * @return client config properties
   */
  public Map<String, String> clientConfig(
      Provider provider, String signerEndpoint, IcebergConfig icebergConfig) {
    return requireSigner(provider).clientConfig(icebergConfig, signerEndpoint);
  }

  /**
   * Signs a remote request using the provider declared in the request or inferred from the URI.
   *
   * @param request remote sign request
   * @param credential credential used to sign the request
   * @return signed URI and headers
   */
  public RemoteSignResponse sign(RemoteSignRequest request, Credential credential) {
    Provider provider = Provider.fromRequest(request.provider(), request.uri());
    return requireSigner(provider).sign(request, credential);
  }

  private RemoteRequestSigner requireSigner(Provider provider) {
    RemoteRequestSigner signer = signers.get(provider);
    if (signer == null) {
      throw new UnsupportedOperationException(
          "Remote signing is not implemented for provider: " + provider.providerName());
    }
    return signer;
  }

  /**
   * Validates that the request URI resolves to an object under one of the allowed prefixes.
   *
   * @param provider storage provider for the table
   * @param requestUri URI from the remote sign request
   * @param allowedPrefixes table location prefixes
   * @throws ForbiddenException if the URI is outside the allowed prefixes
   */
  public static void validateWithinPrefixes(
      Provider provider, URI requestUri, Set<String> allowedPrefixes) {
    String requestLocation = normalize(provider, requestUri);
    for (String prefix : allowedPrefixes) {
      if (StringUtils.isBlank(prefix)) {
        continue;
      }
      String normalizedPrefix = normalize(provider, URI.create(prefix));
      if (requestLocation.equals(normalizedPrefix)
          || requestLocation.startsWith(normalizedPrefix + "/")) {
        return;
      }
    }
    throw new ForbiddenException(
        "Remote sign URI %s is outside allowed table locations: %s", requestUri, allowedPrefixes);
  }

  /**
   * Converts a request URI to a normalized storage location string for prefix checks.
   *
   * @param provider storage provider
   * @param uri request URI
   * @return canonical location without a trailing slash
   */
  public static String normalize(Provider provider, URI uri) {
    switch (provider) {
      case S3:
        return normalizeS3(uri);
      case GCS:
      case OSS:
        return normalizeBucketObject(provider, uri);
      case ADLS:
        return normalizeAdls(uri);
      default:
        throw new IllegalArgumentException("Unsupported provider: " + provider);
    }
  }

  /**
   * Parses bucket and object key from a normalized storage URI.
   *
   * @param provider storage provider
   * @param location normalized storage location
   * @return bucket and key
   */
  public static BucketKey parseBucketKey(Provider provider, String location) {
    String expectedPrefix = provider.locationPrefix();
    if (!location.startsWith(expectedPrefix)) {
      throw new IllegalArgumentException("Expected " + expectedPrefix + " location: " + location);
    }
    String withoutScheme = location.substring(expectedPrefix.length());
    int slash = withoutScheme.indexOf('/');
    if (slash < 0) {
      throw new IllegalArgumentException("Object key is required: " + location);
    }
    String bucket = withoutScheme.substring(0, slash);
    String key = withoutScheme.substring(slash + 1);
    if (StringUtils.isBlank(bucket) || StringUtils.isBlank(key)) {
      throw new IllegalArgumentException("Bucket and key are required: " + location);
    }
    return new BucketKey(bucket, key);
  }

  /** Parsed bucket and object key. */
  public static final class BucketKey {
    private final String bucket;
    private final String key;

    BucketKey(String bucket, String key) {
      this.bucket = bucket;
      this.key = key;
    }

    public String bucket() {
      return bucket;
    }

    public String key() {
      return key;
    }
  }

  private static String normalizeBucketObject(Provider provider, URI uri) {
    String scheme = uri.getScheme();
    if (scheme == null) {
      throw new IllegalArgumentException("URI scheme is required: " + uri);
    }
    String canonicalScheme =
        provider == Provider.GCS
            ? "gs"
            : provider == Provider.OSS ? "oss" : provider.providerName();
    if (!canonicalScheme.equalsIgnoreCase(scheme)) {
      if (provider == Provider.GCS && "https".equalsIgnoreCase(scheme)) {
        return normalizeGcsHttps(uri);
      }
      if (provider == Provider.OSS
          && ("http".equalsIgnoreCase(scheme) || "https".equalsIgnoreCase(scheme))) {
        return normalizeOssHttps(uri);
      }
      throw new IllegalArgumentException(
          "Unsupported URI scheme for " + provider + " remote signing: " + scheme);
    }
    return trimTrailingSlash(canonicalScheme + "://" + uri.getHost() + uri.getPath());
  }

  private static String normalizeS3(URI uri) {
    String scheme = uri.getScheme();
    if (scheme == null) {
      throw new IllegalArgumentException("URI scheme is required: " + uri);
    }
    if ("s3".equalsIgnoreCase(scheme) || "s3a".equalsIgnoreCase(scheme)) {
      return trimTrailingSlash("s3://" + uri.getHost() + uri.getPath());
    }
    if ("http".equalsIgnoreCase(scheme) || "https".equalsIgnoreCase(scheme)) {
      return normalizeS3Https(uri);
    }
    throw new IllegalArgumentException("Unsupported URI scheme for S3 remote signing: " + scheme);
  }

  private static String normalizeS3Https(URI uri) {
    String host = uri.getHost();
    if (host == null) {
      throw new IllegalArgumentException("URI host is required: " + uri);
    }
    String path = trimLeadingSlash(uri.getPath());
    int s3Index = host.indexOf(".s3");
    if (s3Index > 0) {
      String bucket = host.substring(0, s3Index);
      return trimTrailingSlash("s3://" + bucket + (path.isEmpty() ? "" : "/" + path));
    }
    if (host.startsWith("s3.") || "s3.amazonaws.com".equals(host)) {
      int slash = path.indexOf('/');
      if (slash < 0) {
        throw new IllegalArgumentException("Path-style S3 URI must include a key: " + uri);
      }
      String bucket = path.substring(0, slash);
      String key = path.substring(slash);
      return trimTrailingSlash("s3://" + bucket + key);
    }
    throw new IllegalArgumentException("Unrecognized S3 HTTP endpoint: " + host);
  }

  private static String normalizeGcsHttps(URI uri) {
    String host = uri.getHost();
    if (host == null || !host.equals("storage.googleapis.com")) {
      throw new IllegalArgumentException("Unrecognized GCS HTTP endpoint: " + host);
    }
    String path = trimLeadingSlash(uri.getPath());
    int slash = path.indexOf('/');
    if (slash < 0) {
      throw new IllegalArgumentException("GCS URI must include an object key: " + uri);
    }
    String bucket = path.substring(0, slash);
    String key = path.substring(slash + 1);
    return trimTrailingSlash("gs://" + bucket + "/" + key);
  }

  private static String normalizeOssHttps(URI uri) {
    String host = uri.getHost();
    if (host == null) {
      throw new IllegalArgumentException("URI host is required: " + uri);
    }
    String path = trimLeadingSlash(uri.getPath());
    if (path.isEmpty()) {
      throw new IllegalArgumentException("OSS URI must include an object key: " + uri);
    }
    if (host.startsWith("oss-")) {
      int dot = host.indexOf('.', 4);
      String bucket = dot > 0 ? host.substring(4, dot) : host.substring(4);
      return trimTrailingSlash("oss://" + bucket + "/" + path);
    }
    int slash = path.indexOf('/');
    if (slash < 0) {
      throw new IllegalArgumentException("Path-style OSS URI must include a key: " + uri);
    }
    String bucket = path.substring(0, slash);
    String key = path.substring(slash + 1);
    return trimTrailingSlash("oss://" + bucket + "/" + key);
  }

  private static String normalizeAdls(URI uri) {
    String scheme = uri.getScheme();
    if (scheme == null) {
      throw new IllegalArgumentException("URI scheme is required: " + uri);
    }
    String normalizedScheme = scheme.toLowerCase(Locale.ROOT);
    if (!"abfs".equals(normalizedScheme)
        && !"abfss".equals(normalizedScheme)
        && !"wasb".equals(normalizedScheme)
        && !"wasbs".equals(normalizedScheme)) {
      if ("http".equalsIgnoreCase(scheme) || "https".equalsIgnoreCase(scheme)) {
        return normalizeAdlsHttps(uri);
      }
      throw new IllegalArgumentException(
          "Unsupported URI scheme for ADLS remote signing: " + scheme);
    }
    String authority = uri.getAuthority();
    if (StringUtils.isBlank(authority) || !authority.contains("@")) {
      throw new IllegalArgumentException("ADLS URI must include container@account: " + uri);
    }
    return trimTrailingSlash(normalizedScheme + "://" + authority + uri.getPath());
  }

  private static String normalizeAdlsHttps(URI uri) {
    String host = uri.getHost();
    if (host == null || !host.endsWith(".dfs.core.windows.net")) {
      throw new IllegalArgumentException("Unrecognized ADLS HTTP endpoint: " + host);
    }
    String account = host.substring(0, host.length() - ".dfs.core.windows.net".length());
    String path = trimLeadingSlash(uri.getPath());
    int slash = path.indexOf('/');
    if (slash < 0) {
      throw new IllegalArgumentException("ADLS HTTP URI must include container and path: " + uri);
    }
    String container = path.substring(0, slash);
    String objectPath = path.substring(slash);
    return trimTrailingSlash(
        "abfs://" + container + "@" + account + ".dfs.core.windows.net" + objectPath);
  }

  private static String trimLeadingSlash(String path) {
    if (path == null) {
      return "";
    }
    return path.startsWith("/") ? path.substring(1) : path;
  }

  private static String trimTrailingSlash(String location) {
    String normalized = location.trim();
    while (normalized.endsWith("/")) {
      normalized = normalized.substring(0, normalized.length() - 1);
    }
    return normalized;
  }
}
