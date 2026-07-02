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
package org.apache.gravitino.iceberg.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.EntityTag;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.credential.Credential;
import org.apache.gravitino.credential.CredentialPropertyUtils;
import org.apache.gravitino.iceberg.service.authorization.IcebergRESTServerContext;
import org.apache.gravitino.listener.api.event.IcebergRequestContext;
import org.apache.gravitino.utils.PrincipalUtils;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.StorageCredential;
import org.apache.iceberg.io.SupportsStorageCredentials;
import org.apache.iceberg.rest.RESTUtil;
import org.apache.iceberg.rest.responses.ErrorResponse;
import org.apache.iceberg.rest.responses.ImmutableLoadCredentialsResponse;
import org.apache.iceberg.rest.responses.LoadCredentialsResponse;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IcebergRESTUtils {

  private static final Logger LOG = LoggerFactory.getLogger(IcebergRESTUtils.class);

  /**
   * URL-encoded UTF-8 namespace separator ({@code 0x1F}) required by the Iceberg REST catalog spec.
   *
   * <p>Iceberg 1.11 allows a configurable separator in {@link RESTUtil#decodeNamespace}; use this
   * value so decoding stays aligned with the wire format used in existing deployments and tests.
   */
  public static final String NAMESPACE_SEPARATOR_URLENCODED_UTF_8 = "%1F";

  public static final String SNAPSHOT_ALL = "all";

  public static final String SNAPSHOT_REFS = "refs";

  /** Snapshot modes for the Iceberg loadTable endpoint. */
  public enum SnapshotMode {
    ALL(SNAPSHOT_ALL),
    REFS(SNAPSHOT_REFS);

    private final String value;

    SnapshotMode(String value) {
      this.value = value;
    }

    public String getValue() {
      return value;
    }
  }

  private IcebergRESTUtils() {}

  /**
   * Builds an Iceberg REST {@link org.apache.iceberg.rest.credentials.Credential} for load-table,
   * scan-plan, or credentials API responses.
   *
   * <p>Refresh credential endpoints are added only for credential types whose Iceberg client
   * modules support vended credential refresh: S3 ({@link
   * org.apache.gravitino.credential.S3TokenCredential}, {@link
   * org.apache.gravitino.credential.AwsIrsaCredential}), GCS ({@link
   * org.apache.gravitino.credential.GCSTokenCredential}), and ADLS ({@link
   * org.apache.gravitino.credential.ADLSTokenCredential}). OSS temporary credentials ({@link
   * org.apache.gravitino.credential.OSSTokenCredential}) are omitted because Iceberg's Aliyun OSS
   * FileIO does not consume {@code client.refresh-credentials-endpoint}; only the initial STS
   * properties are returned.
   *
   * @param catalogName IRC catalog name used in the refresh path
   * @param tableIdentifier table receiving the credential
   * @param credential Gravitino credential to vend
   * @param tableMetadata table metadata used to derive the storage prefix
   * @return Iceberg REST credential with prefix and config
   */
  public static org.apache.iceberg.rest.credentials.Credential toRESTCredential(
      String catalogName,
      TableIdentifier tableIdentifier,
      Credential credential,
      TableMetadata tableMetadata) {
    Map<String, String> config =
        new HashMap<>(CredentialPropertyUtils.toIcebergProperties(credential));
    config.putAll(buildRefreshProps(catalogName, tableIdentifier, config));

    return toRESTCredential(tableMetadata.location(), config);
  }

  /**
   * Builds an Iceberg REST {@link org.apache.iceberg.rest.credentials.Credential} from a storage
   * prefix and credential config map.
   *
   * @param prefix storage location prefix for the credential
   * @param config Iceberg credential config properties
   * @return Iceberg REST credential
   */
  public static org.apache.iceberg.rest.credentials.Credential toRESTCredential(
      String prefix, Map<String, String> config) {
    Map<String, String> credentialConfig = ImmutableMap.copyOf(config);
    return new org.apache.iceberg.rest.credentials.Credential() {
      @Override
      public String prefix() {
        return prefix;
      }

      @Override
      public Map<String, String> config() {
        return credentialConfig;
      }

      @Override
      public void validate() {}
    };
  }

  /**
   * Builds Iceberg REST 1.11 {@code storage-credentials} from an upstream REST catalog proxy.
   *
   * <p>Upstream credentials are read from {@link SupportsStorageCredentials#credentials()}. When
   * present, they are filtered and rewritten with IRC-local refresh endpoints via {@link
   * CredentialPropertyUtils}. Credential properties in {@link FileIO#properties()} must be handled
   * separately by the caller.
   *
   * @param catalogName IRC catalog name used to build refresh paths
   * @param tableIdentifier table receiving the credentials
   * @param fileIO table FileIO returned by the upstream catalog
   * @return rewritten storage credentials for the downstream client, or an empty list
   */
  public static List<org.apache.iceberg.rest.credentials.Credential> buildStorageCreds(
      String catalogName, TableIdentifier tableIdentifier, FileIO fileIO) {
    if (!(fileIO instanceof SupportsStorageCredentials)) {
      return Collections.emptyList();
    }

    List<StorageCredential> credentials = ((SupportsStorageCredentials) fileIO).credentials();
    if (credentials == null || credentials.isEmpty()) {
      return Collections.emptyList();
    }

    List<org.apache.iceberg.rest.credentials.Credential> restCredentials = new ArrayList<>();
    for (StorageCredential credential : credentials) {
      restCredentials.add(
          rewriteCredential(
              catalogName, tableIdentifier, credential.prefix(), credential.config()));
    }

    return List.copyOf(restCredentials);
  }

  /**
   * Rewrites credentials returned by an upstream REST catalog so their {@code
   * *.refresh-credentials-endpoint} entries point at this IRC instance instead of the upstream
   * catalog. Without this, a federated {@code getTableCredentials} would return refresh endpoints
   * scoped to the remote catalog's prefix, inconsistent with the loadTable/createTable paths that
   * already rewrite via {@link #buildStorageCreds}.
   *
   * @param catalogName IRC catalog name used to build refresh paths
   * @param tableIdentifier table receiving the credentials
   * @param upstream the credentials response returned by the upstream REST catalog
   * @return a credentials response with IRC-local refresh endpoints
   */
  public static LoadCredentialsResponse rewriteTableCredentials(
      String catalogName, TableIdentifier tableIdentifier, LoadCredentialsResponse upstream) {
    ImmutableLoadCredentialsResponse.Builder builder = ImmutableLoadCredentialsResponse.builder();
    for (org.apache.iceberg.rest.credentials.Credential credential : upstream.credentials()) {
      builder.addCredentials(
          rewriteCredential(
              catalogName, tableIdentifier, credential.prefix(), credential.config()));
    }
    return builder.build();
  }

  /**
   * Filters an upstream credential's config to recognized credential properties and re-applies
   * IRC-local refresh-endpoint properties, dropping any refresh endpoint scoped to the upstream
   * catalog.
   *
   * @param catalogName IRC catalog name used to build refresh paths
   * @param tableIdentifier table receiving the credential
   * @param prefix storage location prefix for the credential
   * @param config upstream credential config
   * @return an Iceberg REST credential with IRC-local refresh endpoints
   */
  private static org.apache.iceberg.rest.credentials.Credential rewriteCredential(
      String catalogName,
      TableIdentifier tableIdentifier,
      String prefix,
      Map<String, String> config) {
    Map<String, String> filteredConfig = CredentialPropertyUtils.filterCredentialProperties(config);
    filteredConfig.putAll(buildRefreshProps(catalogName, tableIdentifier, filteredConfig));
    return toRESTCredential(prefix, ImmutableMap.copyOf(filteredConfig));
  }

  public static <T> Response ok(T t) {
    return Response.status(Response.Status.OK).entity(t).type(MediaType.APPLICATION_JSON).build();
  }

  /**
   * Builds an OK response with the ETag header derived from the table metadata location. Uses the
   * default snapshots value to ensure ETags from create/update/register are consistent with the
   * default loadTable endpoint.
   *
   * @param loadTableResponse the table response to include in the body
   * @return a Response with ETag header set
   */
  public static Response buildResponseWithETag(LoadTableResponse loadTableResponse) {
    Optional<EntityTag> etag =
        generateETag(
            loadTableResponse.tableMetadata().metadataFileLocation(), SnapshotMode.ALL.getValue());
    return buildResponseWithETag(loadTableResponse, etag);
  }

  /**
   * Builds an OK response with the given ETag header.
   *
   * @param loadTableResponse the table response to include in the body
   * @param etag the pre-computed ETag
   * @return a Response with ETag header set if etag is present
   */
  public static Response buildResponseWithETag(
      LoadTableResponse loadTableResponse, Optional<EntityTag> etag) {
    Response.ResponseBuilder responseBuilder =
        Response.ok(loadTableResponse, MediaType.APPLICATION_JSON_TYPE);
    etag.ifPresent(responseBuilder::tag);
    return responseBuilder.build();
  }

  /**
   * Generates an ETag based on the table metadata file location. The ETag is a SHA-256 hash of the
   * metadata location, which changes whenever the table metadata is updated. Uses the default
   * snapshots value to ensure consistency.
   *
   * @param metadataLocation the metadata file location
   * @return the generated ETag
   */
  public static Optional<EntityTag> generateETag(String metadataLocation) {
    return generateETag(metadataLocation, SnapshotMode.ALL.getValue());
  }

  /**
   * Generates an ETag based on the table metadata file location and snapshot mode. The ETag is a
   * SHA-256 hash that incorporates both the metadata location and the snapshots parameter, ensuring
   * distinct ETags for different representations of the same table version (e.g., snapshots=all vs
   * snapshots=refs).
   *
   * @param metadataLocation the metadata file location
   * @param snapshots the snapshots query parameter value (e.g., "all", "refs")
   * @return the generated ETag
   */
  public static Optional<EntityTag> generateETag(String metadataLocation, String snapshots) {
    if (StringUtils.isBlank(metadataLocation)) {
      return Optional.empty();
    }
    try {
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      digest.update(metadataLocation.getBytes(StandardCharsets.UTF_8));
      digest.update(snapshots.getBytes(StandardCharsets.UTF_8));
      byte[] hash = digest.digest();
      StringBuilder hexString = new StringBuilder();
      for (byte b : hash) {
        String hex = Integer.toHexString(0xff & b);
        if (hex.length() == 1) {
          hexString.append('0');
        }
        hexString.append(hex);
      }
      return Optional.of(new EntityTag(hexString.toString()));
    } catch (NoSuchAlgorithmException e) {
      LOG.warn("Failed to generate ETag for metadata location: {}", metadataLocation, e);
      return Optional.empty();
    }
  }

  public static Response okWithoutContent() {
    return Response.status(Response.Status.OK).build();
  }

  public static Response noContent() {
    return Response.status(Status.NO_CONTENT).build();
  }

  public static Response notExists() {
    return Response.status(Status.NOT_FOUND).build();
  }

  public static Response errorResponse(Throwable ex, int httpStatus) {
    ErrorResponse errorResponse =
        ErrorResponse.builder()
            .responseCode(httpStatus)
            .withType(ex.getClass().getSimpleName())
            .withMessage(ex.getMessage())
            .withStackTrace(ex)
            .build();
    return Response.status(httpStatus)
        .entity(errorResponse)
        .type(MediaType.APPLICATION_JSON)
        .build();
  }

  /**
   * Build an Iceberg {@link ErrorResponse} for a given HTTP status code and message, without an
   * exception. Used by the Jetty error handler for pre-JAX-RS errors.
   */
  public static ErrorResponse errorResponse(int httpStatus, String type, String message) {
    return ErrorResponse.builder()
        .responseCode(httpStatus)
        .withType(type)
        .withMessage(message)
        .build();
  }

  public static Instant calculateNewTimestamp(Instant currentTimestamp, int hours) {
    LocalDateTime currentDateTime =
        LocalDateTime.ofInstant(currentTimestamp, ZoneId.systemDefault());
    LocalDateTime nextHourDateTime;
    if (hours > 0) {
      nextHourDateTime = currentDateTime.plusHours(hours);
    } else {
      nextHourDateTime = currentDateTime.minusHours(-hours);
    }
    return nextHourDateTime.atZone(ZoneId.systemDefault()).toInstant();
  }

  public static NameIdentifier getGravitinoNameIdentifier(
      String metalakeName, String catalogName, TableIdentifier icebergIdentifier) {
    Stream<String> catalogNS =
        Stream.concat(
            Stream.of(metalakeName, catalogName),
            Arrays.stream(icebergIdentifier.namespace().levels()));
    String[] catalogNSTable =
        Stream.concat(catalogNS, Stream.of(icebergIdentifier.name())).toArray(String[]::new);
    return NameIdentifier.of(catalogNSTable);
  }

  public static String getCatalogName(String rawPrefix) {
    String catalogName = normalizePrefix(rawPrefix);
    if (StringUtils.isBlank(catalogName)) {
      return IcebergRESTServerContext.getInstance().defaultCatalogName();
    }
    return catalogName;
  }

  public static <T> T cloneIcebergRESTObject(Object message, Class<T> className) {
    ObjectMapper icebergObjectMapper = IcebergObjectMapper.getInstance();
    try {
      byte[] values = icebergObjectMapper.writeValueAsBytes(message);
      return icebergObjectMapper.readValue(values, className);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static NameIdentifier getGravitinoNameIdentifier(
      String metalakeName, String catalogName, Namespace namespace) {
    Stream<String> catalogNS =
        Stream.concat(Stream.of(metalakeName, catalogName), Arrays.stream(namespace.levels()));
    return NameIdentifier.of(catalogNS.toArray(String[]::new));
  }

  public static Map<String, String> getHttpHeaders(HttpServletRequest httpServletRequest) {
    Map<String, String> headers = new HashMap<>();
    Enumeration<String> headerNames = httpServletRequest.getHeaderNames();
    while (headerNames.hasMoreElements()) {
      String headerName = headerNames.nextElement();
      String headerValue = httpServletRequest.getHeader(headerName);
      if (headerValue != null) {
        headers.put(headerName, headerValue);
      }
    }
    return headers;
  }

  /**
   * Builds an {@link IcebergRequestContext} from the current HTTP request.
   *
   * @param httpServletRequest The HTTP servlet request.
   * @param catalogName The Iceberg catalog name.
   * @return A new {@link IcebergRequestContext}.
   */
  public static IcebergRequestContext getIcebergRequestContext(
      HttpServletRequest httpServletRequest, String catalogName) {
    return getIcebergRequestContext(httpServletRequest, catalogName, false);
  }

  /**
   * Builds an {@link IcebergRequestContext} from the current HTTP request.
   *
   * @param httpServletRequest The HTTP servlet request.
   * @param catalogName The Iceberg catalog name.
   * @param requestCredentialVending Whether the request is for credential vending.
   * @return A new {@link IcebergRequestContext}.
   */
  public static IcebergRequestContext getIcebergRequestContext(
      HttpServletRequest httpServletRequest, String catalogName, boolean requestCredentialVending) {
    return new IcebergRequestContext(
        catalogName,
        PrincipalUtils.getCurrentUserName(),
        resolveClientAddress(httpServletRequest),
        getHttpHeaders(httpServletRequest),
        requestCredentialVending);
  }

  /**
   * Builds refresh credential endpoint properties for a table, with Iceberg REST path encoding.
   *
   * @param catalogName IRC catalog name used in the refresh path
   * @param tableIdentifier table receiving the credentials
   * @param credentialProperties Iceberg credential properties used to determine refresh keys
   * @return refresh endpoint properties keyed by Iceberg client config names
   */
  public static Map<String, String> buildRefreshProps(
      String catalogName,
      TableIdentifier tableIdentifier,
      Map<String, String> credentialProperties) {
    return CredentialPropertyUtils.buildRefreshProps(
        RESTUtil.encodeString(catalogName),
        RESTUtil.encodeNamespace(tableIdentifier.namespace(), NAMESPACE_SEPARATOR_URLENCODED_UTF_8),
        RESTUtil.encodeString(tableIdentifier.name()),
        credentialProperties);
  }

  private static String resolveClientAddress(HttpServletRequest request) {
    String xForwardedFor = request.getHeader("X-Forwarded-For");
    if (StringUtils.isNotBlank(xForwardedFor)) {
      return xForwardedFor.split(",")[0].trim();
    }
    return request.getRemoteHost();
  }

  // remove the last '/' from the prefix, for example transform 'iceberg_catalog/' to
  // 'iceberg_catalog'
  private static String normalizePrefix(String rawPrefix) {
    if (StringUtils.isBlank(rawPrefix)) {
      return rawPrefix;
    } else {
      // rawPrefix is a string matching ([^/]*/) which end with /
      Preconditions.checkArgument(
          rawPrefix.endsWith("/"), String.format("rawPrefix %s format is illegal", rawPrefix));
      return rawPrefix.substring(0, rawPrefix.length() - 1);
    }
  }
}
