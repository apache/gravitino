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
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.EntityTag;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.iceberg.service.authorization.IcebergRESTServerContext;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.responses.ErrorResponse;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IcebergRESTUtils {

  private static final Logger LOG = LoggerFactory.getLogger(IcebergRESTUtils.class);

  private static final String DEFAULT_SNAPSHOTS = "all";

  private IcebergRESTUtils() {}

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
    EntityTag etag =
        generateETag(loadTableResponse.tableMetadata().metadataFileLocation(), DEFAULT_SNAPSHOTS);
    return buildResponseWithETag(loadTableResponse, etag);
  }

  /**
   * Builds an OK response with the given ETag header.
   *
   * @param loadTableResponse the table response to include in the body
   * @param etag the pre-computed ETag, may be null
   * @return a Response with ETag header set if etag is non-null
   */
  public static Response buildResponseWithETag(
      LoadTableResponse loadTableResponse, EntityTag etag) {
    Response.ResponseBuilder responseBuilder =
        Response.ok(loadTableResponse, MediaType.APPLICATION_JSON_TYPE);
    if (etag != null) {
      responseBuilder.tag(etag);
    }
    return responseBuilder.build();
  }

  /**
   * Generates an ETag based on the table metadata file location. The ETag is a SHA-256 hash of the
   * metadata location, which changes whenever the table metadata is updated.
   *
   * @param metadataLocation the metadata file location
   * @return the generated ETag, or null if generation fails
   */
  public static EntityTag generateETag(String metadataLocation) {
    return generateETag(metadataLocation, null);
  }

  /**
   * Generates an ETag based on the table metadata file location and snapshot mode. The ETag is a
   * SHA-256 hash that incorporates both the metadata location and the snapshots parameter, ensuring
   * distinct ETags for different representations of the same table version (e.g., snapshots=all vs
   * snapshots=refs).
   *
   * @param metadataLocation the metadata file location
   * @param snapshots the snapshots query parameter value (e.g., "all", "refs"), may be null
   * @return the generated ETag, or null if generation fails
   */
  public static EntityTag generateETag(String metadataLocation, String snapshots) {
    if (metadataLocation == null) {
      return null;
    }
    try {
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      digest.update(metadataLocation.getBytes(StandardCharsets.UTF_8));
      if (snapshots != null) {
        digest.update(snapshots.getBytes(StandardCharsets.UTF_8));
      }
      byte[] hash = digest.digest();
      StringBuilder hexString = new StringBuilder();
      for (byte b : hash) {
        String hex = Integer.toHexString(0xff & b);
        if (hex.length() == 1) {
          hexString.append('0');
        }
        hexString.append(hex);
      }
      return new EntityTag(hexString.toString());
    } catch (NoSuchAlgorithmException e) {
      LOG.warn("Failed to generate ETag for metadata location: {}", metadataLocation, e);
      return null;
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
