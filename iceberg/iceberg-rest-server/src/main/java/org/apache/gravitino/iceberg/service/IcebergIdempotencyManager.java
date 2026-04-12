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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.collect.ImmutableMap;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.iceberg.common.IcebergConfig;

/** Manages idempotent replay behavior for mutation requests that carry an Idempotency-Key. */
public class IcebergIdempotencyManager {

  public static final String IDEMPOTENCY_KEY_HEADER = "Idempotency-Key";
  public static final String IDEMPOTENCY_KEY_LIFETIME = "idempotency-key-lifetime";

  private static final Pattern UUID_V7_PATTERN =
      Pattern.compile(
          "^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-7[0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$");

  private static final long MAX_CACHE_ENTRIES = 10_000L;

  private final Cache<IdempotencyRequestKey, CachedResponse> responseCache;
  private final ConcurrentHashMap<IdempotencyRequestKey, Object> keyLocks;
  private final Duration idempotencyKeyLifetime;

  public IcebergIdempotencyManager(IcebergConfig icebergConfig) {
    this(Duration.ofMinutes(icebergConfig.get(IcebergConfig.IDEMPOTENCY_KEY_LIFETIME_MINUTES)));
  }

  IcebergIdempotencyManager(Duration idempotencyKeyLifetime) {
    if (idempotencyKeyLifetime == null
        || idempotencyKeyLifetime.isZero()
        || idempotencyKeyLifetime.isNegative()) {
      throw new IllegalArgumentException("Idempotency key lifetime must be positive");
    }

    this.idempotencyKeyLifetime = idempotencyKeyLifetime;
    this.keyLocks = new ConcurrentHashMap<>();
    this.responseCache =
        Caffeine.newBuilder()
            .maximumSize(MAX_CACHE_ENTRIES)
            .expireAfterWrite(idempotencyKeyLifetime)
            .build();
  }

  public Optional<String> getValidatedIdempotencyKey(String idempotencyKey) {
    if (StringUtils.isBlank(idempotencyKey)) {
      return Optional.empty();
    }

    String normalized = idempotencyKey.trim();
    if (!UUID_V7_PATTERN.matcher(normalized).matches()) {
      throw new IllegalArgumentException(
          "Invalid Idempotency-Key header. Expect UUIDv7 format for mutation endpoints.");
    }

    return Optional.of(normalized);
  }

  public Response replayOrExecute(
      String idempotencyKey, HttpServletRequest request, Supplier<Response> operation) {
    IdempotencyRequestKey cacheKey =
        new IdempotencyRequestKey(
            idempotencyKey,
            StringUtils.defaultString(request.getMethod()),
            StringUtils.defaultString(request.getRequestURI()),
            StringUtils.defaultString(request.getQueryString()));

    CachedResponse cachedResponse = responseCache.getIfPresent(cacheKey);
    if (cachedResponse != null) {
      return cachedResponse.toResponse();
    }

    Object keyLock = keyLocks.computeIfAbsent(cacheKey, k -> new Object());
    try {
      synchronized (keyLock) {
        cachedResponse = responseCache.getIfPresent(cacheKey);
        if (cachedResponse != null) {
          return cachedResponse.toResponse();
        }

        Response response = operation.get();
        if (response.getStatusInfo().getFamily() == Response.Status.Family.SUCCESSFUL) {
          responseCache.put(cacheKey, CachedResponse.from(response));
        }

        return response;
      }
    } finally {
      keyLocks.remove(cacheKey, keyLock);
    }
  }

  public String getIdempotencyKeyLifetime() {
    return idempotencyKeyLifetime.toString();
  }

  private static class IdempotencyRequestKey {
    private final String idempotencyKey;
    private final String method;
    private final String requestUri;
    private final String query;

    private IdempotencyRequestKey(
        String idempotencyKey, String method, String requestUri, String query) {
      this.idempotencyKey = idempotencyKey;
      this.method = method;
      this.requestUri = requestUri;
      this.query = normalizeQueryString(query);
    }

    private static String normalizeQueryString(String queryString) {
      if (StringUtils.isBlank(queryString)) {
        return "";
      }

      String[] queryPairs = queryString.split("&");
      Arrays.sort(queryPairs);
      return String.join("&", queryPairs);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof IdempotencyRequestKey)) {
        return false;
      }
      IdempotencyRequestKey that = (IdempotencyRequestKey) o;
      return idempotencyKey.equals(that.idempotencyKey)
          && method.equals(that.method)
          && requestUri.equals(that.requestUri)
          && query.equals(that.query);
    }

    @Override
    public int hashCode() {
      int result = idempotencyKey.hashCode();
      result = 31 * result + method.hashCode();
      result = 31 * result + requestUri.hashCode();
      result = 31 * result + query.hashCode();
      return result;
    }
  }

  private static class CachedResponse {
    private final int status;
    private final Object entity;
    private final MediaType mediaType;
    private final Map<String, List<Object>> headers;

    private CachedResponse(
        int status, Object entity, MediaType mediaType, Map<String, List<Object>> headers) {
      this.status = status;
      this.entity = entity;
      this.mediaType = mediaType;
      this.headers = headers;
    }

    private static CachedResponse from(Response response) {
      ImmutableMap.Builder<String, List<Object>> headersBuilder = ImmutableMap.builder();
      response
          .getHeaders()
          .forEach((headerName, values) -> headersBuilder.put(headerName, new ArrayList<>(values)));
      return new CachedResponse(
          response.getStatus(),
          response.getEntity(),
          response.getMediaType(),
          headersBuilder.build());
    }

    private Response toResponse() {
      Response.ResponseBuilder builder = Response.status(status);
      if (entity != null) {
        builder.entity(entity);
      }
      if (mediaType != null) {
        builder.type(mediaType);
      }

      headers.forEach(
          (headerName, values) -> {
            if (!HttpHeaders.CONTENT_TYPE.equalsIgnoreCase(headerName)) {
              values.forEach(value -> builder.header(headerName, value));
            }
          });
      return builder.build();
    }
  }
}
