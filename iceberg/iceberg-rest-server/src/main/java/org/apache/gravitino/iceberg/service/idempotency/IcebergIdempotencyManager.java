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
package org.apache.gravitino.iceberg.service.idempotency;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.StringJoiner;
import java.util.TreeMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.iceberg.common.IcebergConfig;
import org.apache.gravitino.iceberg.common.idempotency.IdempotencyKeys;
import org.apache.gravitino.iceberg.common.idempotency.IdempotencyRecord;
import org.apache.gravitino.iceberg.common.idempotency.IdempotencyStore;
import org.apache.gravitino.iceberg.common.idempotency.IdempotencyStore.ReserveResult;
import org.apache.gravitino.iceberg.common.idempotency.IdempotencyStoreFactory;
import org.apache.gravitino.iceberg.service.IcebergExceptionMapper;
import org.apache.gravitino.iceberg.service.IcebergObjectMapper;
import org.apache.gravitino.iceberg.service.IcebergRESTUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Applies {@code Idempotency-Key} semantics to Iceberg REST mutation endpoints.
 *
 * <p>Each mutation runs through {@link #replayOrExecute}, which claims the key, executes the
 * mutation exactly once, and stores its response so that retries carrying the same key get the
 * original answer back instead of a second mutation. Idempotency is off by default; when it is off,
 * or when a request carries no key, the mutation runs directly and nothing is stored.
 *
 * <p>A replayed response carries the original status and body. Response headers are not stored, so
 * a replayed create or update does not carry the {@code ETag} the first response did; clients that
 * rely on it simply load the table again.
 */
public class IcebergIdempotencyManager {

  private static final Logger LOG = LoggerFactory.getLogger(IcebergIdempotencyManager.class);

  /** The idempotency key header defined by the Iceberg REST spec. */
  public static final String IDEMPOTENCY_KEY = "Idempotency-Key";

  /** Cap on the stored operation binding, matching the width a database backed store can hold. */
  @VisibleForTesting static final int MAX_OPERATION_BINDING_LENGTH = 512;

  /** Seconds a client should wait before retrying a request whose key is still in flight. */
  private static final int RETRY_AFTER_SECONDS = 1;

  /**
   * Statuses that are terminal for the request but not for the key. Authentication and
   * authorization outcomes depend on the caller's credentials rather than on catalog state, so
   * replaying them would keep failing a retry that has since obtained valid credentials.
   */
  private static final Set<Integer> NON_FINALIZABLE_STATUSES = ImmutableSet.of(401, 403, 419);

  private final boolean enabled;
  private final Duration keyLifetime;
  private final ObjectMapper icebergObjectMapper;
  private final Optional<IdempotencyStore> store;

  /**
   * Creates a manager, loading the configured store when idempotency is enabled.
   *
   * @param icebergConfig the Iceberg REST server configuration
   */
  public IcebergIdempotencyManager(IcebergConfig icebergConfig) {
    this(
        icebergConfig,
        icebergConfig.get(IcebergConfig.ICEBERG_IDEMPOTENCY_ENABLED)
            ? Optional.of(IdempotencyStoreFactory.create(icebergConfig))
            : Optional.empty());
  }

  @VisibleForTesting
  IcebergIdempotencyManager(IcebergConfig icebergConfig, Optional<IdempotencyStore> store) {
    this.enabled = icebergConfig.get(IcebergConfig.ICEBERG_IDEMPOTENCY_ENABLED);
    this.keyLifetime =
        Duration.parse(icebergConfig.get(IcebergConfig.ICEBERG_IDEMPOTENCY_KEY_LIFETIME));
    this.icebergObjectMapper = IcebergObjectMapper.getInstance();
    this.store = store;
  }

  /**
   * Returns the key reuse window to advertise to clients.
   *
   * @return the lifetime as an ISO-8601 duration, or {@link Optional#empty()} when idempotency is
   *     disabled. The Iceberg REST spec reads the absence of {@code idempotency-key-lifetime} in
   *     the config response as "this server does not support idempotency".
   */
  public Optional<String> advertisedKeyLifetime() {
    return enabled ? Optional.of(keyLifetime.toString()) : Optional.empty();
  }

  /**
   * Runs a mutation under the given idempotency key.
   *
   * <p>Without a key, or with idempotency disabled, this just runs {@code action}. With a key, the
   * first request claims it and stores the response; retries carrying the same key replay that
   * response. A retry that arrives while the first request is still running, or that reuses a key
   * already spent on a different operation, gets {@code 409 Conflict}.
   *
   * @param idempotencyKey the raw {@code Idempotency-Key} header value, may be {@code null}
   * @param operationBinding request identity, see {@link #operationBinding(String, UriInfo)}
   * @param action the mutation, which is expected to return its own error responses rather than
   *     throw
   * @return the response to send to the client
   */
  public Response replayOrExecute(
      @Nullable String idempotencyKey, String operationBinding, Supplier<Response> action) {
    if (!enabled || StringUtils.isBlank(idempotencyKey) || !store.isPresent()) {
      return action.get();
    }

    String key;
    try {
      key = IdempotencyKeys.canonicalize(idempotencyKey);
    } catch (IllegalArgumentException e) {
      return IcebergExceptionMapper.toRESTResponse(e);
    }

    IdempotencyStore idempotencyStore = store.get();
    String binding = truncate(operationBinding);
    long expiresAtMs = System.currentTimeMillis() + keyLifetime.toMillis();
    // A fresh claim per attempt, so that if this reservation is dropped underneath us and another
    // request takes the key, our finalize or release lands on nothing instead of on its record.
    long claim = ThreadLocalRandom.current().nextLong();
    if (idempotencyStore.reserve(key, binding, claim, expiresAtMs) == ReserveResult.DUPLICATE) {
      return replay(idempotencyStore, key, binding);
    }

    boolean finalized = false;
    try {
      Response response = action.get();
      if (isFinalizable(response.getStatus())) {
        finalized = tryFinalize(idempotencyStore, key, claim, response);
      }
      return response;
    } finally {
      if (!finalized) {
        // The mutation failed in a way the spec treats as retryable, or its response could not be
        // stored. Either way the key must go back so the client can retry with it.
        idempotencyStore.release(key, claim);
      }
    }
  }

  /**
   * Builds the request identity stored alongside a key. Retries of the same logical operation
   * produce the same binding, while a key reused for a different request does not.
   *
   * <p>The identity covers the method, path, and query parameters, not the request body. A client
   * that reuses one key for two different bodies on the same endpoint gets the first response
   * replayed, which is exactly what the header promises; the spec already requires a fresh key per
   * operation.
   *
   * @param httpMethod the HTTP method of the endpoint
   * @param uriInfo the request URI, may be {@code null} outside a request context
   * @return the operation binding, for example {@code POST v1/cat1/namespaces/ns1/tables}
   */
  public static String operationBinding(String httpMethod, @Nullable UriInfo uriInfo) {
    if (uriInfo == null) {
      return httpMethod;
    }
    StringBuilder binding = new StringBuilder(httpMethod).append(' ').append(uriInfo.getPath());
    MultivaluedMap<String, String> queryParameters = uriInfo.getQueryParameters();
    if (queryParameters != null && !queryParameters.isEmpty()) {
      // Sorted, so that clients that order query parameters differently between a request and its
      // retry still land on the same binding.
      StringJoiner query = new StringJoiner("&", "?", "");
      for (Map.Entry<String, List<String>> parameter : new TreeMap<>(queryParameters).entrySet()) {
        for (String value : parameter.getValue()) {
          query.add(parameter.getKey() + "=" + value);
        }
      }
      binding.append(query);
    }
    return binding.toString();
  }

  /** Closes the underlying store. */
  public void close() {
    store.ifPresent(
        idempotencyStore -> {
          try {
            idempotencyStore.close();
          } catch (IOException e) {
            LOG.warn("Close Iceberg idempotency store failed.", e);
          }
        });
  }

  private Response replay(
      IdempotencyStore idempotencyStore, String idempotencyKey, String binding) {
    Optional<IdempotencyRecord> record = idempotencyStore.load(idempotencyKey);
    if (!record.isPresent()) {
      // Reserved but not finalized: the first request is still running, here or on another node.
      return conflict(
          "Idempotency-Key: "
              + idempotencyKey
              + " is being processed by another request, retry later",
          true);
    }

    IdempotencyRecord found = record.get();
    if (!found.operationBinding().equals(binding)) {
      // Replaying here would hand the client the response of an unrelated operation.
      return conflict(
          "Idempotency-Key: "
              + idempotencyKey
              + " was already used for `"
              + found.operationBinding()
              + "`, the Iceberg REST spec requires a new key for a different operation",
          false);
    }

    LOG.info(
        "Replaying the stored response for Idempotency-Key: {}, operation: {}, http status: {}.",
        idempotencyKey,
        binding,
        found.httpStatus());
    Response.ResponseBuilder builder = Response.status(found.httpStatus());
    if (found.responseSummary() != null) {
      builder.entity(found.responseSummary()).type(MediaType.APPLICATION_JSON);
    }
    return builder.build();
  }

  private boolean tryFinalize(
      IdempotencyStore idempotencyStore, String idempotencyKey, long claim, Response response) {
    String responseSummary;
    try {
      responseSummary = serializeEntity(response.getEntity());
    } catch (JsonProcessingException e) {
      LOG.warn(
          "Failed to serialize the response for Idempotency-Key: {}, it will not be replayed.",
          idempotencyKey,
          e);
      return false;
    }
    idempotencyStore.finalizeRecord(idempotencyKey, claim, response.getStatus(), responseSummary);
    return true;
  }

  @Nullable
  private String serializeEntity(@Nullable Object entity) throws JsonProcessingException {
    if (entity == null) {
      return null;
    }
    if (entity instanceof String) {
      return (String) entity;
    }
    return icebergObjectMapper.writeValueAsString(entity);
  }

  private static Response conflict(String message, boolean retryable) {
    Response response =
        IcebergRESTUtils.errorResponse(
            new IdempotencyConflictException(message), Response.Status.CONFLICT.getStatusCode());
    if (!retryable) {
      return response;
    }
    return Response.fromResponse(response)
        .header(HttpHeaders.RETRY_AFTER, RETRY_AFTER_SECONDS)
        .build();
  }

  /**
   * Returns whether a response finalizes the key. Per the Iceberg REST spec, {@code 5xx} responses
   * are retryable and must not be stored, while {@code 2xx} and deterministic terminal {@code 4xx}
   * responses are replayed.
   */
  private static boolean isFinalizable(int httpStatus) {
    return httpStatus < Response.Status.INTERNAL_SERVER_ERROR.getStatusCode()
        && !NON_FINALIZABLE_STATUSES.contains(httpStatus);
  }

  private static String truncate(String operationBinding) {
    return operationBinding.length() <= MAX_OPERATION_BINDING_LENGTH
        ? operationBinding
        : operationBinding.substring(0, MAX_OPERATION_BINDING_LENGTH);
  }
}
