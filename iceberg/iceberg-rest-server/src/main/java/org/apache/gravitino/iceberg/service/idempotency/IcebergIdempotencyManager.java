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
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.StringJoiner;
import java.util.TreeMap;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.iceberg.common.IcebergConfig;
import org.apache.gravitino.iceberg.service.IcebergExceptionMapper;
import org.apache.gravitino.iceberg.service.IcebergObjectMapper;
import org.apache.gravitino.iceberg.service.IcebergRESTUtils;
import org.apache.iceberg.exceptions.ServiceUnavailableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Applies {@code Idempotency-Key} semantics to Iceberg REST mutation endpoints.
 *
 * <p>Each mutation runs through {@link #replayOrExecute}, which claims the key, executes the
 * mutation, and stores its response. While that record remains available, retries carrying the same
 * key get the original answer back instead of a second mutation. Idempotency is off by default;
 * when it is off, or when a request carries no key, the mutation runs directly and nothing is
 * stored.
 *
 * <p>A replayed response carries the original status and body. Response headers are not stored, so
 * a replayed create or update does not carry the {@code ETag} the first response did; clients that
 * rely on it simply load the table again.
 *
 * <p>Request bodies and behavior-changing headers are not compared. Reusing a key on the same URL
 * with different body/header values replays the first response; clients must use a new key for a
 * different logical operation. The catalog mutation and response storage are not transactional:
 * eviction, expiry, a crash, or a post-commit exception can permit re-execution. If retaining a
 * terminal response fails, its reservation is kept until expiry to prevent an immediate retry from
 * re-executing the mutation.
 *
 * <p>Stores enforce deadlines and arrange their own cleanup. The in-memory store uses Caffeine for
 * reclamation; the JDBC store owns its purge scheduler.
 */
public class IcebergIdempotencyManager {

  private static final Logger LOG = LoggerFactory.getLogger(IcebergIdempotencyManager.class);

  /** The idempotency key header defined by the Iceberg REST spec. */
  public static final String IDEMPOTENCY_KEY = "Idempotency-Key";

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
    long expiresAtMs = System.currentTimeMillis() + keyLifetime.toMillis();
    try {
      if (!idempotencyStore.reserve(key, operationBinding, expiresAtMs)) {
        return replay(idempotencyStore, key, operationBinding);
      }
    } catch (RuntimeException e) {
      LOG.warn(
          "Failed to access idempotency state for key {} before executing the mutation", key, e);
      // A lost commit acknowledgement may leave a reservation. Do not release or execute it.
      return Response.fromResponse(
              IcebergExceptionMapper.toRESTResponse(
                  new ServiceUnavailableException(
                      "Idempotency store unavailable; this attempt did not execute the mutation. "
                          + "Retry with the same Idempotency-Key within its reuse window.")))
          .header(HttpHeaders.RETRY_AFTER, RETRY_AFTER_SECONDS)
          .build();
    }

    boolean release = true;
    try {
      Response response = action.get();
      int status = response.getStatus();
      // Retain terminal outcomes even if recording the response fails: the mutation may have
      // committed, so freeing its reservation would allow an immediate duplicate execution.
      if (status < Response.Status.INTERNAL_SERVER_ERROR.getStatusCode()
          && !NON_FINALIZABLE_STATUSES.contains(status)) {
        release = false;
        tryFinalize(idempotencyStore, key, response);
      }
      return response;
    } finally {
      if (release) {
        // Release only when the action throws or returns a retryable/auth response.
        idempotencyStore.release(key);
      }
    }
  }

  /**
   * Builds the request identity stored alongside a key. Retries of the same logical operation
   * produce the same binding, while a key reused for a different request does not.
   *
   * <p>The identity covers the method, encoded path, and sorted, encoded query parameters. Bodies
   * and behavior-changing headers are not compared; clients must use a fresh key for a different
   * logical operation.
   *
   * @param httpMethod the HTTP method of the endpoint
   * @param uriInfo the request URI, may be {@code null} outside a request context
   * @return the operation binding, for example {@code POST /v1/cat1/namespaces/ns1/tables}
   */
  public static String operationBinding(String httpMethod, @Nullable UriInfo uriInfo) {
    if (uriInfo == null) {
      return httpMethod;
    }
    StringBuilder binding =
        new StringBuilder(httpMethod).append(" /").append(uriInfo.getPath(false));
    MultivaluedMap<String, String> queryParameters = uriInfo.getQueryParameters();
    if (queryParameters != null && !queryParameters.isEmpty()) {
      // Sorted, so that clients that order query parameters differently between a request and its
      // retry still land on the same binding.
      StringJoiner query = new StringJoiner("&");
      for (Map.Entry<String, List<String>> parameter : new TreeMap<>(queryParameters).entrySet()) {
        for (String value : parameter.getValue().stream().sorted().collect(Collectors.toList())) {
          query.add(
              URLEncoder.encode(parameter.getKey(), StandardCharsets.UTF_8)
                  + "="
                  + URLEncoder.encode(value, StandardCharsets.UTF_8));
        }
      }
      binding.append('?').append(query);
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
              + " was already used for a different operation; use a new key",
          false);
    }

    LOG.info(
        "Replaying the stored response for Idempotency-Key: {}, http status: {}.",
        idempotencyKey,
        found.httpStatus());
    Response.ResponseBuilder builder = Response.status(found.httpStatus());
    if (found.responseSummary() != null) {
      builder.entity(found.responseSummary()).type(MediaType.APPLICATION_JSON);
    }
    return builder.build();
  }

  private void tryFinalize(
      IdempotencyStore idempotencyStore, String idempotencyKey, Response response) {
    String responseSummary;
    try {
      Object entity = response.getEntity();
      if (entity == null) {
        responseSummary = null;
      } else if (entity instanceof String) {
        responseSummary = (String) entity;
      } else {
        responseSummary = icebergObjectMapper.writeValueAsString(entity);
      }
      idempotencyStore.finalizeRecord(idempotencyKey, response.getStatus(), responseSummary);
      boolean finalized =
          idempotencyStore.load(idempotencyKey).filter(IdempotencyRecord::isFinalized).isPresent();
      if (!finalized) {
        LOG.warn(
            "Response for Idempotency-Key: {} was not retained; leaving any reservation until expiry",
            idempotencyKey);
      }
    } catch (JsonProcessingException | RuntimeException e) {
      LOG.warn(
          "Failed to retain the response for Idempotency-Key: {}; leaving any reservation until expiry",
          idempotencyKey,
          e);
    }
  }

  private static Response conflict(String message, boolean retryable) {
    Response response =
        Response.status(Response.Status.CONFLICT)
            .entity(
                IcebergRESTUtils.errorResponse(
                    Response.Status.CONFLICT.getStatusCode(),
                    "IdempotencyConflictException",
                    message))
            .type(MediaType.APPLICATION_JSON)
            .build();
    if (!retryable) {
      return response;
    }
    return Response.fromResponse(response)
        .header(HttpHeaders.RETRY_AFTER, RETRY_AFTER_SECONDS)
        .build();
  }
}
