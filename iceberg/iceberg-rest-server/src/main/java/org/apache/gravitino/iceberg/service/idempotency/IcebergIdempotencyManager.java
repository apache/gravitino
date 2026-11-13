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
import com.google.common.hash.Hashing;
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
import java.util.concurrent.ThreadLocalRandom;
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
 * eviction, a crash, serialization failure, or a post-commit exception can permit re-execution.
 *
 * <p>This phase does not schedule {@link IdempotencyStore#purgeExpired}. The in-memory store uses
 * Caffeine for reclamation. Custom stores must enforce deadlines and arrange their own cleanup;
 * scheduling durable cleanup belongs to the JDBC-store integration.
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
    String binding =
        Hashing.sha256().hashString(operationBinding, StandardCharsets.UTF_8).toString();
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
      int status = response.getStatus();
      // Store successful and terminal client responses. Release 5xx and auth outcomes so retries
      // can re-evaluate them; serialization/finalization failure also releases only our claim.
      if (status < Response.Status.INTERNAL_SERVER_ERROR.getStatusCode()
          && !NON_FINALIZABLE_STATUSES.contains(status)) {
        finalized = tryFinalize(idempotencyStore, key, claim, response);
      }
      return response;
    } finally {
      if (!finalized) {
        // Only release this attempt's claim when the response is retryable or was not retained.
        // A replacement owner's reservation must remain untouched.
        idempotencyStore.release(key, claim);
      }
    }
  }

  /**
   * Builds the request identity stored alongside a key. Retries of the same logical operation
   * produce the same binding, while a key reused for a different request does not.
   *
   * <p>The identity covers the method, encoded path, and sorted, encoded query parameters. Bodies
   * and behavior-changing headers are not compared; clients must use a fresh key for a different
   * logical operation. The manager stores a SHA-256 digest of the complete identity to fit the
   * store's bounded binding column without truncating distinct requests.
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
      StringJoiner query = new StringJoiner("&", "?", "");
      for (Map.Entry<String, List<String>> parameter : new TreeMap<>(queryParameters).entrySet()) {
        for (String value : parameter.getValue().stream().sorted().collect(Collectors.toList())) {
          query.add(
              URLEncoder.encode(parameter.getKey(), StandardCharsets.UTF_8)
                  + "="
                  + URLEncoder.encode(value, StandardCharsets.UTF_8));
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
              + " was already used for a different operation; use a new key",
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
      Object entity = response.getEntity();
      if (entity == null) {
        responseSummary = null;
      } else if (entity instanceof String) {
        responseSummary = (String) entity;
      } else {
        responseSummary = icebergObjectMapper.writeValueAsString(entity);
      }
    } catch (JsonProcessingException e) {
      LOG.warn(
          "Failed to serialize the response for Idempotency-Key: {}, it will not be replayed.",
          idempotencyKey,
          e);
      return false;
    }
    idempotencyStore.finalizeRecord(idempotencyKey, claim, response.getStatus(), responseSummary);
    boolean finalized =
        idempotencyStore
            .load(idempotencyKey)
            .filter(record -> record.isFinalized() && record.claim() == claim)
            .isPresent();
    if (!finalized) {
      LOG.warn(
          "Response for Idempotency-Key: {} was not retained under this claim", idempotencyKey);
    }
    // Keep the actual mutation response: returning a synthetic 5xx could encourage clients to
    // retry an operation that already committed. Release is fenced to our claim in the caller.
    return finalized;
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
