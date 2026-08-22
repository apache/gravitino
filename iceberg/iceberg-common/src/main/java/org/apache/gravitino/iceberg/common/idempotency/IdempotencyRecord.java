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
package org.apache.gravitino.iceberg.common.idempotency;

import com.google.common.base.Preconditions;
import java.util.Objects;
import javax.annotation.Nullable;

/**
 * One stored idempotency record: the reservation for an in-flight mutation, and after finalization
 * the response replayed to retries carrying the same key.
 *
 * <p>Instances are immutable; {@link #withResponse(int, String)} returns a new record rather than
 * mutating this one. Timestamps are unix epoch milliseconds so that a database backed store can map
 * them onto the {@code BIGINT} columns used by Gravitino's other metadata tables.
 */
public final class IdempotencyRecord {

  private final String idempotencyKey;
  private final String operationBinding;
  @Nullable private final Integer httpStatus;
  @Nullable private final String responseSummary;
  private final long createdAtMs;
  private final long expiresAtMs;

  private IdempotencyRecord(
      String idempotencyKey,
      String operationBinding,
      @Nullable Integer httpStatus,
      @Nullable String responseSummary,
      long createdAtMs,
      long expiresAtMs) {
    this.idempotencyKey = idempotencyKey;
    this.operationBinding = operationBinding;
    this.httpStatus = httpStatus;
    this.responseSummary = responseSummary;
    this.createdAtMs = createdAtMs;
    this.expiresAtMs = expiresAtMs;
  }

  /**
   * Creates a reserved (not yet finalized) record.
   *
   * @param idempotencyKey the client-provided UUIDv7 key
   * @param operationBinding request identity, for example {@code POST
   *     /v1/cat1/namespaces/ns1/tables}
   * @param createdAtMs reservation time in unix epoch millis
   * @param expiresAtMs time after which the record may be purged, in unix epoch millis
   * @return a record in the reserved state
   */
  public static IdempotencyRecord reserved(
      String idempotencyKey, String operationBinding, long createdAtMs, long expiresAtMs) {
    Preconditions.checkArgument(idempotencyKey != null, "idempotencyKey should not be null");
    Preconditions.checkArgument(operationBinding != null, "operationBinding should not be null");
    return new IdempotencyRecord(
        idempotencyKey, operationBinding, null, null, createdAtMs, expiresAtMs);
  }

  /**
   * Returns a copy of this record in the finalized state, carrying the response to replay.
   *
   * @param status HTTP status of the original response
   * @param summary serialized response body, {@code null} for responses without a body
   * @return a new finalized record
   */
  public IdempotencyRecord withResponse(int status, @Nullable String summary) {
    return new IdempotencyRecord(
        idempotencyKey, operationBinding, status, summary, createdAtMs, expiresAtMs);
  }

  /**
   * Returns the client-provided idempotency key.
   *
   * @return the UUIDv7 key in canonical string form
   */
  public String idempotencyKey() {
    return idempotencyKey;
  }

  /**
   * Returns the request identity this key was first used for. Retries that present the same key for
   * a different operation violate the Iceberg REST spec and are rejected instead of replayed.
   *
   * @return the operation binding
   */
  public String operationBinding() {
    return operationBinding;
  }

  /**
   * Returns the HTTP status of the finalized response.
   *
   * @return the status code, or {@code null} while the record is still reserved
   */
  @Nullable
  public Integer httpStatus() {
    return httpStatus;
  }

  /**
   * Returns the serialized response body replayed to retries.
   *
   * @return the response body, or {@code null} while the record is reserved or for responses
   *     without a body
   */
  @Nullable
  public String responseSummary() {
    return responseSummary;
  }

  /**
   * Returns when the key was reserved.
   *
   * @return reservation time in unix epoch millis
   */
  public long createdAtMs() {
    return createdAtMs;
  }

  /**
   * Returns when the record becomes eligible for purging.
   *
   * @return expiry time in unix epoch millis
   */
  public long expiresAtMs() {
    return expiresAtMs;
  }

  /**
   * Returns whether the mutation finished and its response can be replayed.
   *
   * @return {@code true} once the record carries a response
   */
  public boolean isFinalized() {
    return httpStatus != null;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof IdempotencyRecord)) {
      return false;
    }
    IdempotencyRecord that = (IdempotencyRecord) other;
    return createdAtMs == that.createdAtMs
        && expiresAtMs == that.expiresAtMs
        && Objects.equals(idempotencyKey, that.idempotencyKey)
        && Objects.equals(operationBinding, that.operationBinding)
        && Objects.equals(httpStatus, that.httpStatus)
        && Objects.equals(responseSummary, that.responseSummary);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        idempotencyKey, operationBinding, httpStatus, responseSummary, createdAtMs, expiresAtMs);
  }

  @Override
  public String toString() {
    return "IdempotencyRecord{idempotencyKey="
        + idempotencyKey
        + ", operationBinding="
        + operationBinding
        + ", httpStatus="
        + httpStatus
        + ", createdAtMs="
        + createdAtMs
        + ", expiresAtMs="
        + expiresAtMs
        + "}";
  }
}
