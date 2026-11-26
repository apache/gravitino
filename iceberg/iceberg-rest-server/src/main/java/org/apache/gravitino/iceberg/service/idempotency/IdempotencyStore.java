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

import java.io.Closeable;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import javax.annotation.Nullable;

/**
 * Storage for {@code Idempotency-Key} records, pluggable so that a deployment can trade durability
 * for latency.
 *
 * <p>The server drives a store through a reserve-execute-finalize cycle: exactly one caller wins
 * {@link #reserve}, executes the mutation, and then either {@link #finalizeRecord finalizes} the
 * response for later replay or {@link #release releases} the reservation so the client can retry
 * with the same key. Implementations must make {@link #reserve} atomic across every caller that can
 * see the same storage, since that is the only thing standing between a retried request and a
 * duplicate mutation.
 *
 * <p>A reservation does not always survive until its owner finishes: a bounded store can drop it
 * under memory pressure, and a purge can remove it once it expires. The key is then free and
 * treated as never seen, so another caller can reserve it while the first is still running. Callers
 * pass the {@code claim} from {@link #reserve} into {@link #finalizeRecord} and {@link #release}.
 * Built-in stores use a fixed claim for every live row; after hard delete the same claim value may
 * be reused, matching common HTTP {@code Idempotency-Key} practice and the Iceberg REST reuse
 * window.
 *
 * <p>Keys are compared in the folded form returned by {@link IdempotencyKeys#canonicalize}, so
 * implementations must canonicalize before reading or writing storage rather than trusting the
 * caller to have done it.
 *
 * <p>Implementations must be thread-safe and are expected to have a public no-argument constructor
 * so they can be loaded by class name.
 */
public interface IdempotencyStore extends Closeable {

  /**
   * Initializes the store from the Iceberg REST server configuration.
   *
   * @param properties the full Iceberg REST server configuration
   */
  void initialize(Map<String, String> properties);

  /**
   * Atomically attempts to claim a key. An expired record is treated as absent, so the key can be
   * reserved again without waiting for a purge.
   *
   * <p>The store retains the operation binding but does not compare it when a reservation already
   * exists. The REST handler owns binding checks: when {@link #load} returns a finalized record,
   * the handler must reject a different binding with {@code 409 Conflict} instead of replaying it.
   * When the key is still in flight, {@code load} returns empty and the handler returns the same
   * in-flight {@code 409 Conflict}, regardless of whether the bindings match.
   *
   * @param idempotencyKey the client-provided UUIDv7 key
   * @param operationBinding request identity, as the HTTP method, a space, and the encoded path. A
   *     query fingerprint may follow the path, separated by a question mark.
   * @param expiresAtMs time after which the record may be purged, in unix epoch millis
   * @return the reservation claim when acquired, or empty if the key is live. Callers must pass the
   *     returned claim to {@link #finalizeRecord} or {@link #release}. Built-in stores may reuse
   *     the same claim value after release, eviction, or purge.
   */
  OptionalLong reserve(String idempotencyKey, String operationBinding, long expiresAtMs);

  /**
   * Loads a finalized record for replay.
   *
   * @param idempotencyKey the client-provided key
   * @return the record, or {@link Optional#empty()} if the key is unknown, expired, or still
   *     reserved by an in-flight request
   */
  Optional<IdempotencyRecord> load(String idempotencyKey);

  /**
   * Marks a reserved key finalized, storing the response replayed to later retries.
   *
   * <p>The call is a no-op when the record has already expired or been purged, so it is not
   * resurrected, and when the stored claim differs. Built-in stores reuse a fixed claim after hard
   * delete, so a late finalize can update a newer reservation for the same key.
   *
   * @param idempotencyKey the client-provided key
   * @param claim the claim this caller reserved with
   * @param httpStatus HTTP status of the original response
   * @param responseSummary serialized response body, {@code null} for responses without a body
   */
  void finalizeRecord(
      String idempotencyKey, long claim, int httpStatus, @Nullable String responseSummary);

  /**
   * Releases a reservation so the client can retry with the same key. Called when the mutation
   * failed in a way the Iceberg REST spec treats as retryable, which is any {@code 5xx} response.
   *
   * <p>The call is a no-op when the stored claim differs. Built-in stores reuse a fixed claim after
   * hard delete, so a late release can free a newer reservation for the same key.
   *
   * @param idempotencyKey the client-provided key
   * @param claim the claim this caller reserved with
   */
  void release(String idempotencyKey, long claim);

  /**
   * Purges records whose reuse window has elapsed.
   *
   * @param beforeMs cutoff in unix epoch millis; records expiring at or before this time are
   *     removed
   * @return the number of records purged
   */
  int purgeExpired(long beforeMs);
}
