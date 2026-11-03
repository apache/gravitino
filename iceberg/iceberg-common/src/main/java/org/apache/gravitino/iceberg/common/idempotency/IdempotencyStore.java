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

import java.io.Closeable;
import java.util.Map;
import java.util.Optional;
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
 * under memory pressure, and a purge can remove it once it expires. The key is then free, and
 * another caller can reserve it while the first is still running. Every call that mutates an
 * existing record therefore carries the {@code claim} its caller reserved with, and implementations
 * must ignore the call when the stored claim differs, so a caller that has lost its reservation
 * cannot finalize or release a record that now belongs to someone else.
 *
 * <p>Keys are compared in the folded form returned by {@link IdempotencyKeys#canonicalize}, so
 * implementations must canonicalize before reading or writing storage rather than trusting the
 * caller to have done it.
 *
 * <p>Implementations must be thread-safe and are expected to have a public no-argument constructor
 * so they can be loaded by class name.
 */
public interface IdempotencyStore extends Closeable {

  /** Outcome of an attempt to claim a key. */
  enum ReserveResult {
    /** The key was newly claimed by this caller, which now owns the mutation. */
    RESERVED,
    /** The key is already reserved or finalized, so the request is a retry. */
    DUPLICATE
  }

  /**
   * Initializes the store from the Iceberg REST server configuration.
   *
   * @param properties the full Iceberg REST server configuration
   */
  void initialize(Map<String, String> properties);

  /**
   * Atomically attempts to claim a key.
   *
   * @param idempotencyKey the client-provided UUIDv7 key
   * @param operationBinding request identity, for example {@code POST
   *     /v1/cat1/namespaces/ns1/tables}
   * @param claim a fencing token identifying this attempt, which the caller must generate afresh
   *     for every reservation and pass back to {@link #finalizeRecord} and {@link #release}
   * @param expiresAtMs time after which the record may be purged, in unix epoch millis
   * @return {@link ReserveResult#RESERVED} if newly claimed, {@link ReserveResult#DUPLICATE} if a
   *     record for this key already exists
   */
  ReserveResult reserve(
      String idempotencyKey, String operationBinding, long claim, long expiresAtMs);

  /**
   * Loads a finalized record for replay.
   *
   * @param idempotencyKey the client-provided key
   * @return the record, or {@link Optional#empty()} if the key is unknown or still reserved by an
   *     in-flight request
   */
  Optional<IdempotencyRecord> load(String idempotencyKey);

  /**
   * Marks a reserved key finalized, storing the response replayed to later retries.
   *
   * <p>The call is a no-op when the record has already expired or been purged, so it is not
   * resurrected, and when the stored claim differs, so a caller that has lost its reservation
   * cannot overwrite the record of whoever holds the key now.
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
   * <p>The call is a no-op when the stored claim differs, so a caller that has lost its reservation
   * cannot free a key another caller is currently executing under.
   *
   * @param idempotencyKey the client-provided key
   * @param claim the claim this caller reserved with
   */
  void release(String idempotencyKey, long claim);

  /**
   * Purges records whose reuse window has elapsed.
   *
   * @param beforeMs cutoff in unix epoch millis; records expiring before this time are removed
   * @return the number of records purged
   */
  int purgeExpired(long beforeMs);
}
