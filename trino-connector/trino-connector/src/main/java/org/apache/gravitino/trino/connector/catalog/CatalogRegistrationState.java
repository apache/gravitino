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
package org.apache.gravitino.trino.connector.catalog;

import com.google.common.base.Preconditions;
import javax.annotation.Nullable;
import org.apache.gravitino.trino.connector.metadata.GravitinoCatalog;

/**
 * The registration state of a single Apache Gravitino catalog in Trino.
 *
 * <p>Instances are immutable and are always replaced as a whole, so a reader never observes a
 * partially updated state.
 */
public final class CatalogRegistrationState {

  /** The registration status of a catalog. */
  public enum Status {
    /** The catalog was registered in Trino with a CREATE CATALOG statement. */
    REGISTERED,
    /** The last registration attempt failed. */
    FAILED,
    /** The catalog matches `gravitino.trino.skip-catalog-patterns` and is not registered. */
    SKIPPED,
    /** The catalog type or provider is not supported by the connector. */
    UNSUPPORTED
  }

  private final String metalake;
  private final String catalogName;
  private final String trinoCatalogName;
  private final String provider;
  private final Status status;
  private final String lastError;
  private final long lastAttemptTimeMs;
  private final long lastSuccessTimeMs;
  private final long failureCount;

  private CatalogRegistrationState(
      String metalake,
      String catalogName,
      String trinoCatalogName,
      String provider,
      Status status,
      String lastError,
      long lastAttemptTimeMs,
      long lastSuccessTimeMs,
      long failureCount) {
    this.metalake = metalake;
    this.catalogName = catalogName;
    this.trinoCatalogName = trinoCatalogName;
    this.provider = provider;
    this.status = status;
    this.lastError = lastError;
    this.lastAttemptTimeMs = lastAttemptTimeMs;
    this.lastSuccessTimeMs = lastSuccessTimeMs;
    this.failureCount = failureCount;
  }

  /**
   * Creates a state for a catalog that was registered in Trino successfully.
   *
   * @param catalog the Gravitino catalog
   * @param trinoCatalogName the name the catalog is registered under in Trino
   * @return the registration state
   */
  public static CatalogRegistrationState succeeded(
      GravitinoCatalog catalog, String trinoCatalogName) {
    return of(
        catalog.getMetalake(),
        catalog.getName(),
        trinoCatalogName,
        catalog.getProvider(),
        Status.REGISTERED,
        null);
  }

  /**
   * Creates a state for a catalog whose registration attempt failed.
   *
   * @param metalake the name of the metalake the catalog belongs to
   * @param catalogName the name of the catalog in Gravitino
   * @param trinoCatalogName the name the catalog would be registered under in Trino
   * @param provider the catalog provider, null if it could not be determined
   * @param error the error that prevented the registration
   * @return the registration state
   */
  public static CatalogRegistrationState failed(
      String metalake,
      String catalogName,
      String trinoCatalogName,
      @Nullable String provider,
      String error) {
    return of(metalake, catalogName, trinoCatalogName, provider, Status.FAILED, error);
  }

  /**
   * Creates a state for a catalog that is deliberately not registered because it matches {@code
   * gravitino.trino.skip-catalog-patterns}.
   *
   * @param metalake the name of the metalake the catalog belongs to
   * @param catalogName the name of the catalog in Gravitino
   * @param trinoCatalogName the name the catalog would be registered under in Trino
   * @param reason a human readable explanation of why the catalog is skipped
   * @return the registration state
   */
  public static CatalogRegistrationState skipped(
      String metalake, String catalogName, String trinoCatalogName, String reason) {
    return of(metalake, catalogName, trinoCatalogName, null, Status.SKIPPED, reason);
  }

  /**
   * Creates a state for a catalog the connector cannot register because of its type or provider.
   *
   * @param metalake the name of the metalake the catalog belongs to
   * @param catalogName the name of the catalog in Gravitino
   * @param trinoCatalogName the name the catalog would be registered under in Trino
   * @param provider the catalog provider, null if it could not be determined
   * @param reason a human readable explanation of why the catalog is not supported
   * @return the registration state
   */
  public static CatalogRegistrationState unsupported(
      String metalake,
      String catalogName,
      String trinoCatalogName,
      @Nullable String provider,
      String reason) {
    return of(metalake, catalogName, trinoCatalogName, provider, Status.UNSUPPORTED, reason);
  }

  // Shared by the factory methods above: they differ only in status and a couple of fields, and
  // funneling them through one constructor call keeps a newly added field from being forgotten in
  // one of several near-identical call sites.
  private static CatalogRegistrationState of(
      String metalake,
      String catalogName,
      String trinoCatalogName,
      @Nullable String provider,
      Status status,
      @Nullable String lastError) {
    Preconditions.checkArgument(metalake != null, "metalake must not be null");
    Preconditions.checkArgument(catalogName != null, "catalogName must not be null");
    Preconditions.checkArgument(trinoCatalogName != null, "trinoCatalogName must not be null");
    Preconditions.checkArgument(
        status == Status.REGISTERED || lastError != null,
        "lastError must not be null for status %s",
        status);
    long now = System.currentTimeMillis();
    return new CatalogRegistrationState(
        metalake,
        catalogName,
        trinoCatalogName,
        provider,
        status,
        lastError,
        now,
        status == Status.REGISTERED ? now : 0,
        status == Status.FAILED ? 1 : 0);
  }

  /**
   * Returns this state with the history carried over from the state it replaces. A catalog keeps
   * the time it was last registered even after it starts failing, and consecutive failures are
   * counted across attempts.
   *
   * @param previous the state being replaced, null if this catalog was never seen before
   * @return this state if there is no history to carry over, a new state carrying it otherwise
   */
  CatalogRegistrationState withHistoryOf(@Nullable CatalogRegistrationState previous) {
    if (previous == null) {
      return this;
    }
    // A registered catalog stamps its own success time; every other status keeps the last one.
    long successTime = status == Status.REGISTERED ? lastSuccessTimeMs : previous.lastSuccessTimeMs;
    // Consecutive failures only accumulate while the catalog keeps failing. Any other status
    // interrupts the run, and its own count is already zero.
    long failures = status == Status.FAILED ? previous.failureCount + 1 : failureCount;
    // A failure that happens before the provider is known (e.g. metalake.loadCatalog() itself
    // throws) must not blank out a provider a previous attempt already discovered.
    String effectiveProvider = provider != null ? provider : previous.provider;
    if (successTime == lastSuccessTimeMs
        && failures == failureCount
        && effectiveProvider == provider) {
      return this;
    }
    return new CatalogRegistrationState(
        metalake,
        catalogName,
        trinoCatalogName,
        effectiveProvider,
        status,
        lastError,
        lastAttemptTimeMs,
        successTime,
        failures);
  }

  /**
   * Retrieves the name of the metalake the catalog belongs to.
   *
   * @return the metalake name
   */
  public String getMetalake() {
    return metalake;
  }

  /**
   * Retrieves the name of the catalog in Gravitino.
   *
   * @return the catalog name
   */
  public String getCatalogName() {
    return catalogName;
  }

  /**
   * Retrieves the name the catalog is registered under in Trino.
   *
   * @return the Trino catalog name
   */
  public String getTrinoCatalogName() {
    return trinoCatalogName;
  }

  /**
   * Retrieves the catalog provider.
   *
   * @return the provider, null if it could not be determined
   */
  @Nullable
  public String getProvider() {
    return provider;
  }

  /**
   * Retrieves the registration status of the catalog.
   *
   * @return the status
   */
  public Status getStatus() {
    return status;
  }

  /**
   * Retrieves the error or the reason why the catalog is not registered.
   *
   * @return the message, null if the catalog is registered
   */
  @Nullable
  public String getLastError() {
    return lastError;
  }

  /**
   * Retrieves the time of the last registration attempt.
   *
   * @return the time in milliseconds since the epoch
   */
  public long getLastAttemptTimeMs() {
    return lastAttemptTimeMs;
  }

  /**
   * Retrieves the time of the last successful registration.
   *
   * @return the time in milliseconds since the epoch, 0 if the catalog was never registered
   */
  public long getLastSuccessTimeMs() {
    return lastSuccessTimeMs;
  }

  /**
   * Retrieves the number of consecutive failed registration attempts.
   *
   * @return the failure count, 0 if the last attempt succeeded
   */
  public long getFailureCount() {
    return failureCount;
  }

  @Override
  public String toString() {
    return String.format(
        "CatalogRegistrationState{metalake=%s, catalog=%s, trinoCatalog=%s, provider=%s,"
            + " status=%s, lastError=%s, failureCount=%d}",
        metalake, catalogName, trinoCatalogName, provider, status, lastError, failureCount);
  }
}
