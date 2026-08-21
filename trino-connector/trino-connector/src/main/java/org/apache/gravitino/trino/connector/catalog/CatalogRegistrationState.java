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
    long now = System.currentTimeMillis();
    return new CatalogRegistrationState(
        catalog.getMetalake(),
        catalog.getName(),
        trinoCatalogName,
        catalog.getProvider(),
        Status.REGISTERED,
        null,
        now,
        now,
        0);
  }

  /**
   * Creates a state for a catalog whose registration attempt failed.
   *
   * @param metalake the name of the metalake the catalog belongs to
   * @param catalogName the name of the catalog in Gravitino
   * @param trinoCatalogName the name the catalog would be registered under in Trino
   * @param provider the catalog provider, null if it could not be determined
   * @param error the error that prevented the registration
   * @param previous the previous state of the catalog, null if the catalog was never seen before
   * @return the registration state
   */
  public static CatalogRegistrationState failed(
      String metalake,
      String catalogName,
      String trinoCatalogName,
      @Nullable String provider,
      String error,
      @Nullable CatalogRegistrationState previous) {
    return new CatalogRegistrationState(
        metalake,
        catalogName,
        trinoCatalogName,
        provider,
        Status.FAILED,
        error,
        System.currentTimeMillis(),
        previous == null ? 0 : previous.lastSuccessTimeMs,
        previous == null ? 1 : previous.failureCount + 1);
  }

  /**
   * Creates a state for a catalog that is intentionally not registered in Trino.
   *
   * @param metalake the name of the metalake the catalog belongs to
   * @param catalogName the name of the catalog in Gravitino
   * @param trinoCatalogName the name the catalog would be registered under in Trino
   * @param provider the catalog provider, null if it could not be determined
   * @param status the reason category, either {@link Status#SKIPPED} or {@link Status#UNSUPPORTED}
   * @param reason a human readable explanation of why the catalog is not registered
   * @return the registration state
   */
  public static CatalogRegistrationState notLoaded(
      String metalake,
      String catalogName,
      String trinoCatalogName,
      @Nullable String provider,
      Status status,
      String reason) {
    return new CatalogRegistrationState(
        metalake,
        catalogName,
        trinoCatalogName,
        provider,
        status,
        reason,
        System.currentTimeMillis(),
        0,
        0);
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
