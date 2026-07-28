/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.iceberg.service.deletion;

/** Typed, non-secret failure from the Iceberg table deletion lifecycle. */
public class IcebergDeletionException extends RuntimeException {

  private static final String NOT_FOUND_MESSAGE = "Deleted table is not available";

  /** Stable API outcome for a deletion-lifecycle request. */
  public enum Outcome {
    BAD_REQUEST,
    NOT_FOUND,
    CONFLICT,
    GONE,
    PRECONDITION_FAILED,
    PRECONDITION_REQUIRED
  }

  private final Outcome outcome;

  /**
   * Creates a typed deletion-lifecycle failure.
   *
   * @param outcome stable API outcome
   * @param message safe client-facing message
   */
  public IcebergDeletionException(Outcome outcome, String message) {
    super(message);
    this.outcome = outcome;
  }

  /**
   * Returns the uniform, non-disclosing deleted-table absence failure.
   *
   * @return sanitized not-found failure
   */
  public static IcebergDeletionException notFound() {
    return new IcebergDeletionException(Outcome.NOT_FOUND, NOT_FOUND_MESSAGE);
  }

  /**
   * Returns the stable API outcome.
   *
   * @return API outcome
   */
  public Outcome outcome() {
    return outcome;
  }
}
