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

package org.apache.gravitino.storage.relational;

import java.util.Objects;

/**
 * Provides the database fixture owned by the current test invocation.
 *
 * <p>The extension owns the fixture lifecycle. Tests must not close the supplied backend.
 */
public final class DatabaseTestContext {
  private final String backendType;
  private final RelationalBackend backend;
  private final DatabaseIsolation isolation;

  DatabaseTestContext(String backendType, RelationalBackend backend, DatabaseIsolation isolation) {
    this.backendType = Objects.requireNonNull(backendType, "backendType");
    this.backend = Objects.requireNonNull(backend, "backend");
    this.isolation = Objects.requireNonNull(isolation, "isolation");
  }

  /** Returns the database implementation selected for this invocation. */
  public String backendType() {
    return backendType;
  }

  /** Returns the initialized relational backend owned by the test fixture. */
  public RelationalBackend backend() {
    return backend;
  }

  /** Returns the isolation policy requested by the test. */
  public DatabaseIsolation isolation() {
    return isolation;
  }
}
