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

import java.util.Locale;
import java.util.Optional;
import java.util.Set;

/** Selects a single JDBC backend for the core database test suites. */
public final class BackendTestSelector {

  private static final String BACKEND_PROPERTY = "gravitino.core.test.backend";
  private static final Set<String> SUPPORTED_BACKENDS = Set.of("h2", "mysql", "postgresql");

  private BackendTestSelector() {}

  /**
   * Returns the selected backend, or an empty value when the legacy all-applicable-backends
   * behavior should be used.
   *
   * @return the normalized selected backend
   * @throws IllegalArgumentException if the configured backend is unsupported
   */
  public static Optional<String> selectedBackend() {
    String configuredBackend = System.getProperty(BACKEND_PROPERTY);
    if (configuredBackend == null) {
      return Optional.empty();
    }

    return Optional.of(validate(configuredBackend));
  }

  /**
   * Returns whether a backend should run under the current selection.
   *
   * @param backend backend to test
   * @return true when no backend is selected or the backend matches the selection
   * @throws IllegalArgumentException if either backend value is unsupported
   */
  public static boolean isSelected(String backend) {
    String normalizedBackend = validate(backend);
    return selectedBackend().map(normalizedBackend::equals).orElse(true);
  }

  private static String validate(String backend) {
    String normalizedBackend = backend.trim().toLowerCase(Locale.ROOT);
    if (!SUPPORTED_BACKENDS.contains(normalizedBackend)) {
      throw new IllegalArgumentException(
          String.format(
              "Unsupported core test backend '%s'; expected one of %s",
              backend, SUPPORTED_BACKENDS));
    }
    return normalizedBackend;
  }
}
