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

/**
 * The core database backends exercised by {@code core/build.gradle.kts}'s database test lanes. The
 * enum constant name is what a test declares in {@link DatabaseTest#backends()}; {@link
 * #propertyValue()} is the wire value the lane tasks publish via the {@code
 * gravitino.core.test.backend} system property and {@link BackendTestSelector} reads back.
 */
public enum DatabaseBackend {
  H2("h2"),
  MYSQL("mysql"),
  POSTGRESQL("postgresql");

  private final String propertyValue;

  DatabaseBackend(String propertyValue) {
    this.propertyValue = propertyValue;
  }

  /**
   * Returns the value this backend is identified by in the {@code gravitino.core.test.backend}
   * system property.
   *
   * @return the property value
   */
  public String propertyValue() {
    return propertyValue;
  }
}
