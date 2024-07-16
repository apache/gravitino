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
package org.apache.gravitino.meta;

/** The major and minor versions of a schema. */
public enum SchemaVersion {
  V_0_1(0, 1);

  public final int majorVersion;

  public final int minorVersion;

  /**
   * Constructs a SchemaVersion enum value with the provided version numbers.
   *
   * @param majorVersion The major version number.
   * @param minorVersion The minor version number.
   */
  SchemaVersion(int majorVersion, int minorVersion) {
    this.majorVersion = majorVersion;
    this.minorVersion = minorVersion;
  }

  /**
   * The major version number of the schema.
   *
   * @return The major version number.
   */
  public int getMajorVersion() {
    return majorVersion;
  }

  /**
   * The minor version number of the schema.
   *
   * @return The minor version number.
   */
  public int getMinorVersion() {
    return minorVersion;
  }

  /**
   * The SchemaVersion enum value corresponding to the provided major and minor version numbers.
   *
   * @param majorVersion The major version number.
   * @param minorVersion The minor version number.
   * @return The corresponding SchemaVersion enum value.
   * @throws IllegalArgumentException If no schema version is found.
   */
  public static SchemaVersion forValues(int majorVersion, int minorVersion) {
    for (SchemaVersion schemaVersion : SchemaVersion.values()) {
      if (schemaVersion.majorVersion == majorVersion
          && schemaVersion.minorVersion == minorVersion) {
        return schemaVersion;
      }
    }

    throw new IllegalArgumentException(
        String.format(
            "No schema version found for major version %d and minor version %d",
            majorVersion, minorVersion));
  }
}
