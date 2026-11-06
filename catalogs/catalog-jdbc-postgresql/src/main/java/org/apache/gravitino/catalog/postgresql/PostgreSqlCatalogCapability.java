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
package org.apache.gravitino.catalog.postgresql;

import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.Set;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.connector.capability.Capability;
import org.apache.gravitino.connector.capability.CapabilityResult;

public class PostgreSqlCatalogCapability implements Capability {
  /**
   * Regular expression explanation: ^[_a-zA-Z\p{L}/][\w\p{L}-$/=]{0,62}$
   *
   * <p>^[_a-zA-Z\p{L}/] - Start with an underscore, a letter, or a letter from any language
   *
   * <p>[\w\p{L}-$/=]{0,62} - Consist of 0 to 62 characters (making the total length at most 63) of
   * letters (both cases), digits, underscores, any kind of letter from any language, hyphens,
   * dollar signs, slashes or equal signs
   *
   * <p>$ - End of the string
   */
  public static final String POSTGRESQL_NAME_PATTERN = "^[_a-zA-Z\\p{L}/][\\w\\p{L}-$/=]{0,62}$";

  /** Reserved schema and table names in PostgreSQL that cannot be used for user-defined schemas. */
  private static final Set<String> POSTGRESQL_RESERVED_WORDS =
      Set.of("pg_catalog", "information_schema");

  /** {@inheritDoc} */
  @Override
  public CapabilityResult specificationOnExistingName(Scope scope, String name) {
    if (scope != Scope.TABLE && scope != Scope.SCHEMA && scope != Scope.COLUMN) {
      return specificationOnName(scope, name);
    }
    if (MetadataObjects.METADATA_OBJECT_RESERVED_NAME.equals(name)) {
      return CapabilityResult.unsupported("The metadata wildcard name is reserved");
    }
    if (name.isEmpty()
        || name.indexOf('\0') >= 0
        || name.getBytes(StandardCharsets.UTF_8).length > 63) {
      return CapabilityResult.unsupported(
          "PostgreSQL native names must contain 1 to 63 UTF-8 bytes and no NUL");
    }
    if (scope == Scope.SCHEMA
        && POSTGRESQL_RESERVED_WORDS.contains(name.toLowerCase(Locale.ROOT))) {
      return CapabilityResult.unsupported("PostgreSQL system schema names are reserved");
    }
    return CapabilityResult.SUPPORTED;
  }

  @Override
  public CapabilityResult specificationOnName(Scope scope, String name) {
    if (!name.matches(POSTGRESQL_NAME_PATTERN)
        || name.getBytes(StandardCharsets.UTF_8).length > 63) {
      return CapabilityResult.unsupported(
          String.format(
              "The %s name '%s' is illegal. Names must use supported characters and contain at most 63 UTF-8 bytes.",
              scope, name));
    }

    if (scope == Scope.SCHEMA
        && POSTGRESQL_RESERVED_WORDS.contains(name.toLowerCase(Locale.ROOT))) {
      return CapabilityResult.unsupported(
          String.format("The %s name '%s' is reserved and cannot be used.", scope, name));
    }

    return CapabilityResult.SUPPORTED;
  }
}
