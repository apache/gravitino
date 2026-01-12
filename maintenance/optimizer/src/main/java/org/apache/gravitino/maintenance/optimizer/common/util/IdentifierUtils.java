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

package org.apache.gravitino.maintenance.optimizer.common.util;

import com.google.common.base.Preconditions;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;

/** Utilities for working with fully qualified table identifiers. */
public class IdentifierUtils {

  private static final String NORMALIZED_IDENTIFIER_MESSAGE =
      "Identifier must be catalog.schema.table";

  /**
   * Removes the catalog level from a catalog.schema.table identifier.
   *
   * @param tableIdentifier fully qualified table identifier
   * @return schema.table identifier
   * @throws IllegalArgumentException if the identifier is not catalog.schema.table
   */
  public static NameIdentifier removeCatalogFromIdentifier(NameIdentifier tableIdentifier) {
    Preconditions.checkArgument(tableIdentifier != null, "tableIdentifier must not be null");
    Namespace namespace = tableIdentifier.namespace();
    Preconditions.checkArgument(
        namespace != null && namespace.levels().length == 2, NORMALIZED_IDENTIFIER_MESSAGE);
    return NameIdentifier.of(namespace.levels()[1], tableIdentifier.name());
  }

  /**
   * Returns the catalog name from a catalog.schema.table identifier.
   *
   * @param tableIdentifier fully qualified table identifier
   * @return catalog name
   * @throws IllegalArgumentException if the identifier is not catalog.schema.table
   */
  public static String getCatalogNameFromTableIdentifier(NameIdentifier tableIdentifier) {
    Preconditions.checkArgument(tableIdentifier != null, "tableIdentifier must not be null");
    Namespace namespace = tableIdentifier.namespace();
    Preconditions.checkArgument(
        namespace != null && namespace.levels().length == 2, NORMALIZED_IDENTIFIER_MESSAGE);
    return namespace.levels()[0];
  }

  /**
   * Validates that a table identifier is normalized as catalog.schema.table.
   *
   * @param tableIdentifier identifier to validate
   * @throws IllegalArgumentException if the identifier is not catalog.schema.table
   */
  public static void requireTableIdentifierNormalized(NameIdentifier tableIdentifier) {
    Preconditions.checkArgument(tableIdentifier != null, "tableIdentifier must not be null");
    Namespace namespace = tableIdentifier.namespace();
    Preconditions.checkArgument(
        namespace != null && namespace.levels().length == 2, NORMALIZED_IDENTIFIER_MESSAGE);
  }
}
