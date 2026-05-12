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
package org.apache.gravitino.rel;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nullable;
import org.apache.gravitino.Auditable;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.annotation.Unstable;

/**
 * An interface representing a logical view in a {@link Namespace}. A view is a named query whose
 * definition may be expressed in one or more SQL dialects. A catalog implementation with {@link
 * ViewCatalog} should implement this interface.
 */
@Unstable
public interface View extends Auditable {

  /**
   * Returns the name of the view.
   *
   * @return The view name.
   */
  String name();

  /**
   * Returns the comment of the view, or {@code null} if no comment is set.
   *
   * @return The view comment, or {@code null}.
   */
  @Nullable
  default String comment() {
    return null;
  }

  /**
   * Returns the output schema of the view. Implementations should return an empty array when the
   * output schema is unknown; callers should not rely on a {@code null} return value.
   *
   * @return The view output columns.
   */
  Column[] columns();

  /**
   * Returns the representations of the view. A view carries at least one {@link Representation},
   * typically a {@link SQLRepresentation} for one or more dialects.
   *
   * @return The view representations.
   */
  Representation[] representations();

  /**
   * Returns the default catalog used to resolve unqualified identifiers referenced by the view
   * definition, or {@code null} if not set. The default catalog is shared across all {@link
   * #representations() representations}.
   *
   * @return The default catalog, or {@code null}.
   */
  @Nullable
  default String defaultCatalog() {
    return null;
  }

  /**
   * Returns the default schema used to resolve unqualified identifiers referenced by the view
   * definition, or {@code null} if not set. The default schema is shared across all {@link
   * #representations() representations}.
   *
   * @return The default schema, or {@code null}.
   */
  @Nullable
  default String defaultSchema() {
    return null;
  }

  /**
   * Looks up the {@link SQLRepresentation} for the given dialect. Matching is case-insensitive.
   *
   * @param dialect The dialect identifier to look up, e.g. {@code "trino"}.
   * @return An {@link Optional} containing the matching {@link SQLRepresentation}, or {@link
   *     Optional#empty()} if no representation for the dialect exists or if the matching
   *     representation is not a {@link SQLRepresentation}.
   */
  default Optional<SQLRepresentation> sqlFor(String dialect) {
    if (dialect == null) {
      return Optional.empty();
    }
    Representation[] reps = representations();
    for (Representation rep : reps) {
      if (rep instanceof SQLRepresentation) {
        SQLRepresentation sqlRep = (SQLRepresentation) rep;
        if (dialect.equalsIgnoreCase(sqlRep.dialect())) {
          return Optional.of(sqlRep);
        }
      }
    }
    return Optional.empty();
  }

  /**
   * Returns the properties of the view, or an empty map if no properties are set.
   *
   * @return The view properties.
   */
  default Map<String, String> properties() {
    return Collections.emptyMap();
  }
}
