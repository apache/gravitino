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

import java.util.Map;
import javax.annotation.Nullable;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.annotation.Unstable;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.NoSuchViewException;
import org.apache.gravitino.exceptions.ViewAlreadyExistsException;

/**
 * The ViewCatalog interface defines the public API for managing views in a schema. If the catalog
 * implementation supports views, it must implement this interface.
 */
@Unstable
public interface ViewCatalog {

  /**
   * List the views in a namespace from the catalog.
   *
   * @param namespace A namespace.
   * @return An array of view identifiers in the namespace.
   * @throws NoSuchSchemaException If the schema does not exist.
   */
  NameIdentifier[] listViews(Namespace namespace) throws NoSuchSchemaException;

  /**
   * Load view metadata by {@link NameIdentifier} from the catalog.
   *
   * @param ident A view identifier.
   * @return The view metadata.
   * @throws NoSuchViewException If the view does not exist.
   */
  View loadView(NameIdentifier ident) throws NoSuchViewException;

  /**
   * Check if a view exists using its identifier.
   *
   * @param ident A view identifier.
   * @return true If the view exists, false otherwise.
   */
  default boolean viewExists(NameIdentifier ident) {
    try {
      return loadView(ident) != null;
    } catch (NoSuchViewException e) {
      return false;
    }
  }

  /**
   * Create a view in the catalog.
   *
   * @param ident A view identifier.
   * @param comment The view comment, may be {@code null}.
   * @param columns The output columns of the view.
   * @param representations The representations of the view. At least one representation is
   *     expected.
   * @param defaultCatalog The default catalog used to resolve unqualified identifiers referenced by
   *     the view definition, or {@code null} if not set.
   * @param defaultSchema The default schema used to resolve unqualified identifiers referenced by
   *     the view definition, or {@code null} if not set.
   * @param properties The view properties.
   * @return The created view metadata.
   * @throws NoSuchSchemaException If the schema does not exist.
   * @throws ViewAlreadyExistsException If the view already exists.
   */
  View createView(
      NameIdentifier ident,
      String comment,
      Column[] columns,
      Representation[] representations,
      @Nullable String defaultCatalog,
      @Nullable String defaultSchema,
      Map<String, String> properties)
      throws NoSuchSchemaException, ViewAlreadyExistsException;

  /**
   * Apply the {@link ViewChange changes} to a view in the catalog.
   *
   * <p>Implementations may reject the change. If any change is rejected, no changes should be
   * applied to the view.
   *
   * @param ident A view identifier.
   * @param changes View changes to apply to the view.
   * @return The updated view metadata.
   * @throws NoSuchViewException If the view does not exist.
   * @throws IllegalArgumentException If the change is rejected by the implementation.
   */
  View alterView(NameIdentifier ident, ViewChange... changes)
      throws NoSuchViewException, IllegalArgumentException;

  /**
   * Drop a view from the catalog.
   *
   * @param ident A view identifier.
   * @return True if the view is dropped, false if the view does not exist.
   */
  boolean dropView(NameIdentifier ident);
}
