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

import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.annotation.Unstable;
import org.apache.gravitino.exceptions.NoSuchViewException;

/**
 * The ViewCatalog interface defines the public API for managing views in a schema. If the catalog
 * implementation supports views, it must implement this interface.
 *
 * <p>Note: This is a minimal interface. Full operations (create, list, alter, drop) will be added
 * when Gravitino APIs support views.
 */
@Unstable
public interface ViewCatalog {

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
}
