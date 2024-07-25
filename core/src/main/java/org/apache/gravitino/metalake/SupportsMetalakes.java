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
package org.apache.gravitino.metalake;

import java.util.Map;
import org.apache.gravitino.Metalake;
import org.apache.gravitino.MetalakeChange;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.annotation.Evolving;
import org.apache.gravitino.exceptions.MetalakeAlreadyExistsException;
import org.apache.gravitino.exceptions.NoSuchMetalakeException;

/**
 * Interface for supporting metalakes. It includes methods for listing, loading, creating, altering
 * and dropping metalakes.
 */
@Evolving
public interface SupportsMetalakes {

  /**
   * List all metalakes.
   *
   * @return The list of metalakes.
   */
  Metalake[] listMetalakes();

  /**
   * Load a metalake by its identifier.
   *
   * @param ident the identifier of the metalake.
   * @return The metalake.
   * @throws NoSuchMetalakeException If the metalake does not exist.
   */
  Metalake loadMetalake(NameIdentifier ident) throws NoSuchMetalakeException;

  /**
   * Check if a metalake exists.
   *
   * @param ident The identifier of the metalake.
   * @return True if the metalake exists, false otherwise.
   */
  default boolean metalakeExists(NameIdentifier ident) {
    try {
      loadMetalake(ident);
      return true;
    } catch (NoSuchMetalakeException e) {
      return false;
    }
  }

  /**
   * Create a metalake with specified identifier.
   *
   * @param ident The identifier of the metalake.
   * @param comment The comment of the metalake.
   * @param properties The properties of the metalake.
   * @return The created metalake.
   * @throws MetalakeAlreadyExistsException If the metalake already exists.
   */
  Metalake createMetalake(NameIdentifier ident, String comment, Map<String, String> properties)
      throws MetalakeAlreadyExistsException;

  /**
   * Alter a metalake with specified identifier.
   *
   * @param ident The identifier of the metalake.
   * @param changes The changes to apply.
   * @return The altered metalake.
   * @throws NoSuchMetalakeException If the metalake does not exist.
   * @throws IllegalArgumentException If the changes cannot be applied to the metalake.
   */
  Metalake alterMetalake(NameIdentifier ident, MetalakeChange... changes)
      throws NoSuchMetalakeException, IllegalArgumentException;

  /**
   * Drop a metalake with specified identifier.
   *
   * @param ident The identifier of the metalake.
   * @return True if the metalake was dropped, false if the metalake does not exist.
   */
  boolean dropMetalake(NameIdentifier ident);
}
