/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.client.api;

import com.datastrato.gravitino.Metalake;
import com.datastrato.gravitino.MetalakeChange;
import com.datastrato.gravitino.annotation.Evolving;
import com.datastrato.gravitino.exceptions.MetalakeAlreadyExistsException;
import com.datastrato.gravitino.exceptions.NoSuchMetalakeException;
import java.util.Map;

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
   * @param name the name of the metalake.
   * @return The metalake.
   * @throws NoSuchMetalakeException If the metalake does not exist.
   */
  Metalake loadMetalake(String name) throws NoSuchMetalakeException;

  /**
   * Check if a metalake exists.
   *
   * @param name The name of the metalake.
   * @return True if the metalake exists, false otherwise.
   */
  default boolean metalakeExists(String name) {
    try {
      loadMetalake(name);
      return true;
    } catch (NoSuchMetalakeException e) {
      return false;
    }
  }

  /**
   * Create a metalake with specified identifier.
   *
   * @param name The name of the metalake.
   * @param comment The comment of the metalake.
   * @param properties The properties of the metalake.
   * @return The created metalake.
   * @throws MetalakeAlreadyExistsException If the metalake already exists.
   */
  Metalake createMetalake(String name, String comment, Map<String, String> properties)
      throws MetalakeAlreadyExistsException;

  /**
   * Alter a metalake with specified identifier.
   *
   * @param name The name of the metalake.
   * @param changes The changes to apply.
   * @return The altered metalake.
   * @throws NoSuchMetalakeException If the metalake does not exist.
   * @throws IllegalArgumentException If the changes cannot be applied to the metalake.
   */
  Metalake alterMetalake(String name, MetalakeChange... changes)
      throws NoSuchMetalakeException, IllegalArgumentException;

  /**
   * Drop a metalake with specified identifier.
   *
   * @param name The identifier of the metalake.
   * @return True if the metalake was dropped, false otherwise.
   */
  boolean dropMetalake(String name);
}
