/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino;

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
   * @return True if the metalake was dropped, false otherwise.
   */
  boolean dropMetalake(NameIdentifier ident);
}
