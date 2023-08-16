/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.graviton.storage;

import com.datastrato.graviton.storage.kv.KvEntityStore;
import java.io.IOException;

/**
 * {@link NameMappingService} manages name to id mappings when using {@link KvEntityStore} to store
 * entity.
 *
 * <p>Note. Implementations of this interface should be thread-safe.
 */
public interface NameMappingService {

  /**
   * Get id from name.
   *
   * @param name the name of the entity
   * @return the id of the name, or null if the name does not exist
   * @throws IOException if the underlying storage failed
   */
  Long get(String name) throws IOException;

  /**
   * If we do not find the id of the name, we create a new id for the name.
   *
   * @param name the name of the entity
   * @return the id of the name, or null if the name does not exist
   * @throws IOException if the underlying storage failed
   */
  Long create(String name) throws IOException;

  /**
   * Update the mapping of the name to id. This method is used to update the mapping when we rename
   * an entity. Please see the example
   *
   * <pre>
   * Before:
   *   oldname -> 1
   *   1       -> oldname
   *
   * After:
   *  newname -> 1
   *  1       -> newname
   * </pre>
   *
   * @param oldName name to be updated
   * @param newName new name
   * @throws IOException if the underlying storage failed
   */
  boolean update(String oldName, String newName) throws IOException;

  /**
   * Delete id mapping for name. Ignore if the name does not exist.
   *
   * @param name name to be deleted
   * @return true if the name exists and is deleted successfully, false if the name does not exist
   * @throws IOException if the underlying storage failed
   */
  boolean delete(String name) throws IOException;
}
