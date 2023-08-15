/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.graviton.storage;

import com.datastrato.graviton.storage.kv.KvEntityStore;
import java.io.IOException;

/**
 * {@link NameMappingService} mangers name to id mappings when using {@link KvEntityStore} to store
 * entity.
 */
public interface NameMappingService {

  /**
   * Get id from name.
   *
   * @param name the name of the entity
   */
  Long get(String name) throws IOException;

  /**
   * If we do not find the id of the name, we create a new id for the name.
   *
   * @param name the name of the entity
   * @return the id of the name, or null if the name does not exist
   */
  Long create(String name);

  /**
   * Get the id of the name. If we do not find the id of the name, we create a new id for the name.
   *
   * @param name the name of the entity
   * @return the id of the name
   */
  default Long getOrCreateId(String name) throws IOException {
    Long id = get(name);
    if (id == null) {
      id = create(name);
    }
    return id;
  }

  /**
   * Update the mapping of the name to id;
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
   */
  boolean update(String oldName, String newName);

  /**
   * Delete id mapping for name. Ignore if the name does not exist.
   *
   * @param name name to be deleted
   * @return true if the name exists and is deleted successfully, false if the name does not exist
   */
  boolean delete(String name);

  /**
   * Get the id generator used by NameMappingService
   *
   * @return the id generator
   */
  IdGenerator getIdGenerator();
}
