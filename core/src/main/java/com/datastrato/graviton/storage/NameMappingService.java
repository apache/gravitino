/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.graviton.storage;

import com.datastrato.graviton.storage.kv.KvEntityStore;
import com.datastrato.graviton.util.Executable;
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
   * If we do not find the id of the name, we create a new id for the name. Note, this method should
   * be called in transaction.
   *
   * @param name the name of the entity
   */
  Long create(String name) throws IOException;

  /**
   * Get the id of the name. If we do not find the id of the name, we create a new id for the name.
   *
   * @param name the name of the entity
   */
  default Long getOrCreateId(String name) throws IOException {
    Long id = get(name);
    if (id == null) {
      id = create(name);
    }
    return id;
  }

  /**
   * Update the mapping btw name and id.
   *
   * <p>For example, if we have the following mapping: name_a ---> 1 id_1 ---> a If we want to
   * change to mapping to b --> 1, then name_b ---> 1 id_1 ---> b
   *
   * @param oldName old name of entity
   * @param newName new name of entity
   */
  boolean update(String oldName, String newName) throws IOException;

  /**
   * Delete related mapping of the name.
   *
   * @param name name to delete
   */
  boolean delete(String name) throws IOException;

  /**
   * Get the id generator used by NameMappingService
   *
   * @return the id generator
   */
  IdGenerator getIdGenerator();

  // Execute some operation in transaction.
  <R, E extends Exception> R executeInTransaction(Executable<R, E> executable)
      throws E, IOException;
}
