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
   * If we do not find the id of the name, we create a new id for the name.
   *
   * @param name the name of the entity
   */
  Long create(String name);

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
   * @param name name of the entity
   * @param id new id for the name
   */
  boolean update(String name, long id);

  /**
   * Get the id generator used by NameMappingService
   *
   * @return the id generator
   */
  IdGenerator getIdGenerator();

  // Execute some operation in transaction.
  <R, E extends Exception> R executeInTransaction(Executable<R, E> executable) throws E;
}
