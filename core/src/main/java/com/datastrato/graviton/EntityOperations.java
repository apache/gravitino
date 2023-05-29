package com.datastrato.graviton;

import com.datastrato.graviton.schema.Entity;
import com.datastrato.graviton.schema.HasIdentifier;
import com.datastrato.graviton.schema.NameIdentifier;

public interface EntityOperations<T extends Entity & HasIdentifier> {

  /**
   * Creates the entity.
   *
   * @param t the entity to create.
   */
  default void create(T t) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  /**
   * Gets the entity by name identifier.
   *
   * @param nameIdentifier the name identifier of the entity.
   * @return the entity.
   */
  default T get(NameIdentifier nameIdentifier) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  /**
   * Updates the entity.
   *
   * @param t the entity to update.
   */
  default void update(T t) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  /**
   * Deletes the entity by name identifier.
   *
   * @param nameIdentifier the name identifier of the entity.
   */
  default void delete(NameIdentifier nameIdentifier) {
    throw new UnsupportedOperationException("Not implemented yet");
  }
}
