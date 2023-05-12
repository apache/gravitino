package com.datastrato.unified_catalog.schema;

import java.io.Serializable;
import java.util.List;

public interface Entity extends Serializable {

  /**
   * Validates the entity if the field arguments are valid.
   *
   * @throws IllegalArgumentException
   */
  void validate() throws IllegalArgumentException;

  /**
   * Returns the schema of the entity.
   * @return List<Field>
   */
  List<Field> schema();
}
