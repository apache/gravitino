package com.datastrato.graviton.meta.catalog.rel;

import com.datastrato.graviton.EntityAlreadyExistsException;

/** Exception thrown when a table with specified name already exists. */
public class TableAlreadyExistsException extends EntityAlreadyExistsException {

  public TableAlreadyExistsException(String message) {
    super(message);
  }

  public TableAlreadyExistsException(String message, Throwable cause) {
    super(message, cause);
  }
}
