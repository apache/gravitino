package com.datastrato.graviton.meta.catalog.meta;

import com.datastrato.graviton.NoSuchEntityException;

/**
 * Exception thrown when a table with specified name is not existed.
 */
public class NoSuchTableException extends NoSuchEntityException {

  public NoSuchTableException(String message) {
    super(message);
  }

  public NoSuchTableException(String message, Throwable cause) {
    super(message, cause);
  }
}
