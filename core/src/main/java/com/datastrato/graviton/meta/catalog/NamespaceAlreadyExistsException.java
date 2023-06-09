package com.datastrato.graviton.meta.catalog;

import com.datastrato.graviton.EntityAlreadyExistsException;

/**
 * Exception thrown when a namespace already exists.
 */
public class NamespaceAlreadyExistsException extends EntityAlreadyExistsException {

  public NamespaceAlreadyExistsException(String message) {
    super(message);
  }

  public NamespaceAlreadyExistsException(String message, Throwable cause) {
    super(message, cause);
  }
}
