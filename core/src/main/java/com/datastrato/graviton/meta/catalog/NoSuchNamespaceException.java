package com.datastrato.graviton.meta.catalog;

import com.datastrato.graviton.NoSuchEntityException;

/** Exception thrown when a namespace with specified name is not existed. */
public class NoSuchNamespaceException extends NoSuchEntityException {

  public NoSuchNamespaceException(String message) {
    super(message);
  }

  public NoSuchNamespaceException(String message, Throwable cause) {
    super(message, cause);
  }
}
