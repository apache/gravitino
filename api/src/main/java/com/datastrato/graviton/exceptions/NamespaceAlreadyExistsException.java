package com.datastrato.graviton.exceptions;

/** Exception thrown when a namespace already exists. */
public class NamespaceAlreadyExistsException extends AlreadyExistsException {

  public NamespaceAlreadyExistsException(String message) {
    super(message);
  }

  public NamespaceAlreadyExistsException(String message, Throwable cause) {
    super(message, cause);
  }
}
