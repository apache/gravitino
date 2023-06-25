package com.datastrato.graviton.meta.catalog;

import com.datastrato.graviton.EntityNotEmptyException;

public class NamespaceNotEmptyException extends EntityNotEmptyException {
  public NamespaceNotEmptyException(String message) {
    super(message);
  }

  public NamespaceNotEmptyException(String message, Throwable cause) {
    super(message, cause);
  }
}
