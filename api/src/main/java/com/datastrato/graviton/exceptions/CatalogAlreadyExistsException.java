/*
* Copyright 2023 Datastrato.
* This software is licensed under the Apache License version 2.
*/

package com.datastrato.graviton.exceptions;

public class CatalogAlreadyExistsException extends AlreadyExistsException {

  public CatalogAlreadyExistsException(String message) {
    super(message);
  }

  public CatalogAlreadyExistsException(String message, Throwable cause) {
    super(message, cause);
  }
}
