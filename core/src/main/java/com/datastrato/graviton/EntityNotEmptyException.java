/*
* Copyright 2023 Datastrato.
* This software is licensed under the Apache License version 2.
*/

package com.datastrato.graviton;

public class EntityNotEmptyException extends RuntimeException {
  public EntityNotEmptyException(String message) {
    super(message);
  }

  public EntityNotEmptyException(String message, Throwable cause) {
    super(message, cause);
  }
}
