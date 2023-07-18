/*
* Copyright 2023 Datastrato.
* This software is licensed under the Apache License version 2.
*/

package com.datastrato.graviton.exceptions;

public class MetalakeAlreadyExistsException extends AlreadyExistsException {

  public MetalakeAlreadyExistsException(String message) {
    super(message);
  }

  public MetalakeAlreadyExistsException(String message, Throwable cause) {
    super(message, cause);
  }
}
