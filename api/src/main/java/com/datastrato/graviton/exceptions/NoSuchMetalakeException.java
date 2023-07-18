/*
* Copyright 2023 Datastrato.
* This software is licensed under the Apache License version 2.
*/

package com.datastrato.graviton.exceptions;

public class NoSuchMetalakeException extends NotFoundException {

  public NoSuchMetalakeException(String message) {
    super(message);
  }

  public NoSuchMetalakeException(String message, Throwable cause) {
    super(message, cause);
  }
}
