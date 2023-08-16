/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.exceptions;

/** This exception is thrown when an entity is not found. */
public class NoSuchEntityException extends RuntimeException {

  /**
   * Constructs a new NoSuchEntityException.
   *
   * @param message The detail message.
   */
  public NoSuchEntityException(String message) {
    super(message);
  }

  /**
   * Constructs a new NoSuchEntityException.
   *
   * @param message The detail message.
   * @param cause The cause of the exception.
   */
  public NoSuchEntityException(String message, Throwable cause) {
    super(message, cause);
  }
}
