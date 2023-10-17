/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino;

/**
 * Exception class indicating that an entity already exists. This exception is thrown when an
 * attempt is made to create an entity that already exists within the Gravitino framework.
 */
public class EntityAlreadyExistsException extends RuntimeException {

  /**
   * Constructs an EntityAlreadyExistsException.
   *
   * @param message The detail message explaining the exception.
   */
  public EntityAlreadyExistsException(String message) {
    super(message);
  }

  /**
   * Constructs an EntityAlreadyExistsException.
   *
   * @param message The detail message explaining the exception.
   * @param cause The cause of the exception.
   */
  public EntityAlreadyExistsException(String message, Throwable cause) {
    super(message, cause);
  }
}
