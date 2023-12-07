/*
 * Copyright 2023 DATASTRATO Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.proto;

/**
 * Custom exception class for Protocol Buffer Serializer and Deserializer (SerDe) related errors.
 */
public class ProtoSerDeException extends RuntimeException {

  /**
   * Constructs a new ProtoSerDeException.
   *
   * @param message The error message.
   */
  public ProtoSerDeException(String message) {
    super(message);
  }

  /**
   * Constructs a new ProtoSerDeException.
   *
   * @param message The error message.
   * @param cause The underlying cause of the exception.
   */
  public ProtoSerDeException(String message, Throwable cause) {
    super(message, cause);
  }
}
