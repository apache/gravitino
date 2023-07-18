/*
* Copyright 2023 Datastrato.
* This software is licensed under the Apache License version 2.
*/

package com.datastrato.graviton.proto;

public class ProtoSerDeException extends RuntimeException {

  public ProtoSerDeException(String message) {
    super(message);
  }

  public ProtoSerDeException(String message, Throwable cause) {
    super(message, cause);
  }
}
