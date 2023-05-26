package com.datastrato.graviton.schema.proto;

public class ProtoSerDeException extends RuntimeException {

  public ProtoSerDeException(String message) {
    super(message);
  }

  public ProtoSerDeException(String message, Throwable cause) {
    super(message, cause);
  }
}
