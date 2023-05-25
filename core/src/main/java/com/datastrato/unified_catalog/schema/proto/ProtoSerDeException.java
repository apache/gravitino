package com.datastrato.unified_catalog.schema.proto;

public class ProtoSerDeException extends RuntimeException {

  public ProtoSerDeException(String message) {
    super(message);
  }

  public ProtoSerDeException(String message, Throwable cause) {
    super(message, cause);
  }
}
