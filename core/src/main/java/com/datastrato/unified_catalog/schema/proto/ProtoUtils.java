package com.datastrato.unified_catalog.schema.proto;

import com.google.protobuf.Timestamp;
import java.time.Instant;

class ProtoUtils {

  private ProtoUtils() {}

  public static Timestamp fromInstant(Instant instant) {
    return Timestamp.newBuilder()
        .setSeconds(instant.getEpochSecond())
        .setNanos(instant.getNano())
        .build();
  }

  public static Instant toInstant(Timestamp timestamp) {
    return Instant.ofEpochSecond(timestamp.getSeconds(), timestamp.getNanos());
  }
}
