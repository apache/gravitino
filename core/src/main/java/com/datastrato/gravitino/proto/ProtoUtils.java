/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.proto;

import com.google.protobuf.Timestamp;
import java.time.Instant;

/** Utility class for converting between Java Instant and Protocol Buffer Timestamp. */
public class ProtoUtils {

  private ProtoUtils() {}

  /**
   * Converts a Java Instant object to a Protocol Buffer Timestamp object.
   *
   * @param instant The Java Instant to convert.
   * @return The corresponding Protocol Buffer Timestamp.
   */
  public static Timestamp fromInstant(Instant instant) {
    return Timestamp.newBuilder()
        .setSeconds(instant.getEpochSecond())
        .setNanos(instant.getNano())
        .build();
  }

  /**
   * Converts a Protocol Buffer Timestamp object to a Java Instant object.
   *
   * @param timestamp The Protocol Buffer Timestamp to convert.
   * @return The corresponding Java Instant.
   */
  public static Instant toInstant(Timestamp timestamp) {
    return Instant.ofEpochSecond(timestamp.getSeconds(), timestamp.getNanos());
  }
}
