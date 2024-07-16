/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.proto;

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
