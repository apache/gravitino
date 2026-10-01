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
package org.apache.gravitino.spark.connector.jdbc.doris;

import com.google.common.collect.ImmutableMap;
import java.util.Map;

/** Immutable, fail-closed policy for the governed Doris batch-write surface. */
final class DorisWritePolicy35 {

  private static final DorisWritePolicy35 DISABLED =
      new DorisWritePolicy35(
          DorisConnectorConstants35.WRITE_DISABLED,
          DorisConnectorConstants35.WRITE_OVERWRITE_REJECT);

  private final String mode;
  private final String overwriteMode;

  private DorisWritePolicy35(String mode, String overwriteMode) {
    this.mode = mode;
    this.overwriteMode = overwriteMode;
  }

  static DorisWritePolicy35 from(Map<String, String> properties) {
    if (properties == null) {
      throw new IllegalArgumentException("Doris catalog properties must not be null");
    }
    String mode =
        properties.getOrDefault(
            DorisConnectorConstants35.GRAVITINO_WRITE_MODE,
            DorisConnectorConstants35.WRITE_DISABLED);
    String overwriteMode =
        properties.getOrDefault(
            DorisConnectorConstants35.GRAVITINO_WRITE_OVERWRITE_MODE,
            DorisConnectorConstants35.WRITE_OVERWRITE_REJECT);
    if (!DorisConnectorConstants35.WRITE_DISABLED.equals(mode)
        && !DorisConnectorConstants35.WRITE_BATCH.equals(mode)) {
      throw new IllegalArgumentException("doris-write-mode must be disabled or batch");
    }
    if (!DorisConnectorConstants35.WRITE_OVERWRITE_REJECT.equals(overwriteMode)
        && !DorisConnectorConstants35.WRITE_OVERWRITE_TRUNCATE.equals(overwriteMode)) {
      throw new IllegalArgumentException("doris-write-overwrite-mode must be reject or truncate");
    }
    if (DorisConnectorConstants35.WRITE_DISABLED.equals(mode)) {
      if (!DorisConnectorConstants35.WRITE_OVERWRITE_REJECT.equals(overwriteMode)) {
        throw new IllegalArgumentException(
            "doris-write-overwrite-mode=truncate requires doris-write-mode=batch");
      }
      return DISABLED;
    }
    return new DorisWritePolicy35(mode, overwriteMode);
  }

  static DorisWritePolicy35 disabled() {
    return DISABLED;
  }

  boolean enabled() {
    return DorisConnectorConstants35.WRITE_BATCH.equals(mode);
  }

  boolean allowsTruncate() {
    return enabled() && DorisConnectorConstants35.WRITE_OVERWRITE_TRUNCATE.equals(overwriteMode);
  }

  Map<String, String> forcedConnectorOptions() {
    if (!enabled()) {
      return ImmutableMap.of();
    }
    return ImmutableMap.<String, String>builder()
        .put(DorisConnectorConstants35.DORIS_SINK_MODE, "stream_load")
        .put(DorisConnectorConstants35.DORIS_SINK_AUTO_REDIRECT, "false")
        .put(DorisConnectorConstants35.DORIS_SINK_ENABLE_2PC, "true")
        .put(DorisConnectorConstants35.DORIS_SINK_STRICT_MODE, "true")
        .put(DorisConnectorConstants35.DORIS_MAX_FILTER_RATIO, "0")
        .put(DorisConnectorConstants35.DORIS_WRITE_SCHEMALESS, "false")
        .build();
  }
}
