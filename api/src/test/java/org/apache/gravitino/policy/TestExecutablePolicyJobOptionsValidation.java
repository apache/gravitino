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
package org.apache.gravitino.policy;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Create/alter validation for maintenance policy {@code jobOptions} when Spark will be scheduled.
 */
public class TestExecutablePolicyJobOptionsValidation {

  private static final Map<String, String> COMPLETE_JOB_OPTIONS =
      ImmutableMap.of(
          "type", "rest",
          "uri", "http://host.docker.internal:9001/iceberg",
          "warehouse_location", "/tmp/iceberg-warehouse");

  @Test
  void noTriggerDoesNotRequireJobOptions() {
    Assertions.assertDoesNotThrow(() -> PolicyContents.icebergDataCompaction().validate());
    Assertions.assertDoesNotThrow(
        () ->
            PolicyContents.icebergSnapshotExpiration(1L, 1L, false, null, null, null, null, null)
                .validate());
    Assertions.assertDoesNotThrow(
        () ->
            PolicyContents.icebergRewriteManifests(
                    null, null, null, null, null, null, null, null, null, null)
                .validate());
    Assertions.assertDoesNotThrow(() -> PolicyContents.icebergOrphanFileRemoval().validate());
  }

  @Test
  void timeTriggerRequiresCatalogJobOptions() {
    IllegalArgumentException ex =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                PolicyContents.icebergDataCompaction(
                        0L,
                        0L,
                        1L,
                        1L,
                        1L,
                        "binpack",
                        null,
                        null,
                        "0/10 * * * * ?",
                        null,
                        null,
                        null,
                        null)
                    .validate());
    Assertions.assertTrue(ex.getMessage().contains("warehouse_location"));
  }

  @Test
  void commitTriggerRequiresCatalogJobOptions() {
    IllegalArgumentException ex =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                PolicyContents.icebergDataCompaction(
                        0L,
                        0L,
                        1L,
                        1L,
                        1L,
                        "binpack",
                        null,
                        null,
                        null,
                        true,
                        null,
                        ImmutableMap.of("type", "rest", "uri", "http://irc"),
                        null)
                    .validate());
    Assertions.assertTrue(ex.getMessage().contains("warehouse_location"));
  }

  @Test
  void shortAliasesSatisfyRequiredJobOptions() {
    Assertions.assertDoesNotThrow(
        () ->
            PolicyContents.icebergDataCompaction(
                    0L,
                    0L,
                    1L,
                    1L,
                    1L,
                    "binpack",
                    null,
                    null,
                    "0 0 2 * * ?",
                    true,
                    0L,
                    COMPLETE_JOB_OPTIONS,
                    null)
                .validate());
  }

  @Test
  void flatKeysSatisfyRequiredJobOptions() {
    Map<String, String> opts =
        ImmutableMap.of(
            "catalog_type", "rest",
            "catalog_uri", "http://host.docker.internal:9001/iceberg",
            "warehouse_location", "/tmp/iceberg-warehouse");
    Assertions.assertDoesNotThrow(
        () ->
            PolicyContents.icebergSnapshotExpiration(
                    1L, 1L, false, "0 0 3 * * ?", null, null, opts, null)
                .validate());
    Assertions.assertDoesNotThrow(
        () ->
            PolicyContents.icebergRewriteManifests(
                    null, null, null, null, null, "0 0 4 * * ?", null, null, opts, null)
                .validate());
    Assertions.assertDoesNotThrow(
        () ->
            PolicyContents.icebergOrphanFileRemoval(
                    IcebergOrphanFileRemovalContent.DEFAULT_OLDER_THAN_DAYS,
                    null,
                    false,
                    "0 0 5 * * ?",
                    null,
                    null,
                    opts,
                    null)
                .validate());
  }

  @Test
  void missingUriRejected() {
    IllegalArgumentException ex =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                PolicyContents.icebergOrphanFileRemoval(
                        3L,
                        null,
                        false,
                        "0 0 5 * * ?",
                        null,
                        null,
                        ImmutableMap.of("type", "rest", "warehouse_location", "/tmp/w"),
                        null)
                    .validate());
    Assertions.assertTrue(
        ex.getMessage().contains("catalog_uri") || ex.getMessage().contains("uri"));
  }

  @Test
  void missingTypeRejected() {
    IllegalArgumentException ex =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                PolicyContents.icebergRewriteManifests(
                        null,
                        null,
                        null,
                        null,
                        null,
                        "0 0 4 * * ?",
                        null,
                        null,
                        ImmutableMap.of(
                            "uri", "http://irc", "warehouse_location", "/tmp/iceberg-warehouse"),
                        null)
                    .validate());
    Assertions.assertTrue(
        ex.getMessage().contains("catalog_type") || ex.getMessage().contains("'type'"));
  }
}
