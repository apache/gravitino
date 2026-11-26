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
package org.apache.gravitino.storage.relational.service;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

class TestSemanticModelSnapshotJoins {
  private static final Pattern SNAPSHOT_TABLE_REFERENCE =
      Pattern.compile(
          "\\bsemantic_model_version_info\\b"
              + "|\\bSemanticModelVersionInfoMapper\\s*\\.\\s*TABLE_NAME\\b"
              + "|\\bSemanticModelMetaMapper\\s*\\.\\s*VERSION_TABLE_NAME\\b",
          Pattern.CASE_INSENSITIVE);
  private static final Pattern DIRECT_LATEST_JOIN =
      Pattern.compile(
          "\\bcurrent_version\\s*=\\s*\\w+\\.version\\b|\\.version\\s*=\\s*\\w+\\.current_version\\b",
          Pattern.CASE_INSENSITIVE);

  @Test
  void testSnapshotProvidersSelectRevisionsThroughTheSharedJoin() throws Exception {
    Path packages = Path.of("src/main/java/org/apache/gravitino/storage/relational/mapper");
    int checked = 0;
    try (Stream<Path> files = Files.walk(packages)) {
      for (Path provider : files.filter(path -> path.toString().endsWith(".java")).toList()) {
        String source = Files.readString(provider);
        if (!SNAPSHOT_TABLE_REFERENCE.matcher(source).find()) {
          continue;
        }
        assertFalse(
            DIRECT_LATEST_JOIN.matcher(source).find(),
            () -> "Select revision joins through snapshotJoin in " + provider);
        checked++;
      }
    }
    assertTrue(checked > 1);
  }
}
