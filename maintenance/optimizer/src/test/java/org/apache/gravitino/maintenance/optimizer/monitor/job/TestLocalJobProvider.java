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

package org.apache.gravitino.maintenance.optimizer.monitor.job;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.maintenance.optimizer.common.OptimizerEnv;
import org.apache.gravitino.maintenance.optimizer.common.conf.OptimizerConfig;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TestLocalJobProvider {

  @TempDir Path tempDir;

  @Test
  void testGetJobNamesMergesAndNormalizesIdentifiers() throws IOException {
    Path jobFile = tempDir.resolve("jobs.jsonl");
    Files.write(
        jobFile,
        List.of(
            "{\"identifier\":\"catalog.schema.table\",\"job-identifiers\":[\"job1\",\"job2\"]}",
            "{\"identifier\":\"schema.table\",\"job-identifiers\":[\"job2\",\"job3\"]}",
            "{\"identifier\":\"catalog.schema.table\",\"job-identifiers\":[\"job4\",\"invalid..job\"]}",
            "{\"identifier\":\"catalog.schema.table\",\"job-identifiers\":[\"\",12]}",
            "{\"identifier\":\"other.table\",\"job-identifiers\":[\"jobX\"]}",
            "malformed json"));

    LocalJobProvider provider = new LocalJobProvider();
    OptimizerEnv optimizerEnv = new OptimizerEnv(createConfig(jobFile));
    provider.initialize(optimizerEnv);

    List<NameIdentifier> jobs =
        provider.jobIdentifiers(NameIdentifier.parse("catalog.schema.table"));

    Assertions.assertEquals(
        List.of(
            NameIdentifier.parse("job1"),
            NameIdentifier.parse("job2"),
            NameIdentifier.parse("job3"),
            NameIdentifier.parse("job4")),
        jobs);
  }

  @Test
  void testInitializeRequiresFilePath() {
    LocalJobProvider provider = new LocalJobProvider();
    OptimizerEnv optimizerEnv = new OptimizerEnv(new OptimizerConfig());

    Assertions.assertThrows(
        IllegalArgumentException.class, () -> provider.initialize(optimizerEnv));
  }

  @Test
  void testInitializeRejectsMissingFilePath() {
    LocalJobProvider provider = new LocalJobProvider();
    Path missingFile = tempDir.resolve("missing-jobs.jsonl");
    OptimizerEnv optimizerEnv = new OptimizerEnv(createConfig(missingFile));

    Assertions.assertThrows(
        IllegalArgumentException.class, () -> provider.initialize(optimizerEnv));
  }

  @Test
  void testJobIdentifiersRequiresInitialize() {
    LocalJobProvider provider = new LocalJobProvider();
    Assertions.assertThrows(
        IllegalStateException.class,
        () -> provider.jobIdentifiers(NameIdentifier.parse("catalog.schema.table")));
  }

  private OptimizerConfig createConfig(Path jobFile) {
    Map<String, String> configs = new HashMap<>();
    configs.put(LocalJobProvider.JOB_FILE_PATH_CONFIG, jobFile.toString());
    configs.put(OptimizerConfig.GRAVITINO_DEFAULT_CATALOG_CONFIG.getKey(), "catalog");
    return new OptimizerConfig(configs);
  }
}
