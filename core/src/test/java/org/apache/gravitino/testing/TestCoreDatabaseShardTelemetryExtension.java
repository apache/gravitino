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

package org.apache.gravitino.testing;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.Nullable;
import org.apache.gravitino.testing.CoreDatabaseShardTelemetryExtension.ClassStatistics;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.io.TempDir;

class TestCoreDatabaseShardTelemetryExtension {

  @Test
  void formatsLeafOutcomesAndClassDuration() {
    ClassStatistics statistics = new ClassStatistics("example.TestStorage", 1_000_000L);
    statistics.recordPassed();
    statistics.recordPassed();
    statistics.recordFailed();
    statistics.recordSkipped();
    statistics.recordSkipped();

    assertEquals(
        "[CORE-DB-SHARD] backend=postgresql worker=2 class=example.TestStorage "
            + "tests=5 passed=2 failed=1 skipped=2 durationMs=8",
        statistics.toRecord("postgresql", "2", 9_000_000L));
  }

  @Test
  void recordsExtensionCallbacksInWorkerFile(@TempDir Path temporaryDirectory) throws Exception {
    String originalOutputDirectory =
        System.getProperty(CoreDatabaseShardTelemetryExtension.OUTPUT_DIRECTORY_PROPERTY);
    String originalBackend =
        System.getProperty(CoreDatabaseShardTelemetryExtension.BACKEND_PROPERTY);
    String originalWorker = System.getProperty("org.gradle.test.worker");
    try {
      System.setProperty(
          CoreDatabaseShardTelemetryExtension.OUTPUT_DIRECTORY_PROPERTY,
          temporaryDirectory.toString());
      System.setProperty(CoreDatabaseShardTelemetryExtension.BACKEND_PROPERTY, "h2");
      System.setProperty("org.gradle.test.worker", "worker/7");

      AtomicLong nanoTime = new AtomicLong(1_000_000L);
      CoreDatabaseShardTelemetryExtension extension =
          new CoreDatabaseShardTelemetryExtension(nanoTime::get);
      ExtensionContext context = mock(ExtensionContext.class);
      doReturn(SampleDatabaseTest.class).when(context).getRequiredTestClass();

      extension.beforeAll(context);
      extension.testSuccessful(context);
      extension.testFailed(context, new AssertionError("expected"));
      extension.testAborted(context, new IllegalStateException("expected"));
      extension.testDisabled(context, Optional.of("expected"));
      nanoTime.set(9_000_000L);
      extension.afterAll(context);

      assertEquals(
          "[CORE-DB-SHARD] backend=h2 worker=worker/7 class="
              + SampleDatabaseTest.class.getName()
              + " tests=4 passed=1 failed=1 skipped=2 durationMs=8"
              + System.lineSeparator(),
          Files.readString(temporaryDirectory.resolve("worker-worker_7.log")));
    } finally {
      restoreProperty(
          CoreDatabaseShardTelemetryExtension.OUTPUT_DIRECTORY_PROPERTY, originalOutputDirectory);
      restoreProperty(CoreDatabaseShardTelemetryExtension.BACKEND_PROPERTY, originalBackend);
      restoreProperty("org.gradle.test.worker", originalWorker);
    }
  }

  private static void restoreProperty(String name, @Nullable String value) {
    if (value == null) {
      System.clearProperty(name);
    } else {
      System.setProperty(name, value);
    }
  }

  private static class SampleDatabaseTest {}
}
