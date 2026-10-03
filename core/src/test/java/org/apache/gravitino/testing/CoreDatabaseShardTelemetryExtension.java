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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Locale;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.LongSupplier;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.TestWatcher;

/** Records class-level Core database-test evidence for each Gradle test worker. */
public final class CoreDatabaseShardTelemetryExtension
    implements BeforeAllCallback, AfterAllCallback, TestWatcher {

  static final String OUTPUT_DIRECTORY_PROPERTY =
      "gravitino.core.database.shard.telemetry.directory";
  static final String BACKEND_PROPERTY = "gravitino.core.test.backend";

  private static final Object FILE_WRITE_LOCK = new Object();

  private final ConcurrentMap<Class<?>, ClassStatistics> statistics = new ConcurrentHashMap<>();
  private final LongSupplier nanoTime;

  /** Creates the extension using the system monotonic clock. */
  public CoreDatabaseShardTelemetryExtension() {
    this(System::nanoTime);
  }

  CoreDatabaseShardTelemetryExtension(LongSupplier nanoTime) {
    this.nanoTime = nanoTime;
  }

  @Override
  public void beforeAll(ExtensionContext context) {
    if (enabled()) {
      statistics.put(
          context.getRequiredTestClass(),
          new ClassStatistics(context.getRequiredTestClass().getName(), nanoTime.getAsLong()));
    }
  }

  @Override
  public void afterAll(ExtensionContext context) throws IOException {
    if (!enabled()) {
      return;
    }

    Class<?> testClass = context.getRequiredTestClass();
    ClassStatistics classStatistics =
        statistics.computeIfAbsent(
            testClass, ignored -> new ClassStatistics(testClass.getName(), nanoTime.getAsLong()));
    statistics.remove(testClass);

    String backend = System.getProperty(BACKEND_PROPERTY, "unknown");
    String worker = System.getProperty("org.gradle.test.worker", "unknown");
    String record = classStatistics.toRecord(backend, worker, nanoTime.getAsLong());
    writeRecord(Paths.get(System.getProperty(OUTPUT_DIRECTORY_PROPERTY)), worker, record);
  }

  @Override
  public void testSuccessful(ExtensionContext context) {
    statistics(context).recordPassed();
  }

  @Override
  public void testAborted(ExtensionContext context, Throwable cause) {
    statistics(context).recordSkipped();
  }

  @Override
  public void testDisabled(ExtensionContext context, Optional<String> reason) {
    statistics(context).recordSkipped();
  }

  @Override
  public void testFailed(ExtensionContext context, Throwable cause) {
    statistics(context).recordFailed();
  }

  private boolean enabled() {
    String outputDirectory = System.getProperty(OUTPUT_DIRECTORY_PROPERTY);
    return outputDirectory != null && !outputDirectory.trim().isEmpty();
  }

  private ClassStatistics statistics(ExtensionContext context) {
    Class<?> testClass = context.getRequiredTestClass();
    return statistics.computeIfAbsent(
        testClass, ignored -> new ClassStatistics(testClass.getName(), nanoTime.getAsLong()));
  }

  static void writeRecord(Path outputDirectory, String worker, String record) throws IOException {
    Files.createDirectories(outputDirectory);
    String safeWorker = worker.replaceAll("[^A-Za-z0-9._-]", "_");
    Path workerRecord = outputDirectory.resolve("worker-" + safeWorker + ".log");
    synchronized (FILE_WRITE_LOCK) {
      Files.write(
          workerRecord,
          (record + System.lineSeparator()).getBytes(StandardCharsets.UTF_8),
          StandardOpenOption.CREATE,
          StandardOpenOption.APPEND);
    }
  }

  static final class ClassStatistics {
    private final String testClass;
    private final long startNanos;
    private final LongAdder passed = new LongAdder();
    private final LongAdder failed = new LongAdder();
    private final LongAdder skipped = new LongAdder();

    ClassStatistics(String testClass, long startNanos) {
      this.testClass = testClass;
      this.startNanos = startNanos;
    }

    void recordPassed() {
      passed.increment();
    }

    void recordFailed() {
      failed.increment();
    }

    void recordSkipped() {
      skipped.increment();
    }

    String toRecord(String backend, String worker, long endNanos) {
      long durationNanos = Math.max(0L, endNanos - startNanos);
      long passedCount = passed.sum();
      long failedCount = failed.sum();
      long skippedCount = skipped.sum();
      return String.format(
          Locale.ROOT,
          "[CORE-DB-SHARD] backend=%s worker=%s class=%s "
              + "tests=%d passed=%d failed=%d skipped=%d durationMs=%d",
          backend,
          worker,
          testClass,
          passedCount + failedCount + skippedCount,
          passedCount,
          failedCount,
          skippedCount,
          TimeUnit.NANOSECONDS.toMillis(durationNanos));
    }
  }
}
