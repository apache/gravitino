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
import java.time.Instant;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Records class-level Core database-test evidence for each Gradle test worker. */
public final class CoreDatabaseShardTelemetryExtension
    implements BeforeAllCallback, AfterAllCallback, TestWatcher {

  private static final Logger LOG =
      LoggerFactory.getLogger(CoreDatabaseShardTelemetryExtension.class);

  static final String OUTPUT_DIRECTORY_PROPERTY =
      "gravitino.core.database.shard.telemetry.directory";
  static final String BACKEND_PROPERTY = "gravitino.core.test.backend";

  /**
   * Optional run identifier (e.g. a CI run id) so timing records from one invocation can be grouped
   * when aggregated across many runs later. Best-effort; omitted from the JSON record when unset.
   */
  static final String RUN_ID_PROPERTY = "gravitino.core.database.shard.telemetry.runId";

  /** Optional git commit sha, best-effort, omitted from the JSON record when unset. */
  static final String GIT_COMMIT_PROPERTY = "gravitino.core.database.shard.telemetry.gitCommit";

  /** Bump when the JSON record's field set changes in a way old consumers must know about. */
  static final int JSON_SCHEMA_VERSION = 1;

  private static final Object FILE_WRITE_LOCK = new Object();
  private static final Object JSON_FILE_WRITE_LOCK = new Object();

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
    long endNanos = nanoTime.getAsLong();
    String record = classStatistics.toRecord(backend, worker, endNanos);
    Path outputDirectory = Paths.get(System.getProperty(OUTPUT_DIRECTORY_PROPERTY));
    writeRecord(outputDirectory, worker, record);

    // Best-effort structured (JSON Lines) sibling of the human-readable record above, for
    // later cross-run aggregation. Must never fail the build: any error here is logged and
    // swallowed, never rethrown.
    try {
      String jsonRecord =
          classStatistics.toJsonRecord(
              backend,
              worker,
              endNanos,
              System.getProperty(RUN_ID_PROPERTY),
              System.getProperty(GIT_COMMIT_PROPERTY));
      writeJsonRecord(outputDirectory, worker, jsonRecord);
    } catch (RuntimeException | IOException e) {
      LOG.warn("Failed to write core-db-shard JSON telemetry record (non-fatal)", e);
    }
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

  /**
   * Writes the structured JSON Lines sibling record. Each Gradle test worker (JVM fork) gets its
   * own file, matching {@link #writeRecord}, so concurrent forks never interleave writes into the
   * same file -- callers aggregate across worker-N.jsonl files downstream, no cross-process
   * locking/merging needed at write time.
   */
  static void writeJsonRecord(Path outputDirectory, String worker, String jsonRecord)
      throws IOException {
    Files.createDirectories(outputDirectory);
    String safeWorker = worker.replaceAll("[^A-Za-z0-9._-]", "_");
    Path workerRecord = outputDirectory.resolve("worker-" + safeWorker + ".jsonl");
    synchronized (JSON_FILE_WRITE_LOCK) {
      Files.write(
          workerRecord,
          (jsonRecord + System.lineSeparator()).getBytes(StandardCharsets.UTF_8),
          StandardOpenOption.CREATE,
          StandardOpenOption.APPEND);
    }
  }

  /** Minimal JSON string escaping for the handful of fields we ever emit (no control chars). */
  private static String jsonEscape(String value) {
    return value.replace("\\", "\\\\").replace("\"", "\\\"");
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

    /**
     * JSON Lines sibling of {@link #toRecord}, same field names/values (backend, worker, class,
     * tests, passed, failed, skipped, durationMs) plus schemaVersion/timestamp for cross-run
     * aggregation, and optional runId/gitCommit when the caller supplies them. {@code runId} and
     * {@code gitCommit} may be {@code null} -- both are omitted from the record entirely rather
     * than emitted as {@code null} literals, so downstream consumers don't need to special-case
     * JSON null for fields that were simply not provided.
     */
    String toJsonRecord(
        String backend, String worker, long endNanos, String runId, String gitCommit) {
      long durationNanos = Math.max(0L, endNanos - startNanos);
      long passedCount = passed.sum();
      long failedCount = failed.sum();
      long skippedCount = skipped.sum();
      StringBuilder json = new StringBuilder(256);
      json.append('{');
      json.append("\"schemaVersion\":").append(JSON_SCHEMA_VERSION).append(',');
      json.append("\"timestamp\":\"").append(Instant.now()).append("\",");
      json.append("\"backend\":\"").append(jsonEscape(backend)).append("\",");
      json.append("\"worker\":\"").append(jsonEscape(worker)).append("\",");
      json.append("\"class\":\"").append(jsonEscape(testClass)).append("\",");
      json.append("\"tests\":").append(passedCount + failedCount + skippedCount).append(',');
      json.append("\"passed\":").append(passedCount).append(',');
      json.append("\"failed\":").append(failedCount).append(',');
      json.append("\"skipped\":").append(skippedCount).append(',');
      json.append("\"durationMs\":").append(TimeUnit.NANOSECONDS.toMillis(durationNanos));
      if (runId != null && !runId.trim().isEmpty()) {
        json.append(",\"runId\":\"").append(jsonEscape(runId)).append('"');
      }
      if (gitCommit != null && !gitCommit.trim().isEmpty()) {
        json.append(",\"gitCommit\":\"").append(jsonEscape(gitCommit)).append('"');
      }
      json.append('}');
      return json.toString();
    }
  }
}
