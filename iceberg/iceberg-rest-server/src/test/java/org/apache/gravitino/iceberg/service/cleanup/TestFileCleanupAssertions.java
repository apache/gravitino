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
package org.apache.gravitino.iceberg.service.cleanup;

import java.io.UncheckedIOException;
import java.nio.file.AccessDeniedException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

/** Regression tests for file observations made while asynchronous cleanup removes files. */
// Files.find calls in static-mock stubs do not open resources; the assertion closes returned
// streams.
@SuppressWarnings("StreamResourceLeak")
public class TestFileCleanupAssertions {

  @TempDir private Path tempDir;

  @Test
  void testMissingAndEmptyDirectories() throws Exception {
    FileCleanupAssertions.assertNoRegularFiles(tempDir.resolve("missing"));
    Files.createDirectories(tempDir.resolve("metadata/empty"));
    FileCleanupAssertions.assertNoRegularFiles(tempDir);
  }

  @Test
  void testRemainingFilesFailTheAssertion() throws Exception {
    Files.writeString(tempDir.resolve("remaining.avro"), "manifest");
    Assertions.assertThrows(
        AssertionError.class, () -> FileCleanupAssertions.assertNoRegularFiles(tempDir));
  }

  @RepeatedTest(20)
  void testConcurrentDeletionRetriesWithoutTreatingAnIncompleteScanAsEmpty() throws Exception {
    Path remaining = tempDir.resolve("remaining.avro");
    AtomicInteger scans = new AtomicInteger();
    AtomicInteger closed = new AtomicInteger();
    try (MockedStatic<Files> files = Mockito.mockStatic(Files.class)) {
      files
          .when(() -> Files.find(Mockito.eq(tempDir), Mockito.eq(Integer.MAX_VALUE), Mockito.any()))
          .thenAnswer(
              invocation -> {
                int scan = scans.incrementAndGet();
                if (scan == 1) {
                  return failingStream(
                          new UncheckedIOException(new NoSuchFileException("manifest")))
                      .onClose(closed::incrementAndGet);
                }
                return (scan == 2 ? Stream.of(remaining) : Stream.<Path>empty())
                    .onClose(closed::incrementAndGet);
              });

      // Static mocks are thread-local, so keep polling on this test thread.
      Awaitility.await()
          .pollInSameThread()
          .pollDelay(Duration.ZERO)
          .pollInterval(Duration.ZERO)
          .atMost(Duration.ofSeconds(1))
          .untilAsserted(() -> FileCleanupAssertions.assertNoRegularFiles(tempDir));
    }
    Assertions.assertEquals(3, scans.get(), "Both the incomplete and nonempty scans must retry");
    Assertions.assertEquals(
        3, closed.get(), "Every traversal must be closed, including failed ones");
  }

  @Test
  void testRootDisappearingBeforeTraversalIsRetryable() throws Exception {
    NoSuchFileException missing = new NoSuchFileException(tempDir.toString());
    try (MockedStatic<Files> files = Mockito.mockStatic(Files.class)) {
      files
          .when(() -> Files.find(Mockito.eq(tempDir), Mockito.eq(Integer.MAX_VALUE), Mockito.any()))
          .thenThrow(missing);
      AssertionError error =
          Assertions.assertThrows(
              AssertionError.class, () -> FileCleanupAssertions.assertNoRegularFiles(tempDir));
      Assertions.assertSame(missing, error.getCause());
    }
  }

  @Test
  void testPermissionFailureOpeningTraversalIsNotRetryable() throws Exception {
    AccessDeniedException denied = new AccessDeniedException(tempDir.toString());
    try (MockedStatic<Files> files = Mockito.mockStatic(Files.class)) {
      files
          .when(() -> Files.find(Mockito.eq(tempDir), Mockito.eq(Integer.MAX_VALUE), Mockito.any()))
          .thenThrow(denied);
      UncheckedIOException error =
          Assertions.assertThrows(
              UncheckedIOException.class,
              () -> FileCleanupAssertions.assertNoRegularFiles(tempDir));
      Assertions.assertSame(denied, error.getCause());
    }
  }

  @Test
  void testPermissionFailureDuringTraversalIsNotRetryable() throws Exception {
    UncheckedIOException denied =
        new UncheckedIOException(new AccessDeniedException("metadata/manifest"));
    try (MockedStatic<Files> files = Mockito.mockStatic(Files.class)) {
      files
          .when(() -> Files.find(Mockito.eq(tempDir), Mockito.eq(Integer.MAX_VALUE), Mockito.any()))
          .thenReturn(failingStream(denied));
      Assertions.assertSame(
          denied,
          Assertions.assertThrows(
              UncheckedIOException.class,
              () -> FileCleanupAssertions.assertNoRegularFiles(tempDir)));
    }
  }

  private static Stream<Path> failingStream(UncheckedIOException failure) {
    Spliterator<Path> paths =
        new Spliterators.AbstractSpliterator<Path>(Long.MAX_VALUE, 0) {
          @Override
          public boolean tryAdvance(Consumer<? super Path> action) {
            throw failure;
          }
        };
    return StreamSupport.stream(paths, false);
  }
}
