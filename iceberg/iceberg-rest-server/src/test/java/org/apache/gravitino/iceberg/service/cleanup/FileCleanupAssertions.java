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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.stream.Stream;
import org.junit.jupiter.api.Assertions;

/** File assertions for tests that observe asynchronous cleanup. */
public final class FileCleanupAssertions {

  private static final String CONCURRENT_DELETION =
      "Files disappeared during the cleanup scan; retry the complete assertion";

  private FileCleanupAssertions() {}

  /**
   * Asserts that no regular files remain, allowing {@code untilAsserted} to retry an incomplete
   * scan caused by concurrent deletion. Other I/O failures propagate rather than looking empty.
   *
   * @param directory directory being cleaned asynchronously
   */
  public static void assertNoRegularFiles(Path directory) {
    if (Files.notExists(directory)) {
      return;
    }
    try (Stream<Path> paths =
        Files.find(
            directory, Integer.MAX_VALUE, (path, attributes) -> attributes.isRegularFile())) {
      Assertions.assertEquals(
          0L, paths.count(), "Background cleanup worker should have deleted all table files");
    } catch (NoSuchFileException e) {
      // The root can disappear after notExists, before the traversal opens it.
      throw new AssertionError(CONCURRENT_DELETION, e);
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to scan cleanup directory " + directory, e);
    } catch (UncheckedIOException e) {
      // A descendant can disappear while the lazy traversal reads its attributes.
      if (e.getCause() instanceof NoSuchFileException) {
        throw new AssertionError(CONCURRENT_DELETION, e);
      }
      throw e;
    }
  }
}
