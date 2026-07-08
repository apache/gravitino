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

package org.apache.gravitino.utils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestDirectoryUtils {

  @TempDir private File tempDir;

  @Test
  public void testEnsureDirectoryCreatesNestedDirectories() throws IOException {
    File nested = new File(tempDir, "a/b/c");

    DirectoryUtils.ensureDirectory(nested);

    Assertions.assertTrue(nested.isDirectory());
  }

  @Test
  public void testEnsureDirectoryIsIdempotent() throws IOException {
    File dir = new File(tempDir, "existing");
    Assertions.assertTrue(dir.mkdirs());

    DirectoryUtils.ensureDirectory(dir);

    Assertions.assertTrue(dir.isDirectory());
  }

  @Test
  public void testEnsureDirectoryRejectsRegularFile() throws IOException {
    File file = new File(tempDir, "regular-file");
    Files.writeString(file.toPath(), "content");

    Assertions.assertThrows(IOException.class, () -> DirectoryUtils.ensureDirectory(file));
  }

  @Test
  public void testEnsureDirectoryToleratesConcurrentCreation() throws Exception {
    int threads = 4;
    int iterations = 200;
    ExecutorService executor = Executors.newFixedThreadPool(threads);
    try {
      for (int i = 0; i < iterations; i++) {
        File dir = new File(tempDir, "race-" + i + "/target");
        CyclicBarrier barrier = new CyclicBarrier(threads);
        List<Future<?>> futures = new ArrayList<>(threads);
        for (int t = 0; t < threads; t++) {
          futures.add(
              executor.submit(
                  () -> {
                    barrier.await();
                    DirectoryUtils.ensureDirectory(dir);
                    return null;
                  }));
        }
        for (Future<?> future : futures) {
          future.get();
        }
        Assertions.assertTrue(dir.isDirectory());
      }
    } finally {
      executor.shutdownNow();
    }
  }
}
