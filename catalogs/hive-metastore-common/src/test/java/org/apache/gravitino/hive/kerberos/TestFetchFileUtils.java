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
package org.apache.gravitino.hive.kerberos;

import java.io.File;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestFetchFileUtils {

  @TempDir File tempDir;

  @Test
  public void testLinkLocalFile() throws Exception {
    File srcFile = new File(tempDir, "source");
    Assertions.assertTrue(srcFile.createNewFile());
    File destFile = new File(tempDir, "dest");

    FetchFileUtils.fetchFileFromUri(srcFile.toURI().toString(), destFile, 10, new Configuration());
    Assertions.assertTrue(Files.isSymbolicLink(destFile.toPath()));
    Assertions.assertEquals(srcFile.toPath(), Files.readSymbolicLink(destFile.toPath()));
  }

  @Test
  public void testConcurrentSymlinkCreation() throws Exception {
    File srcFile = new File(tempDir, "source_concurrent");
    Assertions.assertTrue(srcFile.createNewFile());
    File destFile = new File(tempDir, "dest_concurrent");

    int threadCount = 10;
    CyclicBarrier barrier = new CyclicBarrier(threadCount);
    ExecutorService executor = Executors.newFixedThreadPool(threadCount);
    List<Future<?>> futures = new ArrayList<>();
    try {
      for (int i = 0; i < threadCount; i++) {
        futures.add(
            executor.submit(
                () -> {
                  try {
                    barrier.await();
                    FetchFileUtils.fetchFileFromUri(
                        srcFile.toURI().toString(), destFile, 10, new Configuration());
                  } catch (Exception e) {
                    throw new RuntimeException(e);
                  }
                }));
      }
      for (Future<?> future : futures) {
        future.get();
      }
    } finally {
      executor.shutdownNow();
    }

    Assertions.assertTrue(Files.isSymbolicLink(destFile.toPath()));
    Assertions.assertEquals(srcFile.toPath(), Files.readSymbolicLink(destFile.toPath()));
  }

  @Test
  public void testIdempotentSymlinkCreation() throws Exception {
    File srcFile = new File(tempDir, "source_idempotent");
    Assertions.assertTrue(srcFile.createNewFile());
    File destFile = new File(tempDir, "dest_idempotent");

    Configuration conf = new Configuration();
    String uri = srcFile.toURI().toString();

    FetchFileUtils.fetchFileFromUri(uri, destFile, 10, conf);
    Assertions.assertTrue(Files.isSymbolicLink(destFile.toPath()));

    // Second call to the same dest should succeed without error
    FetchFileUtils.fetchFileFromUri(uri, destFile, 10, conf);
    Assertions.assertTrue(Files.isSymbolicLink(destFile.toPath()));
    Assertions.assertEquals(srcFile.toPath(), Files.readSymbolicLink(destFile.toPath()));
  }

  @Test
  public void testSymlinkReplacedWithDifferentTarget() throws Exception {
    File srcFileA = new File(tempDir, "source_a");
    File srcFileB = new File(tempDir, "source_b");
    Assertions.assertTrue(srcFileA.createNewFile());
    Assertions.assertTrue(srcFileB.createNewFile());
    File destFile = new File(tempDir, "dest_replace");

    Configuration conf = new Configuration();

    FetchFileUtils.fetchFileFromUri(srcFileA.toURI().toString(), destFile, 10, conf);
    Assertions.assertEquals(srcFileA.toPath(), Files.readSymbolicLink(destFile.toPath()));

    FetchFileUtils.fetchFileFromUri(srcFileB.toURI().toString(), destFile, 10, conf);
    Assertions.assertEquals(srcFileB.toPath(), Files.readSymbolicLink(destFile.toPath()));
  }
}
