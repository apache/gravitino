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

import com.sun.net.httpserver.HttpServer;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestFetchFileUtils {

  // The hdfs happy path is exercised reflectively against Hadoop and is covered by the catalog
  // Kerberos integration tests; the common module has no Hadoop on its test classpath, so here we
  // only assert that an hdfs uri without a Hadoop configuration is rejected.
  private static final String UNSAFE_HINT = "'demo.block-unsafe-address' to false";

  @TempDir File tempDir;

  @Test
  public void testLinkLocalFile() throws Exception {
    File srcFile = new File(tempDir, "source");
    Assertions.assertTrue(srcFile.createNewFile());
    File destFile = new File(tempDir, "dest");

    FetchFileUtils.fetchFileFromUri(
        srcFile.toURI().toString(), destFile, 10, null, true, UNSAFE_HINT);
    Assertions.assertTrue(Files.isSymbolicLink(destFile.toPath()));
    Assertions.assertEquals(
        srcFile.toPath().normalize(), Files.readSymbolicLink(destFile.toPath()).normalize());
  }

  @Test
  public void testMissingLocalFileShouldFail() {
    File destFile = new File(tempDir, "dest_missing");
    String uri = new File(tempDir, "does-not-exist-" + UUID.randomUUID()).toURI().toString();

    Assertions.assertThrows(
        IOException.class,
        () -> FetchFileUtils.fetchFileFromUri(uri, destFile, 10, null, true, UNSAFE_HINT));
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
                    barrier.await(30, TimeUnit.SECONDS);
                    FetchFileUtils.fetchFileFromUri(
                        srcFile.toURI().toString(), destFile, 10, null, true, UNSAFE_HINT);
                  } catch (Exception e) {
                    throw new RuntimeException(e);
                  }
                }));
      }
      for (Future<?> future : futures) {
        future.get(30, TimeUnit.SECONDS);
      }
    } finally {
      executor.shutdownNow();
    }

    Assertions.assertTrue(Files.isSymbolicLink(destFile.toPath()));
    Assertions.assertEquals(
        srcFile.toPath().normalize(), Files.readSymbolicLink(destFile.toPath()).normalize());
  }

  @Test
  public void testIdempotentSymlinkCreation() throws Exception {
    File srcFile = new File(tempDir, "source_idempotent");
    Assertions.assertTrue(srcFile.createNewFile());
    File destFile = new File(tempDir, "dest_idempotent");
    String uri = srcFile.toURI().toString();

    FetchFileUtils.fetchFileFromUri(uri, destFile, 10, null, true, UNSAFE_HINT);
    Assertions.assertTrue(Files.isSymbolicLink(destFile.toPath()));

    // Second call to the same dest should succeed without error.
    FetchFileUtils.fetchFileFromUri(uri, destFile, 10, null, true, UNSAFE_HINT);
    Assertions.assertTrue(Files.isSymbolicLink(destFile.toPath()));
    Assertions.assertEquals(
        srcFile.toPath().normalize(), Files.readSymbolicLink(destFile.toPath()).normalize());
  }

  @Test
  public void testSymlinkReplacedWithDifferentTarget() throws Exception {
    File srcFileA = new File(tempDir, "source_a");
    File srcFileB = new File(tempDir, "source_b");
    Assertions.assertTrue(srcFileA.createNewFile());
    Assertions.assertTrue(srcFileB.createNewFile());
    File destFile = new File(tempDir, "dest_replace");

    FetchFileUtils.fetchFileFromUri(
        srcFileA.toURI().toString(), destFile, 10, null, true, UNSAFE_HINT);
    Assertions.assertEquals(
        srcFileA.toPath().normalize(), Files.readSymbolicLink(destFile.toPath()).normalize());

    FetchFileUtils.fetchFileFromUri(
        srcFileB.toURI().toString(), destFile, 10, null, true, UNSAFE_HINT);
    Assertions.assertEquals(
        srcFileB.toPath().normalize(), Files.readSymbolicLink(destFile.toPath()).normalize());
  }

  @Test
  public void testRemoteFetchShouldBlockLocalhostByDefault() {
    File destFile = new File(tempDir, "blocked");

    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                FetchFileUtils.fetchFileFromUri(
                    "http://127.0.0.1:8090/keytab", destFile, 10, null, true, UNSAFE_HINT));

    Assertions.assertTrue(exception.getMessage().contains("Gravitino server side"));
    Assertions.assertTrue(exception.getMessage().contains(UNSAFE_HINT));
  }

  @Test
  public void testRemoteFetchShouldAllowLocalhostWhenBlockingDisabled() throws Exception {
    File destFile = new File(tempDir, "allowed");
    HttpServer server = createLoopbackHttpServer("keytab");

    try {
      server.start();
      int port = server.getAddress().getPort();

      FetchFileUtils.fetchFileFromUri(
          String.format("http://127.0.0.1:%d/keytab", port),
          destFile,
          30000 /* 30s connect/read timeout in milliseconds */,
          null,
          false,
          UNSAFE_HINT);

      Assertions.assertEquals("keytab", Files.readString(destFile.toPath()));
    } finally {
      server.stop(0);
    }
  }

  @Test
  public void testHdfsSchemeWithoutConfShouldFail() {
    File destFile = new File(tempDir, "dest_hdfs");

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            FetchFileUtils.fetchFileFromUri(
                "hdfs://namenode/keytab", destFile, 10, null, true, UNSAFE_HINT));
  }

  @Test
  public void testUnsupportedSchemeShouldFail() {
    File destFile = new File(tempDir, "dest_scp");

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            FetchFileUtils.fetchFileFromUri(
                "scp://host/keytab", destFile, 10, null, true, UNSAFE_HINT));
  }

  @Test
  public void testReturnsDestinationAbsolutePath() throws Exception {
    File srcFile = new File(tempDir, "source_return");
    Assertions.assertTrue(srcFile.createNewFile());
    File destFile = new File(tempDir, "dest_return");

    String returned =
        FetchFileUtils.fetchFileFromUri(
            srcFile.toURI().toString(), destFile, 10, null, true, UNSAFE_HINT);
    Assertions.assertEquals(destFile.getAbsolutePath(), returned);
  }

  @Test
  public void testRemoveLock() throws Exception {
    File srcFile = new File(tempDir, "source_lock");
    Assertions.assertTrue(srcFile.createNewFile());
    File destFile = new File(tempDir, "dest_lock");

    FetchFileUtils.fetchFileFromUri(
        srcFile.toURI().toString(), destFile, 10, null, true, UNSAFE_HINT);
    // removeLock should be a no-op-safe cleanup that does not throw and does not affect the link.
    FetchFileUtils.removeLock(destFile);
    Path destPath = destFile.toPath();
    Assertions.assertTrue(Files.isSymbolicLink(destPath));
  }

  private HttpServer createLoopbackHttpServer(String response) throws Exception {
    HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext(
        "/keytab",
        exchange -> {
          byte[] bytes = response.getBytes(StandardCharsets.UTF_8);
          exchange.sendResponseHeaders(200, bytes.length);
          try (OutputStream outputStream = exchange.getResponseBody()) {
            outputStream.write(bytes);
          }
        });
    return server;
  }
}
