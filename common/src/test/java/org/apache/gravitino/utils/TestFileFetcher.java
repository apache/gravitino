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
import java.nio.file.Paths;
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

/**
 * Tests for {@link FileFetcher}.
 *
 * <p>The hdfs happy path is exercised reflectively against Hadoop and is covered by the catalog
 * Kerberos integration tests; the common module has no Hadoop on its test classpath, so here we
 * only assert that an hdfs uri without a Hadoop configuration is rejected.
 */
public class TestFileFetcher {

  @TempDir File tempDir;

  @Test
  public void testBlockUnsafeRemoteUriConfigName() {
    Assertions.assertEquals(
        "gravitino.fetchFile.blockUnsafeRemoteUri", FileFetcher.BLOCK_UNSAFE_REMOTE_URI_CONFIG);
  }

  @Test
  public void testGetShouldReturnSingleton() {
    Assertions.assertSame(FileFetcher.get(), FileFetcher.get());
  }

  @Test
  public void testLinkLocalFile() throws Exception {
    File srcFile = new File(tempDir, "source");
    Assertions.assertTrue(srcFile.createNewFile());
    File destFile = new File(tempDir, "dest");

    FileFetcher.get().fetchFileFromUri(srcFile.toURI().toString(), destFile, 10, null);
    Assertions.assertTrue(Files.isSymbolicLink(destFile.toPath()));
    Assertions.assertEquals(
        srcFile.toPath().normalize(), Files.readSymbolicLink(destFile.toPath()).normalize());
  }

  @Test
  public void testOpaqueFileUriWithNullPathShouldFail() {
    // An opaque file URI (no authority/path) must fail with a clear IOException, not a raw NPE.
    File destFile = new File(tempDir, "dest_opaque");
    Assertions.assertThrows(
        IOException.class,
        () -> FileFetcher.get().fetchFileFromUri("file:relative", destFile, 10, null));
  }

  @Test
  public void testMissingLocalFileShouldFail() {
    File destFile = new File(tempDir, "dest_missing");
    String uri = new File(tempDir, "does-not-exist-" + UUID.randomUUID()).toURI().toString();

    Assertions.assertThrows(
        IOException.class, () -> FileFetcher.get().fetchFileFromUri(uri, destFile, 10, null));
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
                    FileFetcher.get()
                        .fetchFileFromUri(srcFile.toURI().toString(), destFile, 10, null);
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

    FileFetcher.get().fetchFileFromUri(uri, destFile, 10, null);
    Assertions.assertTrue(Files.isSymbolicLink(destFile.toPath()));

    // Second call to the same dest should succeed without error.
    FileFetcher.get().fetchFileFromUri(uri, destFile, 10, null);
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

    FileFetcher.get().fetchFileFromUri(srcFileA.toURI().toString(), destFile, 10, null);
    Assertions.assertEquals(
        srcFileA.toPath().normalize(), Files.readSymbolicLink(destFile.toPath()).normalize());

    FileFetcher.get().fetchFileFromUri(srcFileB.toURI().toString(), destFile, 10, null);
    Assertions.assertEquals(
        srcFileB.toPath().normalize(), Files.readSymbolicLink(destFile.toPath()).normalize());
  }

  @Test
  public void testRemoteFetchShouldBlockLocalhostByDefault() {
    File destFile = new File(tempDir, "blocked");
    FileFetcher.get().initialize(true);

    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                FileFetcher.get()
                    .fetchFileFromUri("http://127.0.0.1:8090/keytab", destFile, 10, null));

    Assertions.assertTrue(exception.getMessage().contains("Gravitino server side"));
    Assertions.assertTrue(
        exception.getMessage().contains(FileFetcher.BLOCK_UNSAFE_REMOTE_URI_CONFIG));
  }

  @Test
  public void testRemoteFetchShouldAllowLocalhostWhenBlockingDisabled() throws Exception {
    File destFile = new File(tempDir, "allowed");
    HttpServer server = createLoopbackHttpServer("keytab");

    try {
      server.start();
      int port = server.getAddress().getPort();
      FileFetcher.get().initialize(false);
      FileFetcher.get()
          .fetchFileFromUri(
              String.format("http://127.0.0.1:%d/keytab", port),
              destFile,
              30000 /* 30s connect/read timeout in milliseconds */,
              null);

      Assertions.assertEquals("keytab", Files.readString(destFile.toPath()));
    } finally {
      FileFetcher.get().initialize(true);
      server.stop(0);
    }
  }

  @Test
  public void testRelativeLocalSourceProducesResolvableSymlink() throws Exception {
    File srcFile = new File(tempDir, "rel_src");
    Assertions.assertTrue(srcFile.createNewFile());
    File destFile = new File(tempDir, "rel_dest");
    // A relative source path (as an operator might configure) must still produce a symlink that
    // resolves to the real file, not a dangling one.
    Path cwd = Paths.get("").toAbsolutePath();
    String relative = cwd.relativize(srcFile.toPath().toAbsolutePath()).toString();

    FileFetcher.get().fetchFileFromUri(relative, destFile, 10, null);

    Assertions.assertTrue(Files.isSymbolicLink(destFile.toPath()));
    Assertions.assertTrue(
        Files.exists(destFile.toPath()), "symlink must resolve to an existing file, not dangle");
  }

  @Test
  public void testLocalFileMoveFailureCleansTempSymlink() throws Exception {
    File srcFile = new File(tempDir, "src_movefail");
    Assertions.assertTrue(srcFile.createNewFile());
    // Make the destination a non-empty directory so the final atomic rename fails.
    File destDir = new File(tempDir, "dest_is_dir");
    Assertions.assertTrue(destDir.mkdir());
    Assertions.assertTrue(new File(destDir, "occupant").createNewFile());

    Assertions.assertThrows(
        IOException.class,
        () -> FileFetcher.get().fetchFileFromUri(srcFile.toURI().toString(), destDir, 10, null));

    File tmpSymlink = new File(tempDir, "dest_is_dir.symlink.tmp");
    Assertions.assertFalse(
        tmpSymlink.exists(), "temp symlink must be cleaned up after a failed move");
  }

  @Test
  public void testUpperCaseSchemeIsCaseInsensitive() {
    // RFC 3986 schemes are case-insensitive: "HTTP" must route like "http" (i.e. through the SSRF
    // validator), not fall through to the unsupported-scheme branch.
    File destFile = new File(tempDir, "uppercase");
    FileFetcher.get().initialize(true);

    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                FileFetcher.get().fetchFileFromUri("HTTP://127.0.0.1/keytab", destFile, 10, null));
    Assertions.assertTrue(exception.getMessage().contains("Gravitino server side"));
  }

  @Test
  public void testFtpSchemeRejectedWhenBlockingEnabled() {
    // FTP's data channel cannot be pinned to the validated address (PASV/EPSV SSRF), so it must be
    // rejected on the blocking path rather than handed to the JDK FTP client.
    File destFile = new File(tempDir, "ftp");
    FileFetcher.get().initialize(true);

    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                FileFetcher.get()
                    .fetchFileFromUri("ftp://files.example.com/keytab", destFile, 10, null));
    Assertions.assertTrue(exception.getMessage().contains("FTP"));
    Assertions.assertTrue(
        exception.getMessage().contains(FileFetcher.BLOCK_UNSAFE_REMOTE_URI_CONFIG));
  }

  @Test
  public void testHdfsSchemeWithoutConfShouldFail() {
    File destFile = new File(tempDir, "dest_hdfs");

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> FileFetcher.get().fetchFileFromUri("hdfs://namenode/keytab", destFile, 10, null));
  }

  @Test
  public void testUnsupportedSchemeShouldFail() {
    File destFile = new File(tempDir, "dest_scp");

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> FileFetcher.get().fetchFileFromUri("scp://host/keytab", destFile, 10, null));
  }

  @Test
  public void testReturnsDestinationAbsolutePath() throws Exception {
    File srcFile = new File(tempDir, "source_return");
    Assertions.assertTrue(srcFile.createNewFile());
    File destFile = new File(tempDir, "dest_return");

    String returned =
        FileFetcher.get().fetchFileFromUri(srcFile.toURI().toString(), destFile, 10, null);
    Assertions.assertEquals(destFile.getAbsolutePath(), returned);
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
