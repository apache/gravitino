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
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests for {@link RemoteFileDownloader}.
 *
 * <p>The {@code https} scheme is exercised end-to-end with TLS fixtures by the catalog Kerberos
 * integration tests. Here the {@code http} path is covered directly; the only {@code
 * https}-specific production logic is the small {@code startTls} step (SNI and {@code HTTPS}
 * endpoint identification over an already-pinned socket), which then reuses exactly the same
 * status/header/body parsing verified by these {@code http} tests.
 */
public class TestRemoteFileDownloader {

  @TempDir File tempDir;

  @Test
  public void testHttpDownloadUsesPinnedAddressNotHostnameResolution() throws Exception {
    HttpServer server = createLoopbackServer("keytab-content", new AtomicReference<>());
    try {
      server.start();
      int port = server.getAddress().getPort();
      File dest = new File(tempDir, "pinned");
      // The host is a guaranteed-unresolvable name (RFC 6761 reserves .invalid). A successful fetch
      // therefore proves the download connected to the pinned loopback address rather than
      // re-resolving the hostname (which is the DNS-rebinding TOCTOU this guards against).
      URI uri = new URI("http://ssrf-rebind.invalid:" + port + "/keytab");
      InetAddress pinned = InetAddress.getByName("127.0.0.1");

      RemoteFileDownloader.download(uri, pinned, dest, 30000);

      Assertions.assertEquals("keytab-content", Files.readString(dest.toPath()));
      // The temp file must be atomically moved into place, leaving no ".tmp" sibling behind.
      File[] leftovers = tempDir.listFiles((d, name) -> name.endsWith(".tmp"));
      Assertions.assertNotNull(leftovers);
      Assertions.assertEquals(0, leftovers.length, "no temp file should remain after success");
    } finally {
      server.stop(0);
    }
  }

  @Test
  public void testHttpDownloadSendsOriginalHostHeader() throws Exception {
    AtomicReference<String> hostHeader = new AtomicReference<>();
    HttpServer server = createLoopbackServer("data", hostHeader);
    try {
      server.start();
      int port = server.getAddress().getPort();
      File dest = new File(tempDir, "host-header");
      URI uri = new URI("http://ssrf-rebind.invalid:" + port + "/keytab");
      InetAddress pinned = InetAddress.getByName("127.0.0.1");

      RemoteFileDownloader.download(uri, pinned, dest, 30000);

      // The original hostname must still be sent as the Host header, so virtual-hosted servers
      // continue to route correctly even though we connected to a pinned IP.
      Assertions.assertEquals("ssrf-rebind.invalid:" + port, hostHeader.get());
    } finally {
      server.stop(0);
    }
  }

  @Test
  public void testHttpDownloadRejectsNonSuccessStatus() throws Exception {
    HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext(
        "/keytab",
        exchange -> {
          exchange.sendResponseHeaders(404, -1);
          exchange.close();
        });
    try {
      server.start();
      int port = server.getAddress().getPort();
      File dest = new File(tempDir, "missing");
      URI uri = new URI("http://ssrf-rebind.invalid:" + port + "/keytab");
      InetAddress pinned = InetAddress.getByName("127.0.0.1");

      Assertions.assertThrows(
          IOException.class, () -> RemoteFileDownloader.download(uri, pinned, dest, 30000));
      Assertions.assertFalse(dest.exists(), "no destination file should be left on failure");
    } finally {
      server.stop(0);
    }
  }

  @Test
  public void testHttpDownloadRejectsRedirect() throws Exception {
    HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext(
        "/keytab",
        exchange -> {
          // A redirect target would re-resolve to an unvalidated address, so it must be rejected.
          exchange.getResponseHeaders().add("Location", "http://example.com/elsewhere");
          exchange.sendResponseHeaders(302, -1);
          exchange.close();
        });
    try {
      server.start();
      int port = server.getAddress().getPort();
      File dest = new File(tempDir, "redirect");
      URI uri = new URI("http://ssrf-rebind.invalid:" + port + "/keytab");
      InetAddress pinned = InetAddress.getByName("127.0.0.1");

      Assertions.assertThrows(
          IOException.class, () -> RemoteFileDownloader.download(uri, pinned, dest, 30000));
      Assertions.assertFalse(dest.exists());
    } finally {
      server.stop(0);
    }
  }

  @Test
  public void testHttpDownloadDecodesChunkedResponse() throws Exception {
    // An explicit chunked response ("Wiki" + "pedia") must be de-chunked to "Wikipedia"; if the
    // chunk framing were written verbatim the file would be corrupted.
    byte[] response =
        ("HTTP/1.1 200 OK\r\n"
                + "Transfer-Encoding: chunked\r\n"
                + "Connection: close\r\n"
                + "\r\n"
                + "4\r\nWiki\r\n"
                + "5\r\npedia\r\n"
                + "0\r\n"
                + "\r\n")
            .getBytes(StandardCharsets.US_ASCII);
    try (RawServer server = new RawServer(response)) {
      File dest = new File(tempDir, "chunked");
      URI uri = new URI("http://ssrf-rebind.invalid:" + server.port() + "/keytab");
      InetAddress pinned = InetAddress.getByName("127.0.0.1");

      RemoteFileDownloader.download(uri, pinned, dest, 30000);

      Assertions.assertEquals("Wikipedia", Files.readString(dest.toPath()));
    }
  }

  @Test
  public void testChunkedFramingTakesPrecedenceOverContentLength() throws Exception {
    // TE.CL ambiguity (both Transfer-Encoding: chunked and Content-Length present): chunked must
    // win (RFC 7230), so the body de-chunks to "Wikipedia", not 4 bytes.
    byte[] response =
        ("HTTP/1.1 200 OK\r\n"
                + "Transfer-Encoding: chunked\r\n"
                + "Content-Length: 4\r\n"
                + "Connection: close\r\n"
                + "\r\n"
                + "4\r\nWiki\r\n"
                + "5\r\npedia\r\n"
                + "0\r\n\r\n")
            .getBytes(StandardCharsets.US_ASCII);
    try (RawServer server = new RawServer(response)) {
      File dest = new File(tempDir, "te-cl");
      URI uri = new URI("http://ssrf-rebind.invalid:" + server.port() + "/keytab");
      InetAddress pinned = InetAddress.getByName("127.0.0.1");

      RemoteFileDownloader.download(uri, pinned, dest, 30000);

      Assertions.assertEquals("Wikipedia", Files.readString(dest.toPath()));
    }
  }

  @Test
  public void testHttpDownloadRejectsAccumulatedOversizedHeaders() throws Exception {
    // Many header lines, each well under the per-line cap, whose total exceeds the 64 KiB header
    // cap must be rejected (a distinct guard from the per-line cap).
    StringBuilder sb = new StringBuilder("HTTP/1.0 200 OK\r\n");
    String value = new String(new char[1000]).replace('\0', 'A');
    for (int i = 0; i < 70; i++) {
      sb.append("X-Pad-").append(i).append(": ").append(value).append("\r\n");
    }
    sb.append("Content-Length: 4\r\nConnection: close\r\n\r\nbody");
    byte[] response = sb.toString().getBytes(StandardCharsets.US_ASCII);
    try (RawServer server = new RawServer(response)) {
      File dest = new File(tempDir, "header-flood");
      URI uri = new URI("http://ssrf-rebind.invalid:" + server.port() + "/keytab");
      InetAddress pinned = InetAddress.getByName("127.0.0.1");

      IOException exception =
          Assertions.assertThrows(
              IOException.class, () -> RemoteFileDownloader.download(uri, pinned, dest, 30000));
      Assertions.assertTrue(
          exception.getMessage().contains("headers exceed the allowed size"),
          exception.getMessage());
      Assertions.assertFalse(dest.exists());
    }
  }

  @Test
  public void testHttpDownloadRejectsUnboundedChunkedTrailers() throws Exception {
    // After the terminating 0-chunk a malicious server floods trailer headers; their total size
    // must be bounded rather than read in an unbounded loop.
    StringBuilder sb =
        new StringBuilder(
            "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n4\r\nWiki\r\n0\r\n");
    // ~70 KiB of trailer lines, each well under the per-line cap, exceeding the 64 KiB total cap.
    String trailerValue = new String(new char[1000]).replace('\0', 'A');
    for (int i = 0; i < 70; i++) {
      sb.append("X-Trailer: ").append(trailerValue).append("\r\n");
    }
    byte[] response = sb.toString().getBytes(StandardCharsets.US_ASCII);
    try (RawServer server = new RawServer(response)) {
      File dest = new File(tempDir, "trailer-flood");
      URI uri = new URI("http://ssrf-rebind.invalid:" + server.port() + "/keytab");
      InetAddress pinned = InetAddress.getByName("127.0.0.1");

      Assertions.assertThrows(
          IOException.class, () -> RemoteFileDownloader.download(uri, pinned, dest, 30000));
      Assertions.assertFalse(dest.exists());
    }
  }

  @Test
  public void testHttpDownloadAcceptsMultiSpaceStatusLine() throws Exception {
    // RFC 7230 allows one SP, but some servers/proxies emit extra whitespace; a valid 200 must
    // still be accepted.
    byte[] response =
        ("HTTP/1.1  200  OK\r\nContent-Length: 4\r\nConnection: close\r\n\r\nabcd")
            .getBytes(StandardCharsets.US_ASCII);
    try (RawServer server = new RawServer(response)) {
      File dest = new File(tempDir, "multispace");
      URI uri = new URI("http://ssrf-rebind.invalid:" + server.port() + "/keytab");
      InetAddress pinned = InetAddress.getByName("127.0.0.1");

      RemoteFileDownloader.download(uri, pinned, dest, 30000);

      Assertions.assertEquals("abcd", Files.readString(dest.toPath()));
    }
  }

  @Test
  public void testHttpDownloadRejectsTruncatedContentLength() throws Exception {
    // Content-Length promises 100 bytes but only 10 are delivered before the connection closes;
    // the partial download must be rejected, not silently accepted.
    byte[] response =
        ("HTTP/1.0 200 OK\r\nContent-Length: 100\r\nConnection: close\r\n\r\n0123456789")
            .getBytes(StandardCharsets.US_ASCII);
    try (RawServer server = new RawServer(response)) {
      File dest = new File(tempDir, "truncated");
      URI uri = new URI("http://ssrf-rebind.invalid:" + server.port() + "/keytab");
      InetAddress pinned = InetAddress.getByName("127.0.0.1");

      Assertions.assertThrows(
          IOException.class, () -> RemoteFileDownloader.download(uri, pinned, dest, 30000));
      Assertions.assertFalse(dest.exists(), "truncated download must not leave a destination file");
      // The staging temp file must be cleaned up on a failed download, not orphaned.
      File[] leftovers = tempDir.listFiles((d, name) -> name.endsWith(".tmp"));
      Assertions.assertNotNull(leftovers);
      Assertions.assertEquals(0, leftovers.length, "no temp file should remain after a failure");
    }
  }

  @Test
  public void testHttpDownloadRejectsOversizedStatusLine() throws Exception {
    // The status line is bounded only by readLine's per-line cap (the header-total cap does not
    // apply here), so a >64 KiB status line must be rejected by that specific guard.
    String huge = new String(new char[70_000]).replace('\0', 'A');
    byte[] response = ("HTTP/1.1 200 " + huge).getBytes(StandardCharsets.US_ASCII);
    try (RawServer server = new RawServer(response)) {
      File dest = new File(tempDir, "huge-status");
      URI uri = new URI("http://ssrf-rebind.invalid:" + server.port() + "/keytab");
      InetAddress pinned = InetAddress.getByName("127.0.0.1");

      IOException exception =
          Assertions.assertThrows(
              IOException.class, () -> RemoteFileDownloader.download(uri, pinned, dest, 30000));
      Assertions.assertTrue(
          exception.getMessage().contains("HTTP line exceeds the allowed size"),
          exception.getMessage());
      Assertions.assertFalse(dest.exists());
    }
  }

  @Test
  public void testHttpDownloadRejectsObsoleteLineFolding() throws Exception {
    // A header continuation line (obsolete line folding, leading SP/TAB) is a smuggling vector and
    // must be rejected. The continuation carries a colon so the no-colon guard cannot mask the
    // folding guard, and the message is asserted so only the folding guard can satisfy the test.
    byte[] response =
        ("HTTP/1.0 200 OK\r\nX-Folded: a\r\n\tfoo: bar\r\nConnection: close\r\n\r\nbody")
            .getBytes(StandardCharsets.US_ASCII);
    try (RawServer server = new RawServer(response)) {
      File dest = new File(tempDir, "folded");
      URI uri = new URI("http://ssrf-rebind.invalid:" + server.port() + "/keytab");
      InetAddress pinned = InetAddress.getByName("127.0.0.1");

      IOException exception =
          Assertions.assertThrows(
              IOException.class, () -> RemoteFileDownloader.download(uri, pinned, dest, 30000));
      Assertions.assertTrue(
          exception.getMessage().contains("Obsolete line folding"), exception.getMessage());
      Assertions.assertFalse(dest.exists());
    }
  }

  @Test
  public void testHttpDownloadRejectsHeaderWithoutColon() throws Exception {
    byte[] response =
        ("HTTP/1.0 200 OK\r\nBadHeaderNoColon\r\nConnection: close\r\n\r\nbody")
            .getBytes(StandardCharsets.US_ASCII);
    assertDownloadRejected(response, "no-colon");
  }

  @Test
  public void testHttpDownloadRejectsNegativeChunkSize() throws Exception {
    byte[] response =
        ("HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n-1\r\n")
            .getBytes(StandardCharsets.US_ASCII);
    try (RawServer server = new RawServer(response)) {
      File dest = new File(tempDir, "neg-chunk");
      URI uri = new URI("http://ssrf-rebind.invalid:" + server.port() + "/keytab");
      InetAddress pinned = InetAddress.getByName("127.0.0.1");

      IOException exception =
          Assertions.assertThrows(
              IOException.class, () -> RemoteFileDownloader.download(uri, pinned, dest, 30000));
      // Assert on the specific guard so it is independently pinned (a removed guard would otherwise
      // still throw later for a different reason).
      Assertions.assertTrue(
          exception.getMessage().contains("Negative chunk size"), exception.getMessage());
      Assertions.assertFalse(dest.exists());
    }
  }

  @Test
  public void testHttpDownloadRejectsNonNumericChunkSize() throws Exception {
    // A non-hexadecimal chunk-size line must be rejected as an IOException, not propagate a raw
    // unchecked NumberFormatException out of the declared `throws IOException` contract.
    byte[] response =
        ("HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\nGG\r\nWiki\r\n0\r\n\r\n")
            .getBytes(StandardCharsets.US_ASCII);
    assertDownloadRejected(response, "bad-hex-chunk");
  }

  @Test
  public void testHttpDownloadRejectsMalformedChunkTerminator() throws Exception {
    // After the 4-byte chunk data "Wiki" the terminator must be CRLF; "XX" must be rejected.
    byte[] response =
        ("HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n4\r\nWikiXX\r\n0\r\n\r\n")
            .getBytes(StandardCharsets.US_ASCII);
    assertDownloadRejected(response, "bad-terminator");
  }

  @Test
  public void testHttpDownloadRejectsMultiCodingTransferEncoding() throws Exception {
    // "gzip, chunked" is de-chunkable but still gzip-compressed; since we do not gunzip,
    // de-chunking
    // alone would write corrupt bytes. Anything other than a sole "chunked" coding must be
    // rejected.
    byte[] response =
        ("HTTP/1.1 200 OK\r\n"
                + "Transfer-Encoding: gzip, chunked\r\n"
                + "Connection: close\r\n"
                + "\r\n"
                + "4\r\nWiki\r\n0\r\n\r\n")
            .getBytes(StandardCharsets.US_ASCII);
    try (RawServer server = new RawServer(response)) {
      File dest = new File(tempDir, "multi-coding");
      URI uri = new URI("http://ssrf-rebind.invalid:" + server.port() + "/keytab");
      InetAddress pinned = InetAddress.getByName("127.0.0.1");

      IOException exception =
          Assertions.assertThrows(
              IOException.class, () -> RemoteFileDownloader.download(uri, pinned, dest, 30000));
      Assertions.assertTrue(
          exception.getMessage().contains("Unsupported Transfer-Encoding"), exception.getMessage());
      Assertions.assertFalse(dest.exists());
    }
  }

  @Test
  public void testHttpDownloadRejectsChunkedSubstringTransferEncoding() throws Exception {
    // "xchunked" contains the substring "chunked" but is not the chunked coding; it must not be
    // mistaken for it. The message is asserted so the unsupported-coding guard, not a later chunk
    // parse error, is what satisfies the test.
    byte[] response =
        ("HTTP/1.1 200 OK\r\n"
                + "Transfer-Encoding: xchunked\r\n"
                + "Connection: close\r\n"
                + "\r\n"
                + "4\r\nWiki\r\n0\r\n\r\n")
            .getBytes(StandardCharsets.US_ASCII);
    try (RawServer server = new RawServer(response)) {
      File dest = new File(tempDir, "x-chunked");
      URI uri = new URI("http://ssrf-rebind.invalid:" + server.port() + "/keytab");
      InetAddress pinned = InetAddress.getByName("127.0.0.1");

      IOException exception =
          Assertions.assertThrows(
              IOException.class, () -> RemoteFileDownloader.download(uri, pinned, dest, 30000));
      Assertions.assertTrue(
          exception.getMessage().contains("Unsupported Transfer-Encoding"), exception.getMessage());
      Assertions.assertFalse(dest.exists());
    }
  }

  @Test
  public void testHttpDownloadRejectsConflictingDuplicateContentLength() throws Exception {
    // Two conflicting Content-Length values (RFC 7230 §3.3.2) are a response-splitting/smuggling
    // vector; a silent last-wins would let parsers disagree, so the conflict is rejected outright.
    byte[] response =
        ("HTTP/1.0 200 OK\r\n"
                + "Content-Length: 4\r\n"
                + "Content-Length: 5\r\n"
                + "Connection: close\r\n"
                + "\r\n"
                + "abcd")
            .getBytes(StandardCharsets.US_ASCII);
    try (RawServer server = new RawServer(response)) {
      File dest = new File(tempDir, "dup-cl");
      URI uri = new URI("http://ssrf-rebind.invalid:" + server.port() + "/keytab");
      InetAddress pinned = InetAddress.getByName("127.0.0.1");

      IOException exception =
          Assertions.assertThrows(
              IOException.class, () -> RemoteFileDownloader.download(uri, pinned, dest, 30000));
      Assertions.assertTrue(
          exception.getMessage().contains("Conflicting duplicate"), exception.getMessage());
      Assertions.assertFalse(dest.exists());
    }
  }

  @Test
  public void testHttpDownloadRejectsConflictingDuplicateTransferEncoding() throws Exception {
    // Conflicting duplicate Transfer-Encoding headers (a last-wins "chunked" would otherwise pass
    // the chunked check) must be rejected as a smuggling vector before any body decode.
    byte[] response =
        ("HTTP/1.1 200 OK\r\n"
                + "Transfer-Encoding: gzip\r\n"
                + "Transfer-Encoding: chunked\r\n"
                + "Connection: close\r\n"
                + "\r\n"
                + "4\r\nWiki\r\n0\r\n\r\n")
            .getBytes(StandardCharsets.US_ASCII);
    try (RawServer server = new RawServer(response)) {
      File dest = new File(tempDir, "dup-te");
      URI uri = new URI("http://ssrf-rebind.invalid:" + server.port() + "/keytab");
      InetAddress pinned = InetAddress.getByName("127.0.0.1");

      IOException exception =
          Assertions.assertThrows(
              IOException.class, () -> RemoteFileDownloader.download(uri, pinned, dest, 30000));
      Assertions.assertTrue(
          exception.getMessage().contains("Conflicting duplicate"), exception.getMessage());
      Assertions.assertFalse(dest.exists());
    }
  }

  @Test
  public void testHttpDownloadSendsHttp11Request() throws Exception {
    AtomicReference<String> protocol = new AtomicReference<>();
    HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext(
        "/keytab",
        exchange -> {
          protocol.set(exchange.getProtocol());
          byte[] bytes = "data".getBytes(StandardCharsets.UTF_8);
          exchange.sendResponseHeaders(200, bytes.length);
          try (OutputStream os = exchange.getResponseBody()) {
            os.write(bytes);
          }
        });
    try {
      server.start();
      int port = server.getAddress().getPort();
      File dest = new File(tempDir, "http11");
      URI uri = new URI("http://ssrf-rebind.invalid:" + port + "/keytab");
      InetAddress pinned = InetAddress.getByName("127.0.0.1");

      RemoteFileDownloader.download(uri, pinned, dest, 30000);

      // The request must be HTTP/1.1 so origins frame the body (Content-Length or chunked) rather
      // than relying on connection close, which the downloader rejects.
      Assertions.assertEquals("HTTP/1.1", protocol.get());
    } finally {
      server.stop(0);
    }
  }

  private void assertDownloadRejected(byte[] response, String name) throws Exception {
    try (RawServer server = new RawServer(response)) {
      File dest = new File(tempDir, name);
      URI uri = new URI("http://ssrf-rebind.invalid:" + server.port() + "/keytab");
      InetAddress pinned = InetAddress.getByName("127.0.0.1");

      Assertions.assertThrows(
          IOException.class, () -> RemoteFileDownloader.download(uri, pinned, dest, 30000));
      Assertions.assertFalse(dest.exists());
    }
  }

  @Test
  public void testHttpDownloadRejectsUnframedResponse() throws Exception {
    // No Content-Length and no chunked encoding: a close-delimited body cannot be verified for
    // completeness (a premature close looks identical to a complete body), so it is rejected rather
    // than risk installing a silently-truncated keytab/jar.
    byte[] response =
        ("HTTP/1.0 200 OK\r\nConnection: close\r\n\r\nclose-delimited-body")
            .getBytes(StandardCharsets.US_ASCII);
    try (RawServer server = new RawServer(response)) {
      File dest = new File(tempDir, "unframed");
      URI uri = new URI("http://ssrf-rebind.invalid:" + server.port() + "/keytab");
      InetAddress pinned = InetAddress.getByName("127.0.0.1");

      Assertions.assertThrows(
          IOException.class, () -> RemoteFileDownloader.download(uri, pinned, dest, 30000));
      Assertions.assertFalse(dest.exists());
    }
  }

  @Test
  public void testHttpDownloadRejectsOversizedContentLength() throws Exception {
    // A Content-Length beyond the 2 GiB body cap must be rejected without streaming anything.
    byte[] response =
        ("HTTP/1.0 200 OK\r\nContent-Length: 3000000000\r\nConnection: close\r\n\r\n")
            .getBytes(StandardCharsets.US_ASCII);
    try (RawServer server = new RawServer(response)) {
      File dest = new File(tempDir, "too-big");
      URI uri = new URI("http://ssrf-rebind.invalid:" + server.port() + "/keytab");
      InetAddress pinned = InetAddress.getByName("127.0.0.1");

      IOException exception =
          Assertions.assertThrows(
              IOException.class, () -> RemoteFileDownloader.download(uri, pinned, dest, 30000));
      // Assert the specific reason so the Content-Length cap is pinned and a competing truncation
      // failure (read == -1 on the empty body) cannot masquerade as it.
      Assertions.assertTrue(
          exception.getMessage().contains("exceeds the maximum allowed size"),
          exception.getMessage());
      Assertions.assertFalse(dest.exists());
    }
  }

  @Test
  public void testHttpDownloadRejectsOversizedHeader() throws Exception {
    // A single header line larger than the 64 KiB cap must be rejected, not buffered unbounded.
    StringBuilder hugeHeader = new StringBuilder("X-Big: ");
    for (int i = 0; i < 70_000; i++) {
      hugeHeader.append('A');
    }
    byte[] response =
        ("HTTP/1.0 200 OK\r\n" + hugeHeader + "\r\nConnection: close\r\n\r\nbody")
            .getBytes(StandardCharsets.US_ASCII);
    try (RawServer server = new RawServer(response)) {
      File dest = new File(tempDir, "huge-header");
      URI uri = new URI("http://ssrf-rebind.invalid:" + server.port() + "/keytab");
      InetAddress pinned = InetAddress.getByName("127.0.0.1");

      Assertions.assertThrows(
          IOException.class, () -> RemoteFileDownloader.download(uri, pinned, dest, 30000));
      Assertions.assertFalse(dest.exists());
    }
  }

  @Test
  public void testHttpDownloadRejectsLeadingPlusContentLength() throws Exception {
    // Long.parseLong would accept "+5"; RFC 7230 forbids it. The response must be rejected.
    byte[] response =
        ("HTTP/1.0 200 OK\r\nContent-Length: +5\r\nConnection: close\r\n\r\nhello")
            .getBytes(StandardCharsets.US_ASCII);
    try (RawServer server = new RawServer(response)) {
      File dest = new File(tempDir, "plus-cl");
      URI uri = new URI("http://ssrf-rebind.invalid:" + server.port() + "/keytab");
      InetAddress pinned = InetAddress.getByName("127.0.0.1");

      Assertions.assertThrows(
          IOException.class, () -> RemoteFileDownloader.download(uri, pinned, dest, 30000));
      Assertions.assertFalse(dest.exists());
    }
  }

  @Test
  public void testUnsupportedSchemeRejected() throws Exception {
    File dest = new File(tempDir, "scp");
    URI uri = new URI("scp://host:22/keytab");
    InetAddress pinned = InetAddress.getByName("127.0.0.1");

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> RemoteFileDownloader.download(uri, pinned, dest, 30000));
  }

  @Test
  public void testHelpersFormatHostHeaderAndTargets() throws Exception {
    Assertions.assertEquals("h:8080", RemoteFileDownloader.hostHeader("h", 8080));
    Assertions.assertEquals("h", RemoteFileDownloader.hostHeader("h", -1));
    Assertions.assertEquals("/", RemoteFileDownloader.requestTarget(new URI("http://h")));
    Assertions.assertEquals(
        "/p?q=1", RemoteFileDownloader.requestTarget(new URI("http://h/p?q=1")));
  }

  private HttpServer createLoopbackServer(String response, AtomicReference<String> hostHeader)
      throws Exception {
    HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext(
        "/keytab",
        exchange -> {
          hostHeader.set(exchange.getRequestHeaders().getFirst("Host"));
          byte[] bytes = response.getBytes(StandardCharsets.UTF_8);
          exchange.sendResponseHeaders(200, bytes.length);
          try (OutputStream os = exchange.getResponseBody()) {
            os.write(bytes);
          }
        });
    return server;
  }

  /** A single-shot loopback server that writes a fixed raw response, for byte-level control. */
  private static final class RawServer implements AutoCloseable {
    private final ServerSocket serverSocket;
    private final Thread thread;

    RawServer(byte[] response) throws IOException {
      this.serverSocket = new ServerSocket(0, 1, InetAddress.getByName("127.0.0.1"));
      this.thread =
          new Thread(
              () -> {
                try (Socket socket = serverSocket.accept()) {
                  drainRequest(socket.getInputStream());
                  socket.getOutputStream().write(response);
                  socket.getOutputStream().flush();
                } catch (IOException ignored) {
                  // The client may close early; nothing to do.
                }
              });
      this.thread.setDaemon(true);
      this.thread.start();
    }

    int port() {
      return serverSocket.getLocalPort();
    }

    private static void drainRequest(InputStream in) throws IOException {
      int state = 0; // counts how far we are through the terminating \r\n\r\n sequence
      int b;
      while (state < 4 && (b = in.read()) != -1) {
        boolean expectCr = state == 0 || state == 2;
        if (expectCr) {
          state = (b == '\r') ? state + 1 : 0;
        } else {
          state = (b == '\n') ? state + 1 : 0;
        }
      }
    }

    @Override
    public void close() throws IOException {
      serverSocket.close();
    }
  }
}
