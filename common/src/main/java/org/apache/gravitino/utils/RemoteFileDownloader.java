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

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;

/**
 * Downloads an {@code http} or {@code https} file while pinning the TCP connection to a
 * pre-validated address.
 *
 * <p>This closes the DNS-rebinding TOCTOU window between SSRF validation and the actual fetch: the
 * host is resolved and validated once by {@link RemoteUriValidator#resolveAndValidate}, and the
 * resulting {@link InetAddress} is connected to directly here, so the hostname is never
 * re-resolved. A minimal HTTP/1.1 client is used so the connection can be pinned at the socket
 * level (the JDK exposes no per-connection address override for plain {@code HttpURLConnection},
 * and silently drops a {@code Host} header set via {@code setRequestProperty}). The original
 * hostname is still used for the HTTP {@code Host} header and, for {@code https}, for TLS SNI and
 * certificate-hostname verification, so virtual hosting and certificate validation keep working.
 * Redirects are not followed, because a redirect target would re-resolve to an unvalidated address.
 *
 * <p>{@code ftp} is intentionally not handled here: the FTP data channel is opened to an address
 * the server chooses in its {@code PASV}/{@code EPSV} reply, which cannot be pinned, so {@link
 * FileFetcher} rejects {@code ftp} on the SSRF-blocking path instead.
 *
 * <p>Only responses framed by {@code Content-Length} or chunked transfer-encoding are accepted; a
 * connection-close-delimited body is rejected because a premature close cannot be told apart from a
 * complete one. Combined with streaming to a sibling temporary file that is atomically moved into
 * place only on success, this means a connection that drops mid-transfer never leaves a truncated
 * destination behind. Response and header sizes are bounded to prevent a malicious pinned host from
 * exhausting disk or memory.
 */
final class RemoteFileDownloader {

  private static final int DEFAULT_HTTP_PORT = 80;
  private static final int DEFAULT_HTTPS_PORT = 443;
  private static final int MAX_HEADER_BYTES = 64 * 1024;
  private static final long MAX_BODY_BYTES = 2L * 1024 * 1024 * 1024;
  private static final int COPY_BUFFER_BYTES = 8192;

  private RemoteFileDownloader() {}

  /**
   * Downloads {@code uri} into {@code destFile}, connecting only to {@code pinnedAddress}.
   *
   * @param uri the source URI; must use the {@code http} or {@code https} scheme
   * @param pinnedAddress the validated address to connect to
   * @param destFile the local destination file
   * @param timeoutMs the connect/read timeout in milliseconds
   * @throws IOException if the file cannot be fetched or the response is not successful
   */
  static void download(URI uri, InetAddress pinnedAddress, File destFile, int timeoutMs)
      throws IOException {
    String scheme = Optional.ofNullable(uri.getScheme()).orElse("").toLowerCase(Locale.ROOT);
    if (!scheme.equals("http") && !scheme.equals("https")) {
      throw new IllegalArgumentException(
          String.format("Pinned download does not support scheme '%s'", scheme));
    }

    Path destPath = destFile.toPath().toAbsolutePath();
    Path tempPath =
        Files.createTempFile(destPath.getParent(), destPath.getFileName() + ".", ".tmp");
    try {
      httpDownload(uri, pinnedAddress, tempPath, timeoutMs, scheme.equals("https"));
      Files.move(tempPath, destPath, StandardCopyOption.REPLACE_EXISTING);
    } catch (Throwable e) {
      // Catch every throwable (incl. Error/OutOfMemoryError) so the temp file is never orphaned in
      // the staging directory. Precise rethrow keeps the declared `throws IOException`.
      try {
        Files.deleteIfExists(tempPath);
      } catch (IOException suppressed) {
        e.addSuppressed(suppressed);
      }
      throw e;
    }
  }

  private static void httpDownload(
      URI uri, InetAddress pinnedAddress, Path destPath, int timeoutMs, boolean tls)
      throws IOException {
    String host = uri.getHost();
    int port = uri.getPort() != -1 ? uri.getPort() : (tls ? DEFAULT_HTTPS_PORT : DEFAULT_HTTP_PORT);

    Socket socket = new Socket();
    try {
      socket.connect(new InetSocketAddress(pinnedAddress, port), timeoutMs);
      socket.setSoTimeout(timeoutMs);
      if (tls) {
        socket = startTls(socket, host, port);
      }

      sendGetRequest(socket.getOutputStream(), requestTarget(uri), hostHeader(host, uri.getPort()));

      InputStream in = new BufferedInputStream(socket.getInputStream());
      int status = readStatusCode(in);
      if (status < 200 || status >= 300) {
        throw new IOException(
            String.format(
                "Unexpected HTTP status %d fetching pinned URI '%s'", status, SafeUri.redact(uri)));
      }
      Map<String, String> headers = readHeaders(in);
      try (OutputStream out = Files.newOutputStream(destPath)) {
        writeBody(in, headers, out);
      }
    } finally {
      socket.close();
    }
  }

  /**
   * Layers TLS over an already-connected plain socket. The handshake uses {@code host} (the
   * original hostname) for SNI and certificate verification ({@code HTTPS} endpoint
   * identification), while the TCP connection stays pinned to the address the plain socket was
   * connected to.
   */
  private static Socket startTls(Socket plainSocket, String host, int port) throws IOException {
    SSLSocketFactory factory = (SSLSocketFactory) SSLSocketFactory.getDefault();
    SSLSocket sslSocket = (SSLSocket) factory.createSocket(plainSocket, host, port, true);
    SSLParameters parameters = sslSocket.getSSLParameters();
    parameters.setEndpointIdentificationAlgorithm("HTTPS");
    sslSocket.setSSLParameters(parameters);
    sslSocket.startHandshake();
    return sslSocket;
  }

  private static void sendGetRequest(OutputStream out, String target, String hostHeader)
      throws IOException {
    // HTTP/1.1 with "Connection: close" requests a single non-persistent exchange while biasing the
    // origin toward framing the body with Content-Length or chunked encoding. An unframed
    // (close-delimited) body is still rejected by writeBody because its completeness cannot be
    // verified.
    String request =
        "GET "
            + target
            + " HTTP/1.1\r\n"
            + "Host: "
            + hostHeader
            + "\r\n"
            + "Connection: close\r\n"
            + "\r\n";
    out.write(request.getBytes(StandardCharsets.US_ASCII));
    out.flush();
  }

  private static int readStatusCode(InputStream in) throws IOException {
    String statusLine = readLine(in);
    if (statusLine == null) {
      throw new IOException("Empty HTTP response from pinned host");
    }
    // Format: "HTTP/1.x <code> <reason>"; tolerate extra whitespace between tokens.
    String[] parts = statusLine.trim().split("\\s+", 3);
    if (parts.length < 2) {
      throw new IOException("Malformed HTTP status line: " + statusLine);
    }
    try {
      return Integer.parseInt(parts[1]);
    } catch (NumberFormatException e) {
      throw new IOException("Malformed HTTP status line: " + statusLine, e);
    }
  }

  private static Map<String, String> readHeaders(InputStream in) throws IOException {
    Map<String, String> headers = new HashMap<>();
    int total = 0;
    String line;
    while ((line = readLine(in)) != null) {
      if (line.isEmpty()) {
        return headers;
      }
      // Reject obsolete line folding (RFC 7230 §3.2.4); a leading space/tab continuation is treated
      // differently by different parsers and is a smuggling vector.
      if (line.charAt(0) == ' ' || line.charAt(0) == '\t') {
        throw new IOException("Obsolete line folding in HTTP headers is rejected from pinned host");
      }
      total += line.length();
      if (total > MAX_HEADER_BYTES) {
        throw new IOException("HTTP response headers exceed the allowed size from pinned host");
      }
      int colon = line.indexOf(':');
      if (colon <= 0) {
        throw new IOException("Malformed HTTP header line from pinned host: " + line);
      }
      String name = line.substring(0, colon).trim().toLowerCase(Locale.ROOT);
      String value = line.substring(colon + 1).trim();
      String previous = headers.put(name, value);
      // A duplicate Content-Length or Transfer-Encoding with a different value (RFC 7230 §3.3.2 /
      // §3.3.3) is a request-smuggling vector: parsers that disagree on which copy wins frame the
      // body differently. Reject the conflict rather than silently keeping the last one.
      if (previous != null
          && !previous.equals(value)
          && (name.equals("content-length") || name.equals("transfer-encoding"))) {
        throw new IOException("Conflicting duplicate " + name + " headers from pinned host");
      }
    }
    throw new IOException("Unexpected end of stream while reading HTTP headers from pinned host");
  }

  private static void writeBody(InputStream in, Map<String, String> headers, OutputStream out)
      throws IOException {
    String transferEncoding = headers.get("transfer-encoding");
    if (transferEncoding != null) {
      // Accept only a sole "chunked" coding. A value such as "gzip, chunked" is de-chunkable but
      // still compressed, and we do not gunzip, so de-chunking alone would write corrupt bytes; a
      // substring like "xchunked" must likewise not be mistaken for chunked. Reject anything other
      // than an exact "chunked" rather than emit a mis-decoded body.
      if (!transferEncoding.trim().equalsIgnoreCase("chunked")) {
        throw new IOException(
            "Unsupported Transfer-Encoding '"
                + transferEncoding
                + "' from pinned host; only 'chunked' is supported");
      }
      copyChunked(in, out);
      return;
    }
    String contentLength = headers.get("content-length");
    if (contentLength != null) {
      copyExact(in, out, parseContentLength(contentLength));
      return;
    }
    // Reject a connection-close-delimited body (no Content-Length, no chunked framing): a premature
    // close cannot be distinguished from a complete body, so the download's completeness would be
    // unverifiable and a truncated keytab/jar could be installed silently.
    throw new IOException(
        "Response from pinned host has neither Content-Length nor chunked framing; "
            + "its completeness cannot be verified");
  }

  private static long parseContentLength(String value) throws IOException {
    String trimmed = value.trim();
    // RFC 7230 §3.3.2 defines Content-Length as 1*DIGIT; reject anything else (e.g. "+5", "0x10")
    // that Long.parseLong would otherwise accept, to avoid framing disagreement with upstreams.
    if (trimmed.isEmpty() || !isAsciiDigits(trimmed)) {
      throw new IOException("Malformed Content-Length header from pinned host: " + value);
    }
    long length;
    try {
      length = Long.parseLong(trimmed);
    } catch (NumberFormatException e) {
      throw new IOException("Malformed Content-Length header from pinned host: " + value, e);
    }
    if (length > MAX_BODY_BYTES) {
      throw new IOException("Content-Length " + length + " exceeds the maximum allowed size");
    }
    return length;
  }

  private static boolean isAsciiDigits(String value) {
    for (int i = 0; i < value.length(); i++) {
      char c = value.charAt(i);
      if (c < '0' || c > '9') {
        return false;
      }
    }
    return true;
  }

  /** Copies exactly {@code length} bytes, failing if the stream ends early. */
  private static void copyExact(InputStream in, OutputStream out, long length) throws IOException {
    byte[] buffer = new byte[COPY_BUFFER_BYTES];
    long remaining = length;
    while (remaining > 0) {
      int read = in.read(buffer, 0, (int) Math.min(buffer.length, remaining));
      if (read == -1) {
        throw new IOException(
            String.format(
                "Truncated response: %d of %d body bytes missing from pinned host",
                remaining, length));
      }
      out.write(buffer, 0, read);
      remaining -= read;
    }
  }

  private static void copyChunked(InputStream in, OutputStream out) throws IOException {
    long total = 0;
    while (true) {
      String sizeLine = readLine(in);
      if (sizeLine == null) {
        throw new IOException("Unexpected end of stream reading chunk size from pinned host");
      }
      int extension = sizeLine.indexOf(';');
      String hex = (extension >= 0 ? sizeLine.substring(0, extension) : sizeLine).trim();
      int chunkSize;
      try {
        chunkSize = Integer.parseInt(hex, 16);
      } catch (NumberFormatException e) {
        throw new IOException("Malformed chunk size from pinned host: " + sizeLine, e);
      }
      if (chunkSize < 0) {
        throw new IOException("Negative chunk size from pinned host: " + sizeLine);
      }
      if (chunkSize == 0) {
        break;
      }
      total += chunkSize;
      if (total > MAX_BODY_BYTES) {
        throw new IOException("Chunked response exceeds the maximum allowed size from pinned host");
      }
      copyExact(in, out, chunkSize);
      // Each chunk's data is followed by a CRLF, which readLine returns as an empty line.
      String terminator = readLine(in);
      if (terminator == null || !terminator.isEmpty()) {
        throw new IOException("Malformed chunk terminator from pinned host");
      }
    }
    // Discard any trailer headers up to the final blank line, bounding their total size so a server
    // cannot flood unbounded trailers.
    int trailerBytes = 0;
    String line;
    while ((line = readLine(in)) != null && !line.isEmpty()) {
      trailerBytes += line.length();
      if (trailerBytes > MAX_HEADER_BYTES) {
        throw new IOException("Chunked trailer headers exceed the allowed size from pinned host");
      }
    }
  }

  /**
   * Reads a CRLF- or LF-terminated line, returning it without the line terminator, or {@code null}
   * at end of stream. The line is capped at {@link #MAX_HEADER_BYTES} to bound memory.
   */
  private static String readLine(InputStream in) throws IOException {
    ByteArrayOutputStream buffer = new ByteArrayOutputStream();
    boolean read = false;
    int b;
    while ((b = in.read()) != -1) {
      read = true;
      if (b == '\n') {
        break;
      }
      buffer.write(b);
      if (buffer.size() > MAX_HEADER_BYTES) {
        throw new IOException("HTTP line exceeds the allowed size from pinned host");
      }
    }
    if (!read) {
      return null;
    }
    // LF terminates the line; strip a single trailing CR (CRLF), but preserve any other bytes so
    // the parser does not silently mangle content that legitimately contains a CR.
    byte[] bytes = buffer.toByteArray();
    int length = bytes.length;
    if (length > 0 && bytes[length - 1] == '\r') {
      length--;
    }
    return new String(bytes, 0, length, StandardCharsets.US_ASCII);
  }

  static String requestTarget(URI uri) {
    String path = uri.getRawPath();
    if (path == null || path.isEmpty()) {
      path = "/";
    }
    String query = uri.getRawQuery();
    return query == null ? path : path + "?" + query;
  }

  static String hostHeader(String host, int port) {
    return port == -1 ? host : host + ":" + port;
  }
}
