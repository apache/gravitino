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
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Locale;
import java.util.Optional;
import javax.annotation.Nullable;
import org.apache.commons.io.FileUtils;

/**
 * Singleton that fetches a file referenced by a URI to a local destination. Supports {@code file},
 * {@code http}, {@code https}, {@code ftp} and {@code hdfs} schemes. This is the single shared
 * implementation used by the job manager and the Kerberos clients of the Hive, Iceberg, Hadoop and
 * Paimon catalogs.
 *
 * <p>The {@code hdfs} scheme is resolved reflectively against {@code
 * org.apache.hadoop.fs.FileSystem} so that this class can live in the {@code common} module, which
 * does not declare a compile-time dependency on Hadoop. Callers that never use {@code hdfs} URIs
 * (for example the job manager and the Paimon catalog) simply pass {@code null} for the Hadoop
 * configuration.
 */
public final class FileFetcher {

  /** The server configuration that controls unsafe remote URI blocking. */
  public static final String BLOCK_UNSAFE_REMOTE_URI_CONFIG =
      "gravitino.fetchFile.blockUnsafeRemoteUri";

  private volatile boolean blockUnsafeRemoteUri = true;

  private FileFetcher() {}

  private static class InstanceHolder {
    private static final FileFetcher INSTANCE = new FileFetcher();
  }

  /**
   * Returns the singleton file fetcher.
   *
   * @return the singleton file fetcher
   */
  public static FileFetcher get() {
    return InstanceHolder.INSTANCE;
  }

  /**
   * Initializes the file fetcher.
   *
   * @param blockUnsafeRemoteUri whether to block remote URIs that resolve to unsafe addresses
   */
  public void initialize(boolean blockUnsafeRemoteUri) {
    this.blockUnsafeRemoteUri = blockUnsafeRemoteUri;
  }

  /**
   * Fetches the file referenced by {@code fileUri} into {@code destFile}.
   *
   * @param fileUri the source URI; a missing scheme is treated as {@code file}
   * @param destFile the local destination file
   * @param timeoutMs the connect/read timeout in milliseconds, applied to remote (http/https/ftp)
   *     downloads
   * @param hadoopConf an {@code org.apache.hadoop.conf.Configuration} instance, required only for
   *     the {@code hdfs} scheme; may be {@code null} when no {@code hdfs} URI is fetched
   * @return the absolute path of {@code destFile}
   * @throws IOException if the file cannot be fetched
   */
  public String fetchFileFromUri(
      String fileUri, File destFile, int timeoutMs, @Nullable Object hadoopConf)
      throws IOException {
    try {
      URI uri = new URI(fileUri);
      // URI schemes are case-insensitive (RFC 3986); normalize so e.g. "HTTP" routes like "http".
      String scheme = Optional.ofNullable(uri.getScheme()).orElse("file").toLowerCase(Locale.ROOT);

      switch (scheme) {
        case "http":
        case "https":
        case "ftp":
          if (!blockUnsafeRemoteUri) {
            FileUtils.copyURLToFile(uri.toURL(), destFile, timeoutMs, timeoutMs);
          } else if (scheme.equals("ftp")) {
            // FTP opens its data channel to an address the server chooses in its PASV/EPSV reply,
            // which cannot be pinned to the validated host and is therefore vulnerable to SSRF. The
            // operator must opt out of blocking to use FTP from a trusted source.
            throw new IllegalArgumentException(
                String.format(
                    "Refusing to fetch ftp uri '%s' from the Gravitino server side: FTP's data "
                        + "channel cannot be restricted to the validated address. Set %s to false "
                        + "to allow it if the source is trusted.",
                    SafeUri.redact(uri), BLOCK_UNSAFE_REMOTE_URI_CONFIG));
          } else {
            // Resolve and validate the host exactly once, then pin the download to the validated
            // address so the hostname cannot be re-resolved to an unsafe address (DNS rebinding).
            InetAddress pinnedAddress =
                RemoteUriValidator.resolveAndValidate(
                    uri, String.format("'%s' to false", BLOCK_UNSAFE_REMOTE_URI_CONFIG));
            RemoteFileDownloader.download(uri, pinnedAddress, destFile, timeoutMs);
          }
          break;

        case "file":
          linkLocalFile(uri, destFile);
          break;

        case "hdfs":
          copyHdfsFileToLocal(uri, destFile, Optional.ofNullable(hadoopConf));
          break;

        default:
          throw new IllegalArgumentException(
              String.format("The scheme '%s' is not supported", scheme));
      }

      return destFile.getAbsolutePath();
    } catch (URISyntaxException ue) {
      throw new IllegalArgumentException("The uri of file has the wrong format", ue);
    }
  }

  private synchronized void linkLocalFile(URI uri, File destFile) throws IOException {
    String sourcePath = uri.getPath();
    if (sourcePath == null) {
      // Opaque file URIs (e.g. "file:relative", no authority) have a null path; fail with a clear
      // message rather than a context-free NullPointerException.
      throw new IOException("file uri has no path: " + SafeUri.redact(uri));
    }
    // Resolve to an absolute path: a relative source would otherwise be stored as the symlink
    // target and resolved against the link's own directory, producing a dangling symlink.
    Path srcPath = new File(sourcePath).toPath().toAbsolutePath().normalize();
    if (!Files.exists(srcPath)) {
      throw new IOException(
          String.format("Source file does not exist: %s", srcPath.toAbsolutePath()));
    }

    Path destPath = destFile.toPath().toAbsolutePath().normalize();
    // Skip if the symlink already points to the correct target.
    if (Files.isSymbolicLink(destPath)
        && Files.readSymbolicLink(destPath).normalize().equals(srcPath)) {
      return;
    }
    // Replace via a temporary symlink + rename to minimize the window where the destination path
    // is absent (which could otherwise cause a concurrent reader, e.g. loginUserFromKeytab, to
    // fail). REPLACE_EXISTING is used here; on common local filesystems (ext4, xfs, APFS) a
    // same-directory rename is effectively atomic at the OS level.
    Path tmpPath = destPath.resolveSibling(destPath.getFileName() + ".symlink.tmp");
    Files.deleteIfExists(tmpPath);
    Files.createSymbolicLink(tmpPath, srcPath);
    try {
      Files.move(tmpPath, destPath, StandardCopyOption.REPLACE_EXISTING);
    } catch (Throwable e) {
      // Do not orphan the temporary symlink if the rename fails (e.g. read-only or cross-device
      // destination). This method is synchronized on the singleton, so the fixed temp name cannot
      // race with a concurrent local fetch.
      try {
        Files.deleteIfExists(tmpPath);
      } catch (IOException suppressed) {
        e.addSuppressed(suppressed);
      }
      throw e;
    }
  }

  /**
   * Copies an {@code hdfs} file to the local destination reflectively, so that this class does not
   * require a compile-time dependency on Hadoop.
   */
  private void copyHdfsFileToLocal(URI uri, File destFile, Optional<Object> hadoopConfiguration)
      throws IOException {
    Object configuration =
        hadoopConfiguration.orElseThrow(
            () ->
                new IllegalArgumentException(
                    String.format(
                        "A Hadoop configuration is required to fetch an 'hdfs' uri: %s",
                        SafeUri.redact(uri))));
    try {
      Class<?> configurationClass = Class.forName("org.apache.hadoop.conf.Configuration");
      Class<?> fileSystemClass = Class.forName("org.apache.hadoop.fs.FileSystem");
      Class<?> pathClass = Class.forName("org.apache.hadoop.fs.Path");

      Object fileSystem =
          fileSystemClass.getMethod("get", configurationClass).invoke(null, configuration);
      Object srcPath = pathClass.getConstructor(URI.class).newInstance(uri);
      Object destPath = pathClass.getConstructor(URI.class).newInstance(destFile.toURI());
      fileSystemClass
          .getMethod("copyToLocalFile", pathClass, pathClass)
          .invoke(fileSystem, srcPath, destPath);
    } catch (ReflectiveOperationException e) {
      throw new IOException(
          String.format("Failed to fetch file from hdfs uri: %s", SafeUri.redact(uri)), e);
    }
  }
}
