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
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Optional;
import javax.annotation.Nullable;
import org.apache.commons.io.FileUtils;

/**
 * Fetches a file referenced by a URI to a local destination. Supports {@code file}, {@code http},
 * {@code https}, {@code ftp} and {@code hdfs} schemes. This is the single shared implementation
 * used by the job manager and the Kerberos clients of the Hive, Iceberg, Hadoop and Paimon
 * catalogs.
 *
 * <p>The {@code hdfs} scheme is resolved reflectively against {@code
 * org.apache.hadoop.fs.FileSystem} so that this class can live in the {@code common} module, which
 * does not declare a compile-time dependency on Hadoop. Callers that never use {@code hdfs} URIs
 * (for example the job manager and the Paimon catalog) simply pass {@code null} for the Hadoop
 * configuration.
 */
public final class FetchFileUtils {

  /** The server configuration that controls unsafe remote URI blocking. */
  public static final String BLOCK_UNSAFE_REMOTE_URI_CONFIG =
      "gravitino.fetchFile.blockUnsafeRemoteUri";

  private FetchFileUtils() {}

  /**
   * Fetches the file referenced by {@code fileUri} into {@code destFile}.
   *
   * @param fileUri the source URI; a missing scheme is treated as {@code file}
   * @param destFile the local destination file
   * @param timeoutMs the connect/read timeout in milliseconds, applied to remote (http/https/ftp)
   *     downloads
   * @param hadoopConf an {@code org.apache.hadoop.conf.Configuration} instance, required only for
   *     the {@code hdfs} scheme; may be {@code null} when no {@code hdfs} URI is fetched
   * @param blockUnsafeRemoteUri whether to block remote URIs that resolve to unsafe addresses
   * @return the absolute path of {@code destFile}
   * @throws IOException if the file cannot be fetched
   */
  public static String fetchFileFromUri(
      String fileUri,
      File destFile,
      int timeoutMs,
      @Nullable Object hadoopConf,
      boolean blockUnsafeRemoteUri)
      throws IOException {
    try {
      URI uri = new URI(fileUri);
      String scheme = Optional.ofNullable(uri.getScheme()).orElse("file");

      switch (scheme) {
        case "http":
        case "https":
        case "ftp":
          RemoteUriValidator.validate(
              uri,
              blockUnsafeRemoteUri,
              String.format("'%s' to false", BLOCK_UNSAFE_REMOTE_URI_CONFIG));
          FileUtils.copyURLToFile(uri.toURL(), destFile, timeoutMs, timeoutMs);
          break;

        case "file":
          linkLocalFile(uri, destFile);
          break;

        case "hdfs":
          copyHdfsFileToLocal(uri, destFile, hadoopConf);
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

  private static synchronized void linkLocalFile(URI uri, File destFile) throws IOException {
    Path srcPath = new File(uri.getPath()).toPath().normalize();
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
    Files.move(tmpPath, destPath, StandardCopyOption.REPLACE_EXISTING);
  }

  /**
   * Copies an {@code hdfs} file to the local destination reflectively, so that this class does not
   * require a compile-time dependency on Hadoop.
   */
  private static void copyHdfsFileToLocal(URI uri, File destFile, @Nullable Object hadoopConf)
      throws IOException {
    if (hadoopConf == null) {
      throw new IllegalArgumentException(
          String.format("A Hadoop configuration is required to fetch an 'hdfs' uri: %s", uri));
    }
    try {
      Class<?> configurationClass = Class.forName("org.apache.hadoop.conf.Configuration");
      Class<?> fileSystemClass = Class.forName("org.apache.hadoop.fs.FileSystem");
      Class<?> pathClass = Class.forName("org.apache.hadoop.fs.Path");

      Object fileSystem =
          fileSystemClass.getMethod("get", configurationClass).invoke(null, hadoopConf);
      Object srcPath = pathClass.getConstructor(URI.class).newInstance(uri);
      Object destPath = pathClass.getConstructor(URI.class).newInstance(destFile.toURI());
      fileSystemClass
          .getMethod("copyToLocalFile", pathClass, pathClass)
          .invoke(fileSystem, srcPath, destPath);
    } catch (ReflectiveOperationException e) {
      throw new IOException(String.format("Failed to fetch file from hdfs uri: %s", uri), e);
    }
  }
}
