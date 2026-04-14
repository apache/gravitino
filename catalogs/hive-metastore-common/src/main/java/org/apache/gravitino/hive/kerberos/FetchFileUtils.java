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
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class FetchFileUtils {

  /**
   * Per-destination lock map used to serialize concurrent symlink creation for the same keytab
   * file. Keyed by the normalized absolute destination path string to avoid races caused by
   * different path spellings referring to the same file. Entries are removed when the corresponding
   * {@link KerberosClient} is closed, so the map size is bounded by the number of live catalogs.
   */
  private static final ConcurrentHashMap<String, Object> SYMLINK_LOCKS = new ConcurrentHashMap<>();

  private FetchFileUtils() {}

  /**
   * Removes the per-destination lock entry for the given file. Should be called when the keytab
   * file is deleted (e.g., on {@link KerberosClient#close()}) to prevent unbounded map growth.
   *
   * @param destFile the keytab destination file whose lock entry should be removed
   */
  static void removeLock(File destFile) {
    SYMLINK_LOCKS.remove(destFile.toPath().toAbsolutePath().normalize().toString());
  }

  public static void fetchFileFromUri(
      String fileUri, File destFile, int timeout, Configuration conf) throws IOException {
    try {
      URI uri = new URI(fileUri);
      String scheme = Optional.ofNullable(uri.getScheme()).orElse("file");

      switch (scheme) {
        case "http":
        case "https":
        case "ftp":
          FileUtils.copyURLToFile(uri.toURL(), destFile, timeout * 1000, timeout * 1000);
          break;

        case "file":
          var srcPath = new File(uri.getPath()).toPath().normalize();
          var destPath = destFile.toPath().toAbsolutePath().normalize();
          Object lock = SYMLINK_LOCKS.computeIfAbsent(destPath.toString(), k -> new Object());
          synchronized (lock) {
            // Skip if the symlink already points to the correct target.
            if (Files.isSymbolicLink(destPath)
                && Files.readSymbolicLink(destPath).normalize().equals(srcPath)) {
              break;
            }
            // Replace via a temporary symlink + rename to minimize the window where the
            // keytab path is absent (which could cause loginUserFromKeytab to fail).
            // REPLACE_EXISTING is used here; on common local filesystems (ext4, xfs, APFS)
            // a same-directory rename is effectively atomic at the OS level.
            var tmpPath = destPath.resolveSibling(destPath.getFileName() + ".symlink.tmp");
            Files.deleteIfExists(tmpPath);
            Files.createSymbolicLink(tmpPath, srcPath);
            Files.move(tmpPath, destPath, StandardCopyOption.REPLACE_EXISTING);
          }
          break;

        case "hdfs":
          FileSystem.get(conf).copyToLocalFile(new Path(uri), new Path(destFile.toURI()));
          break;

        default:
          throw new IllegalArgumentException(
              String.format("The scheme '%s' is not supported", scheme));
      }
    } catch (URISyntaxException ue) {
      throw new IllegalArgumentException("The uri of file has the wrong format", ue);
    }
  }
}
