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
package org.apache.gravitino.integration.test.container;

import com.github.dockerjava.api.DockerClient;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.zip.GZIPOutputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.ContainerState;

/**
 * Copies container directories to the host as tar archives through Docker's archive API, in place
 * of running {@code tar} inside the container and copying the result.
 */
final class ContainerLogs {

  private ContainerLogs() {}

  /**
   * Writes {@code containerPaths} into one archive at {@code destination}, gzipped when its name
   * ends in {@code .tgz} or {@code .tar.gz}. Entries carry their full container path without the
   * leading slash, as {@code tar cf} inside the container stored them.
   *
   * @param container the running container
   * @param destination the archive to write; replaced if it exists
   * @param containerPaths the files or directories to archive
   * @throws IOException if Docker or the destination cannot be read or written
   */
  static void copyArchive(ContainerState container, Path destination, String... containerPaths)
      throws IOException {
    DockerClient docker = DockerClientFactory.instance().client();
    try (OutputStream file = Files.newOutputStream(destination);
        OutputStream out = isGzip(destination) ? new GZIPOutputStream(file) : file;
        TarArchiveOutputStream tar = new TarArchiveOutputStream(out)) {
      tar.setLongFileMode(TarArchiveOutputStream.LONGFILE_POSIX);
      tar.setBigNumberMode(TarArchiveOutputStream.BIGNUMBER_POSIX);
      for (String path : containerPaths) {
        try (InputStream archive =
            docker.copyArchiveFromContainerCmd(container.getContainerId(), path).exec()) {
          appendEntries(archive, path, tar);
        }
      }
    }
  }

  /**
   * Appends the entries of Docker's archive of {@code containerPath} to {@code tar}, renamed by
   * {@link #entryName}.
   *
   * @param dockerArchive the tar stream Docker returned
   * @param containerPath the archived path
   * @param tar the archive being written
   * @throws IOException if either archive cannot be read or written
   */
  static void appendEntries(
      InputStream dockerArchive, String containerPath, TarArchiveOutputStream tar)
      throws IOException {
    TarArchiveInputStream entries = new TarArchiveInputStream(dockerArchive);
    TarArchiveEntry entry;
    while ((entry = entries.getNextEntry()) != null) {
      entry.setName(entryName(containerPath, entry.getName()));
      if (entry.isLink()) {
        entry.setLinkName(entryName(containerPath, entry.getLinkName()));
      }
      tar.putArchiveEntry(entry);
      entries.transferTo(tar);
      tar.closeArchiveEntry();
    }
  }

  /**
   * Returns the name an entry of Docker's archive of {@code containerPath} gets in the copy. Docker
   * names entries from the path's last element, {@code logs/x.log} for {@code
   * /usr/local/hadoop/logs}; the copy prefixes the parent, {@code usr/local/hadoop/logs/x.log}.
   *
   * @param containerPath the archived path
   * @param entryName the entry's name in Docker's archive
   * @return the entry's name in the copy
   */
  static String entryName(String containerPath, String entryName) {
    String path = containerPath.replaceAll("/+$", "");
    String parent = path.substring(0, path.lastIndexOf('/') + 1).replaceFirst("^/+", "");
    return parent + entryName;
  }

  /**
   * Returns {@code fileName} with the container's per-run name before its extension, so the logs of
   * a class-scoped container do not replace those of the shared one: {@code hive.tar} becomes
   * {@code hive-gravitino-hive-1a2b3c4d-3.tar}.
   *
   * @param fileName the archive's base name, such as {@code hive.tar}
   * @param container the running container
   * @return the file name
   */
  static String fileName(String fileName, ContainerState container) {
    return fileName(fileName, container.getContainerInfo().getName());
  }

  static String fileName(String fileName, String containerName) {
    String name = containerName.replaceFirst("^/+", "");
    int dot = fileName.indexOf('.');
    return dot < 0
        ? fileName + "-" + name
        : fileName.substring(0, dot) + "-" + name + fileName.substring(dot);
  }

  private static boolean isGzip(Path destination) {
    String name = destination.getFileName().toString();
    return name.endsWith(".tgz") || name.endsWith(".tar.gz");
  }
}
