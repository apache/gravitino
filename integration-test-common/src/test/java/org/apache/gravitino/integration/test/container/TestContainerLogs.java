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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestContainerLogs {

  @Test
  void testEntryNameKeepsTheFullContainerPath() {
    Assertions.assertEquals(
        "usr/local/hadoop/logs/nn.log",
        ContainerLogs.entryName("/usr/local/hadoop/logs/", "logs/nn.log"));
    Assertions.assertEquals("tmp/root/", ContainerLogs.entryName("/tmp/root", "root/"));
    Assertions.assertEquals("logs/a", ContainerLogs.entryName("/logs", "logs/a"));
  }

  @Test
  void testFileNameCarriesTheContainerName() {
    Assertions.assertEquals(
        "hive-gravitino-hive-1a2b3c4d-3.tar",
        ContainerLogs.fileName("hive.tar", "/gravitino-hive-1a2b3c4d-3"));
    Assertions.assertEquals(
        "hive-logs-c1.tar.gz", ContainerLogs.fileName("hive-logs.tar.gz", "c1"));
    Assertions.assertEquals("logs-c1", ContainerLogs.fileName("logs", "c1"));
  }

  @Test
  void testAppendEntriesMergesArchivesUnderTheirContainerPaths() throws IOException {
    ByteArrayOutputStream merged = new ByteArrayOutputStream();
    try (TarArchiveOutputStream tar = new TarArchiveOutputStream(merged)) {
      ContainerLogs.appendEntries(
          dockerArchive("logs/", "logs/nn.log"), "/usr/local/hadoop/logs", tar);
      ContainerLogs.appendEntries(dockerArchive("root/", "root/hive.log"), "/tmp/root/", tar);
    }

    List<String> entries = new ArrayList<>();
    try (TarArchiveInputStream in =
        new TarArchiveInputStream(new ByteArrayInputStream(merged.toByteArray()))) {
      TarArchiveEntry entry;
      while ((entry = in.getNextEntry()) != null) {
        String body =
            entry.isDirectory() ? "" : new String(in.readAllBytes(), StandardCharsets.UTF_8);
        entries.add(entry.getName() + "=" + body);
      }
    }
    Assertions.assertEquals(
        List.of(
            "usr/local/hadoop/logs/=",
            "usr/local/hadoop/logs/nn.log=logs/nn.log",
            "tmp/root/=",
            "tmp/root/hive.log=root/hive.log"),
        entries);
  }

  // An archive shaped like Docker's: the directory, then a file whose content is its own name.
  private static ByteArrayInputStream dockerArchive(String dir, String file) throws IOException {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (TarArchiveOutputStream tar = new TarArchiveOutputStream(bytes)) {
      tar.putArchiveEntry(new TarArchiveEntry(dir));
      tar.closeArchiveEntry();
      byte[] content = file.getBytes(StandardCharsets.UTF_8);
      TarArchiveEntry entry = new TarArchiveEntry(file);
      entry.setSize(content.length);
      tar.putArchiveEntry(entry);
      tar.write(content);
      tar.closeArchiveEntry();
    }
    return new ByteArrayInputStream(bytes.toByteArray());
  }
}
