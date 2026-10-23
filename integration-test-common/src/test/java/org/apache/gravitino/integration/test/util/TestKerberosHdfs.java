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
package org.apache.gravitino.integration.test.util;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.fs.permission.FsPermission;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestKerberosHdfs {

  @TempDir java.nio.file.Path tempDir;

  private RecordingFileSystem fs;
  private Path root;

  @BeforeEach
  void setUp() throws IOException {
    fs = new RecordingFileSystem();
    fs.initialize(URI.create("file:///"), new Configuration());
    root = new Path(tempDir.toUri());
    Files.createDirectories(tempDir.resolve("a/b"));
    Files.createFile(tempDir.resolve("a/b/file"));
    Files.createFile(tempDir.resolve("top"));
  }

  @Test
  void testRejectsBadArguments() {
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> new KerberosHdfs("file:///", "host", "/k"));
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> new KerberosHdfs("hdfs://h:9000", " ", "/k"));
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> new KerberosHdfs("hdfs://h:9000", "host", null));
    Assertions.assertDoesNotThrow(() -> new KerberosHdfs("hdfs://h:9000", "host", "/k"));
  }

  @Test
  void testRecursivePermissionReachesEveryPath() throws IOException {
    FsPermission mode = new FsPermission(Short.parseShort("750", 8));

    KerberosHdfs.setPermission(fs, root, mode, true);

    for (String path : new String[] {"", "a", "a/b", "a/b/file", "top"}) {
      Path p = path.isEmpty() ? root : new Path(root, path);
      Assertions.assertEquals(mode, fs.getFileStatus(p).getPermission(), path);
    }
  }

  @Test
  void testNonRecursivePermissionChangesOnlyThePath() throws IOException {
    FsPermission before = fs.getFileStatus(new Path(root, "a")).getPermission();
    FsPermission mode = new FsPermission(Short.parseShort("700", 8));

    KerberosHdfs.setPermission(fs, root, mode, false);

    Assertions.assertEquals(mode, fs.getFileStatus(root).getPermission());
    Assertions.assertEquals(before, fs.getFileStatus(new Path(root, "a")).getPermission());
  }

  @Test
  void testRecursiveOwnerKeepsGroupAndReachesEveryPath() throws IOException {
    KerberosHdfs.setOwner(fs, root, "cli", true);

    Assertions.assertEquals(5, fs.owners.size());
    Assertions.assertTrue(fs.owners.stream().allMatch(call -> call.endsWith("cli:null")));
  }

  @Test
  void testNonRecursiveOwnerChangesOnlyThePath() throws IOException {
    KerberosHdfs.setOwner(fs, root, "cli", false);

    Assertions.assertEquals(List.of(fs.makeQualified(root) + " cli:null"), fs.owners);
  }

  // Changing owners locally needs root, so record the calls instead.
  private static class RecordingFileSystem extends RawLocalFileSystem {
    private final List<String> owners = new ArrayList<>();

    @Override
    public void setOwner(Path p, String username, String groupname) {
      owners.add(makeQualified(p) + " " + username + ":" + groupname);
    }
  }
}
