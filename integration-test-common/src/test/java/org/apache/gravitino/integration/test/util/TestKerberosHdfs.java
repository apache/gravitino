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
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.util.KerberosName;
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
  void testReachableKrb5ConfPointsAtTheContainer() {
    String conf =
        "[realms]\n HADOOPKRB = {\n  kdc = localhost:88\n  admin_server = localhost\n }\n";

    Assertions.assertEquals(
        "[realms]\n HADOOPKRB = {\n  kdc = 172.18.0.2:88\n  admin_server = 172.18.0.2:749\n }\n",
        KerberosHdfs.reachableKrb5Conf(conf, "172.18.0.2"));
  }

  @Test
  void testWithKrb5ConfRestoresTheJvmSettings() throws IOException {
    String key = "java.security.krb5.conf";
    String before = System.getProperty(key);
    java.nio.file.Path krb5Conf =
        Files.writeString(
            tempDir.resolve("krb5.conf"), "[libdefaults]\n default_realm = HADOOPKRB\n");
    try {
      System.clearProperty(key);
      List<String> seen = new ArrayList<>();

      KerberosHdfs.withKrb5Conf(krb5Conf.toString(), () -> seen.add(System.getProperty(key)));

      Assertions.assertEquals(List.of(krb5Conf.toString()), seen);
      Assertions.assertNull(System.getProperty(key));
      Assertions.assertFalse(UserGroupInformation.isSecurityEnabled());
    } finally {
      if (before != null) {
        System.setProperty(key, before);
      }
    }
  }

  // HDFSKerberosIT sets java.security.krb5.conf after the readiness check and does not reload the
  // JDK's cached configuration, so the check must leave the caches as it found them.
  @Test
  void testWithKrb5ConfLeavesTheKerberosCachesAsTheyWere() throws IOException {
    String key = "java.security.krb5.conf";
    String before = System.getProperty(key);
    java.nio.file.Path krb5Conf =
        Files.writeString(
            tempDir.resolve("krb5.conf"), "[libdefaults]\n default_realm = HADOOPKRB\n");
    try {
      System.clearProperty(key);
      Object config = KerberosHdfs.readStatic(KerberosHdfs.KRB5_CONFIG_CLASS, "singleton");
      Object realm = KerberosHdfs.readStatic(KerberosName.class.getName(), "defaultRealm");
      List<String> seen = new ArrayList<>();

      KerberosHdfs.withKrb5Conf(
          krb5Conf.toString(),
          () -> {
            KerberosName.resetDefaultRealm();
            seen.add(KerberosName.getDefaultRealm());
          });

      Assertions.assertEquals(List.of("HADOOPKRB"), seen);
      Assertions.assertSame(
          config, KerberosHdfs.readStatic(KerberosHdfs.KRB5_CONFIG_CLASS, "singleton"));
      Assertions.assertSame(
          realm, KerberosHdfs.readStatic(KerberosName.class.getName(), "defaultRealm"));
    } finally {
      if (before != null) {
        System.setProperty(key, before);
      }
    }
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
