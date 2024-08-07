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
package org.apache.gravitino.filesystem.hadoop;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.file.Fileset;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class TestGvfsBase extends GravitinoMockServerBase {
  protected static final String GVFS_IMPL_CLASS = GravitinoVirtualFileSystem.class.getName();
  protected static final String GVFS_ABSTRACT_IMPL_CLASS = Gvfs.class.getName();
  protected static Configuration conf = new Configuration();
  protected Path localDirPath = null;
  protected Path localFilePath = null;
  protected Path managedFilesetPath = null;
  protected Path externalFilesetPath = null;

  @BeforeAll
  public static void setup() {
    GravitinoMockServerBase.setup();
    conf.set("fs.gvfs.impl", GVFS_IMPL_CLASS);
    conf.set("fs.AbstractFileSystem.gvfs.impl", GVFS_ABSTRACT_IMPL_CLASS);
    conf.set(
        GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_SERVER_URI_KEY,
        GravitinoMockServerBase.serverUri());
    conf.set(
        GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_METALAKE_KEY, metalakeName);
    // close the cache
    conf.set(
        String.format(
            "fs.%s.impl.disable.cache", GravitinoVirtualFileSystemConfiguration.GVFS_SCHEME),
        "true");
  }

  @AfterAll
  public static void tearDown() {
    GravitinoMockServerBase.tearDown();
    String fileName = FileSystemTestUtils.localRootPrefix().replace("file:", "");
    try {
      FileUtils.deleteDirectory(new File(fileName));
    } catch (Exception e) {
      // Ignore
    }
  }

  @BeforeEach
  public void init() {
    mockMetalakeDTO(metalakeName, "comment");
    mockCatalogDTO(catalogName, provider, "comment");

    localDirPath =
        FileSystemTestUtils.createLocalDirPrefix(catalogName, schemaName, managedFilesetName);
    mockFilesetDTO(
        metalakeName,
        catalogName,
        schemaName,
        managedFilesetName,
        Fileset.Type.MANAGED,
        localDirPath.toString());
    managedFilesetPath =
        FileSystemTestUtils.createFilesetPath(catalogName, schemaName, managedFilesetName, true);

    localFilePath =
        new Path(
            FileSystemTestUtils.createLocalDirPrefix(catalogName, schemaName, externalFilesetName)
                + "/test.txt");
    mockFilesetDTO(
        metalakeName,
        catalogName,
        schemaName,
        externalFilesetName,
        Fileset.Type.EXTERNAL,
        localFilePath.toString());
    externalFilesetPath =
        FileSystemTestUtils.createFilesetPath(catalogName, schemaName, externalFilesetName, true);
  }

  @AfterEach
  public void destroy() throws IOException {
    Path localRootPath = FileSystemTestUtils.createLocalRootDir(catalogName);
    try (FileSystem localFileSystem = localDirPath.getFileSystem(conf)) {
      if (localFileSystem.exists(localRootPath)) {
        localFileSystem.delete(localRootPath, true);
      }
    }
  }

  @Test
  public void testFSCache() throws IOException {
    try (FileSystem gravitinoFileSystem = managedFilesetPath.getFileSystem(conf);
        FileSystem localFileSystem = localDirPath.getFileSystem(conf)) {
      FileSystemTestUtils.mkdirs(managedFilesetPath, gravitinoFileSystem);

      Configuration conf1 = localFileSystem.getConf();
      assertEquals(
          "true",
          conf1.get(
              String.format(
                  "fs.%s.impl.disable.cache",
                  GravitinoVirtualFileSystemConfiguration.GVFS_SCHEME)));

      Configuration conf2 = gravitinoFileSystem.getConf();
      assertEquals(
          "true",
          conf2.get(
              String.format(
                  "fs.%s.impl.disable.cache",
                  GravitinoVirtualFileSystemConfiguration.GVFS_SCHEME)));

      // test gvfs, should not get the same fs
      try (FileSystem externalFs = externalFilesetPath.getFileSystem(conf)) {
        assertNotEquals(externalFs, gravitinoFileSystem);
      }

      // test proxied local fs, should not get the same fs
      FileSystemTestUtils.mkdirs(managedFilesetPath, gravitinoFileSystem);
      FileSystem proxyLocalFs =
          Objects.requireNonNull(
                  ((GravitinoVirtualFileSystem) gravitinoFileSystem)
                      .getFilesetCache()
                      .getIfPresent(
                          NameIdentifier.of(
                              metalakeName, catalogName, schemaName, managedFilesetName)))
              .getRight();

      String anotherFilesetName = "test_new_fs";
      Path diffLocalPath =
          FileSystemTestUtils.createLocalDirPrefix(catalogName, schemaName, anotherFilesetName);
      try (FileSystem localFs = diffLocalPath.getFileSystem(conf)) {
        assertNotEquals(localFs, proxyLocalFs);
        localFs.delete(diffLocalPath, true);
      }
    }
  }

  @Test
  public void testInternalCache() throws IOException {
    Configuration configuration = new Configuration(conf);
    configuration.set(
        GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_FILESET_CACHE_MAX_CAPACITY_KEY, "1");
    configuration.set(
        GravitinoVirtualFileSystemConfiguration
            .FS_GRAVITINO_FILESET_CACHE_EVICTION_MILLS_AFTER_ACCESS_KEY,
        "1000");

    Path filesetPath1 =
        FileSystemTestUtils.createFilesetPath(catalogName, schemaName, "fileset1", true);
    try (FileSystem fs = filesetPath1.getFileSystem(configuration)) {
      Path localPath1 =
          FileSystemTestUtils.createLocalDirPrefix(catalogName, schemaName, "fileset1");
      mockFilesetDTO(
          metalakeName,
          catalogName,
          schemaName,
          "fileset1",
          Fileset.Type.MANAGED,
          localPath1.toString());
      FileSystemTestUtils.mkdirs(filesetPath1, fs);

      // expired by size
      Path filesetPath2 =
          FileSystemTestUtils.createFilesetPath(catalogName, schemaName, "fileset2", true);
      Path localPath2 =
          FileSystemTestUtils.createLocalDirPrefix(catalogName, schemaName, "fileset2");
      mockFilesetDTO(
          metalakeName,
          catalogName,
          schemaName,
          "fileset2",
          Fileset.Type.MANAGED,
          localPath2.toString());
      FileSystemTestUtils.mkdirs(filesetPath2, fs);

      Awaitility.await()
          .atMost(5, TimeUnit.SECONDS)
          .pollInterval(1, TimeUnit.SECONDS)
          .untilAsserted(
              () ->
                  assertNull(
                      ((GravitinoVirtualFileSystem) fs)
                          .getFilesetCache()
                          .getIfPresent(
                              NameIdentifier.of(
                                  metalakeName, catalogName, schemaName, "fileset1"))));

      // expired by time
      Awaitility.await()
          .atMost(5, TimeUnit.SECONDS)
          .pollInterval(1, TimeUnit.SECONDS)
          .untilAsserted(
              () ->
                  assertEquals(
                      0, ((GravitinoVirtualFileSystem) fs).getFilesetCache().asMap().size()));

      assertNull(
          ((GravitinoVirtualFileSystem) fs)
              .getFilesetCache()
              .getIfPresent(NameIdentifier.of(metalakeName, catalogName, schemaName, "fileset2")));
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testCreate(boolean withScheme) throws IOException {
    try (FileSystem gravitinoFileSystem = managedFilesetPath.getFileSystem(conf);
        FileSystem localFileSystem = localDirPath.getFileSystem(conf)) {
      FileSystemTestUtils.mkdirs(managedFilesetPath, gravitinoFileSystem);

      // test managed fileset create
      Path filePath = new Path(managedFilesetPath + "/test.txt");
      FileSystemTestUtils.create(filePath, gravitinoFileSystem);
      assertTrue(gravitinoFileSystem.exists(filePath));
      gravitinoFileSystem.delete(filePath, true);

      // mock the invalid fileset not in the server
      String invalidFilesetName = "invalid_fileset";
      Path invalidFilesetPath =
          FileSystemTestUtils.createFilesetPath(
              catalogName, schemaName, invalidFilesetName, withScheme);
      assertThrows(
          RuntimeException.class,
          () -> FileSystemTestUtils.create(invalidFilesetPath, gravitinoFileSystem));

      // mock the not correct protocol prefix path
      Path localPrefixPath =
          FileSystemTestUtils.createLocalDirPrefix(catalogName, schemaName, "test");
      assertThrows(
          RuntimeException.class,
          () -> FileSystemTestUtils.create(localPrefixPath, gravitinoFileSystem));

      // test external fileset mounts a single file
      FileSystemTestUtils.create(externalFilesetPath, gravitinoFileSystem);
      assertTrue(gravitinoFileSystem.exists(externalFilesetPath));
      assertTrue(gravitinoFileSystem.getFileStatus(externalFilesetPath).isFile());
      gravitinoFileSystem.delete(externalFilesetPath, true);
      assertFalse(localFileSystem.exists(localFilePath));
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  @Disabled("Append operation is not supported in LocalFileSystem. We can't test it now.")
  public void testAppend(boolean withScheme) throws IOException {
    try (FileSystem gravitinoFileSystem = managedFilesetPath.getFileSystem(conf);
        FileSystem localFileSystem = localDirPath.getFileSystem(conf)) {
      FileSystemTestUtils.mkdirs(managedFilesetPath, gravitinoFileSystem);

      // test managed fileset append
      Path appendFile = new Path(managedFilesetPath + "/test.txt");
      FileSystemTestUtils.create(appendFile, gravitinoFileSystem);
      FileSystemTestUtils.append(appendFile, gravitinoFileSystem);
      assertTrue(gravitinoFileSystem.exists(appendFile));
      assertTrue(gravitinoFileSystem.getFileStatus(appendFile).isFile());
      assertEquals(
          "Hello, World!",
          new String(
              FileSystemTestUtils.read(appendFile, gravitinoFileSystem), StandardCharsets.UTF_8));
      gravitinoFileSystem.delete(appendFile, true);

      // mock the invalid fileset not in server
      String invalidAppendFilesetName = "invalid_fileset";
      Path invalidAppendFilesetPath =
          FileSystemTestUtils.createFilesetPath(
              catalogName, schemaName, invalidAppendFilesetName, withScheme);
      assertThrows(
          RuntimeException.class,
          () -> FileSystemTestUtils.append(invalidAppendFilesetPath, gravitinoFileSystem));

      // mock the not correct protocol path
      Path localPrefixPath =
          FileSystemTestUtils.createLocalDirPrefix(catalogName, schemaName, "test");
      assertThrows(
          RuntimeException.class,
          () -> FileSystemTestUtils.append(localPrefixPath, gravitinoFileSystem));

      // test external fileset mounts the single file
      FileSystemTestUtils.create(externalFilesetPath, gravitinoFileSystem);
      FileSystemTestUtils.append(externalFilesetPath, gravitinoFileSystem);
      assertTrue(gravitinoFileSystem.exists(externalFilesetPath));
      assertTrue(gravitinoFileSystem.getFileStatus(externalFilesetPath).isFile());
      gravitinoFileSystem.delete(externalFilesetPath, true);
      assertFalse(localFileSystem.exists(localFilePath));
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testRename(boolean withScheme) throws IOException {
    try (FileSystem gravitinoFileSystem = managedFilesetPath.getFileSystem(conf);
        FileSystem localFileSystem = localDirPath.getFileSystem(conf)) {
      FileSystemTestUtils.mkdirs(managedFilesetPath, gravitinoFileSystem);

      // test managed fileset rename
      Path srcRenamePath = new Path(managedFilesetPath + "/rename_src");
      gravitinoFileSystem.mkdirs(srcRenamePath);
      assertTrue(gravitinoFileSystem.getFileStatus(srcRenamePath).isDirectory());
      assertTrue(gravitinoFileSystem.exists(srcRenamePath));

      // cannot rename the identifier
      Path dstRenamePath1 =
          FileSystemTestUtils.createFilesetPath(catalogName, schemaName, "rename_dst1", withScheme);
      assertThrows(
          RuntimeException.class, () -> gravitinoFileSystem.rename(srcRenamePath, dstRenamePath1));

      Path dstRenamePath2 = new Path(managedFilesetPath + "/rename_dst2");
      gravitinoFileSystem.rename(srcRenamePath, dstRenamePath2);
      assertFalse(gravitinoFileSystem.exists(srcRenamePath));
      assertTrue(gravitinoFileSystem.exists(dstRenamePath2));
      gravitinoFileSystem.delete(dstRenamePath2, true);

      // test invalid src path
      Path invalidSrcPath =
          FileSystemTestUtils.createFilesetPath(
              catalogName, schemaName, "invalid_src_name", withScheme);
      Path validDstPath =
          FileSystemTestUtils.createFilesetPath(
              catalogName, schemaName, managedFilesetName, withScheme);
      assertThrows(
          RuntimeException.class, () -> gravitinoFileSystem.rename(invalidSrcPath, validDstPath));

      // test invalid dst path
      Path invalidDstPath =
          FileSystemTestUtils.createFilesetPath(
              catalogName, schemaName, "invalid_dst_name", withScheme);
      assertThrows(
          RuntimeException.class,
          () -> gravitinoFileSystem.rename(managedFilesetPath, invalidDstPath));

      // test external fileset mount the single file
      FileSystemTestUtils.create(externalFilesetPath, gravitinoFileSystem);
      assertTrue(gravitinoFileSystem.exists(externalFilesetPath));
      assertTrue(gravitinoFileSystem.getFileStatus(externalFilesetPath).isFile());

      Path dstPath =
          FileSystemTestUtils.createFilesetPath(catalogName, schemaName, "rename_dst", withScheme);
      assertThrows(
          RuntimeException.class, () -> gravitinoFileSystem.rename(externalFilesetPath, dstPath));
      localFileSystem.delete(localFilePath, true);
      assertFalse(localFileSystem.exists(localFilePath));
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testDelete(boolean withScheme) throws IOException {
    try (FileSystem gravitinoFileSystem = managedFilesetPath.getFileSystem(conf);
        FileSystem localFileSystem = localDirPath.getFileSystem(conf)) {

      FileSystemTestUtils.mkdirs(managedFilesetPath, gravitinoFileSystem);

      // test managed fileset delete
      FileSystemTestUtils.mkdirs(managedFilesetPath, gravitinoFileSystem);
      gravitinoFileSystem.delete(managedFilesetPath, true);
      assertFalse(gravitinoFileSystem.exists(managedFilesetPath));

      // mock the invalid fileset not in server
      String invalidFilesetName = "invalid_fileset";
      Path invalidFilesetPath =
          FileSystemTestUtils.createFilesetPath(
              catalogName, schemaName, invalidFilesetName, withScheme);
      assertThrows(
          RuntimeException.class, () -> gravitinoFileSystem.delete(invalidFilesetPath, true));

      // mock the not correct protocol path
      Path localPrefixPath =
          FileSystemTestUtils.createLocalDirPrefix(catalogName, schemaName, "test");
      assertThrows(RuntimeException.class, () -> gravitinoFileSystem.delete(localPrefixPath, true));

      // test external fileset mounts the single file
      FileSystemTestUtils.create(externalFilesetPath, gravitinoFileSystem);
      assertTrue(gravitinoFileSystem.exists(externalFilesetPath));
      gravitinoFileSystem.delete(externalFilesetPath, true);
      assertFalse(gravitinoFileSystem.exists(externalFilesetPath));
      assertFalse(localFileSystem.exists(localFilePath));
    }
  }

  @Test
  public void testGetStatus() throws IOException {
    try (FileSystem gravitinoFileSystem = managedFilesetPath.getFileSystem(conf);
        FileSystem localFileSystem = localDirPath.getFileSystem(conf)) {
      FileSystemTestUtils.mkdirs(managedFilesetPath, gravitinoFileSystem);

      FileSystemTestUtils.mkdirs(managedFilesetPath, gravitinoFileSystem);
      assertTrue(gravitinoFileSystem.exists(managedFilesetPath));
      assertTrue(gravitinoFileSystem.getFileStatus(managedFilesetPath).isDirectory());
      assertTrue(localFileSystem.exists(localDirPath));

      FileStatus gravitinoStatus = gravitinoFileSystem.getFileStatus(managedFilesetPath);
      FileStatus localStatus = localFileSystem.getFileStatus(localDirPath);
      gravitinoFileSystem.delete(managedFilesetPath, true);

      assertFalse(gravitinoFileSystem.exists(managedFilesetPath));
      assertEquals(
          localStatus.getPath().toString(),
          gravitinoStatus
              .getPath()
              .toString()
              .replaceFirst(
                  GravitinoVirtualFileSystemConfiguration.GVFS_FILESET_PREFIX,
                  FileSystemTestUtils.localRootPrefix()));
    }
  }

  @Test
  public void testListStatus() throws IOException {
    try (FileSystem gravitinoFileSystem = managedFilesetPath.getFileSystem(conf);
        FileSystem localFileSystem = localDirPath.getFileSystem(conf)) {

      FileSystemTestUtils.mkdirs(managedFilesetPath, gravitinoFileSystem);

      FileSystemTestUtils.mkdirs(managedFilesetPath, gravitinoFileSystem);
      assertTrue(gravitinoFileSystem.exists(managedFilesetPath));
      assertTrue(gravitinoFileSystem.getFileStatus(managedFilesetPath).isDirectory());
      assertTrue(localFileSystem.exists(localDirPath));

      for (int i = 0; i < 5; i++) {
        Path subPath = new Path(managedFilesetPath + "/sub" + i);
        FileSystemTestUtils.mkdirs(subPath, gravitinoFileSystem);
        assertTrue(gravitinoFileSystem.exists(subPath));
        assertTrue(gravitinoFileSystem.getFileStatus(subPath).isDirectory());
      }

      List<FileStatus> gravitinoStatuses =
          new ArrayList<>(Arrays.asList(gravitinoFileSystem.listStatus(managedFilesetPath)));
      gravitinoStatuses.sort(Comparator.comparing(FileStatus::getPath));
      assertEquals(5, gravitinoStatuses.size());

      List<FileStatus> localStatuses =
          new ArrayList<>(Arrays.asList(localFileSystem.listStatus(localDirPath)));
      localStatuses.sort(Comparator.comparing(FileStatus::getPath));
      assertEquals(5, localStatuses.size());

      for (int i = 0; i < 5; i++) {
        assertEquals(
            localStatuses.get(i).getPath().toString(),
            gravitinoStatuses
                .get(i)
                .getPath()
                .toString()
                .replaceFirst(
                    GravitinoVirtualFileSystemConfiguration.GVFS_FILESET_PREFIX,
                    FileSystemTestUtils.localRootPrefix()));
        gravitinoFileSystem.delete(gravitinoStatuses.get(i).getPath(), true);
      }
    }
  }

  @Test
  public void testMkdirs() throws IOException {
    try (FileSystem gravitinoFileSystem = managedFilesetPath.getFileSystem(conf);
        FileSystem localFileSystem = localDirPath.getFileSystem(conf)) {

      FileSystemTestUtils.mkdirs(managedFilesetPath, gravitinoFileSystem);

      FileSystemTestUtils.mkdirs(managedFilesetPath, gravitinoFileSystem);
      assertTrue(gravitinoFileSystem.exists(managedFilesetPath));
      assertTrue(gravitinoFileSystem.getFileStatus(managedFilesetPath).isDirectory());

      FileStatus gravitinoStatus = gravitinoFileSystem.getFileStatus(managedFilesetPath);
      FileStatus localStatus = localFileSystem.getFileStatus(localDirPath);
      gravitinoFileSystem.delete(managedFilesetPath, true);
      assertFalse(gravitinoFileSystem.exists(managedFilesetPath));

      assertEquals(
          localStatus.getPath().toString(),
          gravitinoStatus
              .getPath()
              .toString()
              .replaceFirst(
                  GravitinoVirtualFileSystemConfiguration.GVFS_FILESET_PREFIX,
                  FileSystemTestUtils.localRootPrefix()));
    }
  }

  @Test
  public void testExtractIdentifier() throws IOException, URISyntaxException {
    try (GravitinoVirtualFileSystem fs =
        (GravitinoVirtualFileSystem) managedFilesetPath.getFileSystem(conf)) {
      NameIdentifier identifier =
          fs.extractIdentifier(new URI("gvfs://fileset/catalog1/schema1/fileset1"));
      assertEquals(NameIdentifier.of(metalakeName, "catalog1", "schema1", "fileset1"), identifier);

      NameIdentifier identifier2 =
          fs.extractIdentifier(new URI("gvfs://fileset/catalog1/schema1/fileset1/"));
      assertEquals(NameIdentifier.of(metalakeName, "catalog1", "schema1", "fileset1"), identifier2);

      NameIdentifier identifier3 =
          fs.extractIdentifier(new URI("gvfs://fileset/catalog1/schema1/fileset1/files"));
      assertEquals(NameIdentifier.of(metalakeName, "catalog1", "schema1", "fileset1"), identifier3);

      NameIdentifier identifier4 =
          fs.extractIdentifier(new URI("gvfs://fileset/catalog1/schema1/fileset1/dir/dir"));
      assertEquals(NameIdentifier.of(metalakeName, "catalog1", "schema1", "fileset1"), identifier4);

      NameIdentifier identifier5 =
          fs.extractIdentifier(new URI("gvfs://fileset/catalog1/schema1/fileset1/dir/dir/"));
      assertEquals(NameIdentifier.of(metalakeName, "catalog1", "schema1", "fileset1"), identifier5);

      NameIdentifier identifier6 = fs.extractIdentifier(new URI("/catalog1/schema1/fileset1"));
      assertEquals(NameIdentifier.of(metalakeName, "catalog1", "schema1", "fileset1"), identifier6);

      NameIdentifier identifier7 = fs.extractIdentifier(new URI("/catalog1/schema1/fileset1/"));
      assertEquals(NameIdentifier.of(metalakeName, "catalog1", "schema1", "fileset1"), identifier7);

      NameIdentifier identifier8 = fs.extractIdentifier(new URI("/catalog1/schema1/fileset1/dir"));
      assertEquals(NameIdentifier.of(metalakeName, "catalog1", "schema1", "fileset1"), identifier8);

      NameIdentifier identifier9 =
          fs.extractIdentifier(new URI("/catalog1/schema1/fileset1/dir/dir/"));
      assertEquals(NameIdentifier.of(metalakeName, "catalog1", "schema1", "fileset1"), identifier9);

      NameIdentifier identifier10 =
          fs.extractIdentifier(new URI("/catalog1/schema1/fileset1/dir/dir"));
      assertEquals(
          NameIdentifier.of(metalakeName, "catalog1", "schema1", "fileset1"), identifier10);

      StringBuilder longUri = new StringBuilder("gvfs://fileset/catalog1/schema1/fileset1");
      for (int i = 0; i < 1500; i++) {
        longUri.append("/dir");
      }
      NameIdentifier identifier11 = fs.extractIdentifier(new URI(longUri.toString()));
      assertEquals(
          NameIdentifier.of(metalakeName, "catalog1", "schema1", "fileset1"), identifier11);

      NameIdentifier identifier12 = fs.extractIdentifier(new URI(longUri.delete(0, 14).toString()));
      assertEquals(
          NameIdentifier.of(metalakeName, "catalog1", "schema1", "fileset1"), identifier12);

      assertThrows(
          IllegalArgumentException.class,
          () -> fs.extractIdentifier(new URI("gvfs://fileset/catalog1/")));
      assertThrows(
          IllegalArgumentException.class,
          () -> fs.extractIdentifier(new URI("hdfs://fileset/catalog1/schema1/fileset1")));
      assertThrows(
          IllegalArgumentException.class,
          () -> fs.extractIdentifier(new URI("/catalog1/schema1/")));
      assertThrows(
          IllegalArgumentException.class,
          () -> fs.extractIdentifier(new URI("gvfs://fileset/catalog1/schema1/fileset1//")));
      assertThrows(
          IllegalArgumentException.class,
          () -> fs.extractIdentifier(new URI("/catalog1/schema1/fileset1/dir//")));
    }
  }

  @Test
  public void testGetDefaultReplications() throws IOException {
    try (GravitinoVirtualFileSystem fs =
        (GravitinoVirtualFileSystem) managedFilesetPath.getFileSystem(conf)) {
      assertEquals(1, fs.getDefaultReplication(managedFilesetPath));
    }
  }

  @Test
  public void testGetDefaultBlockSize() throws IOException {
    try (GravitinoVirtualFileSystem fs =
        (GravitinoVirtualFileSystem) managedFilesetPath.getFileSystem(conf)) {
      assertEquals(32 * 1024 * 1024, fs.getDefaultBlockSize(managedFilesetPath));
    }
  }
}
