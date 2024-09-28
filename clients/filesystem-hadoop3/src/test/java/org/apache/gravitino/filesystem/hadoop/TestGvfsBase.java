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

import static org.apache.hc.core5.http.HttpStatus.SC_OK;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.core.JsonProcessingException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.dto.responses.FileLocationResponse;
import org.apache.gravitino.rest.RESTUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hc.core5.http.Method;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
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
  protected static final Path localCatalogPath =
      FileSystemTestUtils.createLocalRootDir(catalogName);

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
    try (FileSystem localFileSystem = localCatalogPath.getFileSystem(conf)) {
      if (localFileSystem.exists(localCatalogPath)) {
        localFileSystem.delete(localCatalogPath, true);
      }
    } catch (IOException e) {
      // ignore
    }
  }

  @BeforeEach
  public void init() {
    mockMetalakeDTO(metalakeName, "comment");
    mockCatalogDTO(catalogName, provider, "comment");
  }

  @Test
  public void testFSCache() throws IOException {
    String filesetName = "testFSCache";
    Path managedFilesetPath =
        FileSystemTestUtils.createFilesetPath(catalogName, schemaName, filesetName, true);
    Path localPath = FileSystemTestUtils.createLocalDirPrefix(catalogName, schemaName, filesetName);
    String locationPath =
        String.format(
            "/api/metalakes/%s/catalogs/%s/schemas/%s/filesets/%s/location",
            metalakeName, catalogName, schemaName, filesetName);
    try (FileSystem gravitinoFileSystem = managedFilesetPath.getFileSystem(conf);
        FileSystem localFileSystem = localPath.getFileSystem(conf)) {

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
      Path newGvfsPath =
          FileSystemTestUtils.createFilesetPath(catalogName, schemaName, "new_fileset", true);
      try (FileSystem anotherFS = newGvfsPath.getFileSystem(conf)) {
        assertNotEquals(anotherFS, gravitinoFileSystem);
      }

      // test proxied local fs, should not get the same fs
      FileLocationResponse fileLocationResponse = new FileLocationResponse(localPath.toString());
      Map<String, String> queryParams = new HashMap<>();
      queryParams.put("sub_path", RESTUtils.encodeString(""));
      try {
        buildMockResource(Method.GET, locationPath, queryParams, null, fileLocationResponse, SC_OK);
      } catch (JsonProcessingException e) {
        throw new RuntimeException(e);
      }

      FileSystemTestUtils.mkdirs(managedFilesetPath, gravitinoFileSystem);
      FileSystem proxyLocalFs =
          Objects.requireNonNull(
              ((GravitinoVirtualFileSystem) gravitinoFileSystem)
                  .internalFileSystemCache()
                  .getIfPresent("file"));

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
    Path localPath1 = FileSystemTestUtils.createLocalDirPrefix(catalogName, schemaName, "fileset1");
    Path filesetPath1 =
        FileSystemTestUtils.createFilesetPath(catalogName, schemaName, "fileset1", true);
    String locationPath1 =
        String.format(
            "/api/metalakes/%s/catalogs/%s/schemas/%s/filesets/%s/location",
            metalakeName, catalogName, schemaName, "fileset1");
    Configuration configuration1 = new Configuration(conf);
    configuration1.set(
        GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_FILESET_CACHE_MAX_CAPACITY_KEY, "1");
    configuration1.set(
        GravitinoVirtualFileSystemConfiguration
            .FS_GRAVITINO_FILESET_CACHE_EVICTION_MILLS_AFTER_ACCESS_KEY,
        "1000");
    try (FileSystem fs = filesetPath1.getFileSystem(configuration1)) {
      FileLocationResponse fileLocationResponse = new FileLocationResponse(localPath1.toString());
      Map<String, String> queryParams = new HashMap<>();
      queryParams.put("sub_path", RESTUtils.encodeString(""));
      try {
        buildMockResource(
            Method.GET, locationPath1, queryParams, null, fileLocationResponse, SC_OK);
      } catch (JsonProcessingException e) {
        throw new RuntimeException(e);
      }
      FileSystemTestUtils.mkdirs(filesetPath1, fs);

      // expired by time
      Awaitility.await()
          .atMost(5, TimeUnit.SECONDS)
          .pollInterval(1, TimeUnit.SECONDS)
          .untilAsserted(
              () ->
                  assertEquals(
                      0,
                      ((GravitinoVirtualFileSystem) fs).internalFileSystemCache().asMap().size()));

      assertNull(((GravitinoVirtualFileSystem) fs).internalFileSystemCache().getIfPresent("file"));
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testCreate(boolean withScheme) throws IOException {
    String filesetName = "testCreate";
    Path managedFilesetPath =
        FileSystemTestUtils.createFilesetPath(catalogName, schemaName, filesetName, true);
    Path localPath = FileSystemTestUtils.createLocalDirPrefix(catalogName, schemaName, filesetName);
    String locationPath =
        String.format(
            "/api/metalakes/%s/catalogs/%s/schemas/%s/filesets/%s/location",
            metalakeName, catalogName, schemaName, filesetName);
    try (FileSystem gravitinoFileSystem = managedFilesetPath.getFileSystem(conf);
        FileSystem localFileSystem = localPath.getFileSystem(conf)) {
      FileSystemTestUtils.mkdirs(localPath, localFileSystem);
      assertTrue(localFileSystem.exists(localPath));
      // test gvfs normal create
      FileLocationResponse fileLocationResponse = new FileLocationResponse(localPath + "/test.txt");
      Map<String, String> queryParams = new HashMap<>();
      queryParams.put("sub_path", RESTUtils.encodeString("/test.txt"));
      try {
        buildMockResource(Method.GET, locationPath, queryParams, null, fileLocationResponse, SC_OK);
      } catch (JsonProcessingException e) {
        throw new RuntimeException(e);
      }

      Path localFilePath = new Path(localPath + "/test.txt");
      assertFalse(localFileSystem.exists(localFilePath));
      Path filePath = new Path(managedFilesetPath + "/test.txt");
      FileSystemTestUtils.create(filePath, gravitinoFileSystem);
      assertTrue(localFileSystem.exists(localFilePath));
      localFileSystem.delete(localFilePath, true);

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
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  @Disabled("Append operation is not supported in LocalFileSystem. We can't test it now.")
  public void testAppend(boolean withScheme) throws IOException {
    String filesetName = "testAppend";
    Path managedFilesetPath =
        FileSystemTestUtils.createFilesetPath(catalogName, schemaName, filesetName, true);
    Path localPath = FileSystemTestUtils.createLocalDirPrefix(catalogName, schemaName, filesetName);
    String locationPath =
        String.format(
            "/api/metalakes/%s/catalogs/%s/schemas/%s/filesets/%s/location",
            metalakeName, catalogName, schemaName, filesetName);
    try (FileSystem gravitinoFileSystem = managedFilesetPath.getFileSystem(conf);
        FileSystem localFileSystem = localPath.getFileSystem(conf)) {
      FileSystemTestUtils.mkdirs(localPath, localFileSystem);
      assertTrue(localFileSystem.exists(localPath));

      // test managed fileset append
      FileLocationResponse fileLocationResponse = new FileLocationResponse(localPath + "/test.txt");
      Map<String, String> queryParams = new HashMap<>();
      queryParams.put("sub_path", RESTUtils.encodeString("/test.txt"));
      try {
        buildMockResource(Method.GET, locationPath, queryParams, null, fileLocationResponse, SC_OK);
      } catch (JsonProcessingException e) {
        throw new RuntimeException(e);
      }

      Path appendFile = new Path(managedFilesetPath + "/test.txt");
      Path localAppendFile = new Path(localPath + "/test.txt");
      FileSystemTestUtils.create(localAppendFile, localFileSystem);
      assertTrue(localFileSystem.exists(localAppendFile));
      FileSystemTestUtils.append(appendFile, gravitinoFileSystem);
      assertEquals(
          "Hello, World!",
          new String(
              FileSystemTestUtils.read(localAppendFile, localFileSystem), StandardCharsets.UTF_8));
      localFileSystem.delete(localAppendFile, true);

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
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testRename(boolean withScheme) throws IOException {
    String filesetName = "testRename";
    Path managedFilesetPath =
        FileSystemTestUtils.createFilesetPath(catalogName, schemaName, filesetName, true);
    Path localPath = FileSystemTestUtils.createLocalDirPrefix(catalogName, schemaName, filesetName);
    String locationPath =
        String.format(
            "/api/metalakes/%s/catalogs/%s/schemas/%s/filesets/%s/location",
            metalakeName, catalogName, schemaName, filesetName);
    try (FileSystem gravitinoFileSystem = managedFilesetPath.getFileSystem(conf);
        FileSystem localFileSystem = localPath.getFileSystem(conf)) {
      FileSystemTestUtils.mkdirs(localPath, localFileSystem);
      assertTrue(localFileSystem.exists(localPath));

      // test managed fileset rename
      FileLocationResponse fileLocationResponse =
          new FileLocationResponse(localPath + "/rename_src");
      Map<String, String> queryParams = new HashMap<>();
      queryParams.put("sub_path", RESTUtils.encodeString("/rename_src"));
      try {
        buildMockResource(Method.GET, locationPath, queryParams, null, fileLocationResponse, SC_OK);
      } catch (JsonProcessingException e) {
        throw new RuntimeException(e);
      }

      FileLocationResponse fileLocationResponse1 =
          new FileLocationResponse(localPath + "/rename_dst2");
      Map<String, String> queryParams1 = new HashMap<>();
      queryParams1.put("sub_path", RESTUtils.encodeString("/rename_dst2"));
      try {
        buildMockResource(
            Method.GET, locationPath, queryParams1, null, fileLocationResponse1, SC_OK);
      } catch (JsonProcessingException e) {
        throw new RuntimeException(e);
      }

      Path srcLocalRenamePath = new Path(localPath + "/rename_src");
      localFileSystem.mkdirs(srcLocalRenamePath);
      assertTrue(localFileSystem.getFileStatus(srcLocalRenamePath).isDirectory());
      assertTrue(localFileSystem.exists(srcLocalRenamePath));

      // cannot rename the identifier
      Path srcFilesetRenamePath = new Path(managedFilesetPath + "/rename_src");
      Path dstRenamePath1 =
          FileSystemTestUtils.createFilesetPath(catalogName, schemaName, "rename_dst1", withScheme);
      assertThrows(
          RuntimeException.class,
          () -> gravitinoFileSystem.rename(srcFilesetRenamePath, dstRenamePath1));

      Path dstFilesetRenamePath2 = new Path(managedFilesetPath + "/rename_dst2");
      Path dstLocalRenamePath2 = new Path(localPath + "/rename_dst2");
      gravitinoFileSystem.rename(srcFilesetRenamePath, dstFilesetRenamePath2);
      assertFalse(localFileSystem.exists(srcLocalRenamePath));
      assertTrue(localFileSystem.exists(dstLocalRenamePath2));
      localFileSystem.delete(dstLocalRenamePath2, true);

      // test invalid src path
      Path invalidSrcPath =
          FileSystemTestUtils.createFilesetPath(
              catalogName, schemaName, "invalid_src_name", withScheme);
      Path validDstPath =
          FileSystemTestUtils.createFilesetPath(catalogName, schemaName, filesetName, withScheme);
      assertThrows(
          RuntimeException.class, () -> gravitinoFileSystem.rename(invalidSrcPath, validDstPath));

      // test invalid dst path
      Path invalidDstPath =
          FileSystemTestUtils.createFilesetPath(
              catalogName, schemaName, "invalid_dst_name", withScheme);
      assertThrows(
          RuntimeException.class,
          () -> gravitinoFileSystem.rename(managedFilesetPath, invalidDstPath));
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testDelete(boolean withScheme) throws IOException {
    String filesetName = "testDelete";
    Path managedFilesetPath =
        FileSystemTestUtils.createFilesetPath(catalogName, schemaName, filesetName, true);
    Path localPath = FileSystemTestUtils.createLocalDirPrefix(catalogName, schemaName, filesetName);
    String locationPath =
        String.format(
            "/api/metalakes/%s/catalogs/%s/schemas/%s/filesets/%s/location",
            metalakeName, catalogName, schemaName, filesetName);
    try (FileSystem gravitinoFileSystem = managedFilesetPath.getFileSystem(conf);
        FileSystem localFileSystem = localPath.getFileSystem(conf)) {
      FileSystemTestUtils.mkdirs(localPath, localFileSystem);
      assertTrue(localFileSystem.exists(localPath));

      // test managed fileset delete
      FileLocationResponse fileLocationResponse =
          new FileLocationResponse(localPath + "/test_delete");
      Map<String, String> queryParams = new HashMap<>();
      queryParams.put("sub_path", RESTUtils.encodeString("/test_delete"));
      try {
        buildMockResource(Method.GET, locationPath, queryParams, null, fileLocationResponse, SC_OK);
      } catch (JsonProcessingException e) {
        throw new RuntimeException(e);
      }

      Path dirPath = new Path(managedFilesetPath + "/test_delete");
      Path localDirPath = new Path(localPath + "/test_delete");
      localFileSystem.mkdirs(localDirPath);
      assertTrue(localFileSystem.exists(localDirPath));
      gravitinoFileSystem.delete(dirPath, true);
      assertFalse(localFileSystem.exists(localDirPath));

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
    }
  }

  @Test
  public void testGetStatus() throws IOException {
    String filesetName = "testGetStatus";
    Path managedFilesetPath =
        FileSystemTestUtils.createFilesetPath(catalogName, schemaName, filesetName, true);
    Path localPath = FileSystemTestUtils.createLocalDirPrefix(catalogName, schemaName, filesetName);
    String locationPath =
        String.format(
            "/api/metalakes/%s/catalogs/%s/schemas/%s/filesets/%s/location",
            metalakeName, catalogName, schemaName, filesetName);
    try (FileSystem gravitinoFileSystem = managedFilesetPath.getFileSystem(conf);
        FileSystem localFileSystem = localPath.getFileSystem(conf)) {
      FileSystemTestUtils.mkdirs(localPath, localFileSystem);
      assertTrue(localFileSystem.exists(localPath));

      FileLocationResponse fileLocationResponse = new FileLocationResponse(localPath.toString());
      Map<String, String> queryParams = new HashMap<>();
      queryParams.put("sub_path", RESTUtils.encodeString(""));
      try {
        buildMockResource(Method.GET, locationPath, queryParams, null, fileLocationResponse, SC_OK);
      } catch (JsonProcessingException e) {
        throw new RuntimeException(e);
      }

      FileStatus gravitinoStatus = gravitinoFileSystem.getFileStatus(managedFilesetPath);
      FileStatus localStatus = localFileSystem.getFileStatus(localPath);
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
    String filesetName = "testListStatus";
    Path managedFilesetPath =
        FileSystemTestUtils.createFilesetPath(catalogName, schemaName, filesetName, true);
    Path localPath = FileSystemTestUtils.createLocalDirPrefix(catalogName, schemaName, filesetName);
    String locationPath =
        String.format(
            "/api/metalakes/%s/catalogs/%s/schemas/%s/filesets/%s/location",
            metalakeName, catalogName, schemaName, filesetName);
    try (FileSystem gravitinoFileSystem = managedFilesetPath.getFileSystem(conf);
        FileSystem localFileSystem = localPath.getFileSystem(conf)) {
      FileSystemTestUtils.mkdirs(localPath, localFileSystem);
      assertTrue(localFileSystem.exists(localPath));

      for (int i = 0; i < 5; i++) {
        Path subLocalPath = new Path(localPath + "/sub" + i);
        FileSystemTestUtils.mkdirs(subLocalPath, localFileSystem);
        assertTrue(localFileSystem.exists(subLocalPath));
        assertTrue(localFileSystem.getFileStatus(subLocalPath).isDirectory());
      }

      FileLocationResponse fileLocationResponse = new FileLocationResponse(localPath.toString());
      Map<String, String> queryParams = new HashMap<>();
      queryParams.put("sub_path", RESTUtils.encodeString(""));
      try {
        buildMockResource(Method.GET, locationPath, queryParams, null, fileLocationResponse, SC_OK);
      } catch (JsonProcessingException e) {
        throw new RuntimeException(e);
      }

      List<FileStatus> gravitinoStatuses =
          new ArrayList<>(Arrays.asList(gravitinoFileSystem.listStatus(managedFilesetPath)));
      gravitinoStatuses.sort(Comparator.comparing(FileStatus::getPath));
      assertEquals(5, gravitinoStatuses.size());

      List<FileStatus> localStatuses =
          new ArrayList<>(Arrays.asList(localFileSystem.listStatus(localPath)));
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
      }
    }
  }

  @Test
  public void testMkdirs() throws IOException {
    String filesetName = "testMkdirs";
    Path managedFilesetPath =
        FileSystemTestUtils.createFilesetPath(catalogName, schemaName, filesetName, true);
    Path localPath = FileSystemTestUtils.createLocalDirPrefix(catalogName, schemaName, filesetName);
    String locationPath =
        String.format(
            "/api/metalakes/%s/catalogs/%s/schemas/%s/filesets/%s/location",
            metalakeName, catalogName, schemaName, filesetName);
    try (FileSystem gravitinoFileSystem = managedFilesetPath.getFileSystem(conf);
        FileSystem localFileSystem = localPath.getFileSystem(conf)) {
      FileSystemTestUtils.mkdirs(localPath, localFileSystem);
      assertTrue(localFileSystem.exists(localPath));
      assertTrue(localFileSystem.getFileStatus(localPath).isDirectory());

      FileLocationResponse fileLocationResponse =
          new FileLocationResponse(localPath + "/test_mkdirs");
      Map<String, String> queryParams = new HashMap<>();
      queryParams.put("sub_path", RESTUtils.encodeString("/test_mkdirs"));
      try {
        buildMockResource(Method.GET, locationPath, queryParams, null, fileLocationResponse, SC_OK);
      } catch (JsonProcessingException e) {
        throw new RuntimeException(e);
      }

      Path subDirPath = new Path(managedFilesetPath + "/test_mkdirs");
      Path localDirPath = new Path(localPath + "/test_mkdirs");
      FileSystemTestUtils.mkdirs(subDirPath, gravitinoFileSystem);
      assertTrue(localFileSystem.exists(localDirPath));
      assertTrue(localFileSystem.getFileStatus(localDirPath).isDirectory());

      FileStatus localStatus = localFileSystem.getFileStatus(localDirPath);

      FileStatus gravitinoStatus = gravitinoFileSystem.getFileStatus(subDirPath);

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
    String filesetName = "testExtractIdentifier";
    Path managedFilesetPath =
        FileSystemTestUtils.createFilesetPath(catalogName, schemaName, filesetName, true);
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
    String filesetName = "testGetDefaultReplications";
    Path managedFilesetPath =
        FileSystemTestUtils.createFilesetPath(catalogName, schemaName, filesetName, true);
    Path localPath = FileSystemTestUtils.createLocalDirPrefix(catalogName, schemaName, filesetName);
    String locationPath =
        String.format(
            "/api/metalakes/%s/catalogs/%s/schemas/%s/filesets/%s/location",
            metalakeName, catalogName, schemaName, filesetName);
    try (GravitinoVirtualFileSystem fs =
        (GravitinoVirtualFileSystem) managedFilesetPath.getFileSystem(conf)) {

      FileLocationResponse fileLocationResponse = new FileLocationResponse(localPath.toString());
      Map<String, String> queryParams = new HashMap<>();
      queryParams.put("sub_path", RESTUtils.encodeString(""));
      try {
        buildMockResource(Method.GET, locationPath, queryParams, null, fileLocationResponse, SC_OK);
      } catch (JsonProcessingException e) {
        throw new RuntimeException(e);
      }

      assertEquals(1, fs.getDefaultReplication(managedFilesetPath));
    }
  }

  @Test
  public void testGetDefaultBlockSize() throws IOException {
    String filesetName = "testGetDefaultBlockSize";
    Path managedFilesetPath =
        FileSystemTestUtils.createFilesetPath(catalogName, schemaName, filesetName, true);
    Path localPath = FileSystemTestUtils.createLocalDirPrefix(catalogName, schemaName, filesetName);
    String locationPath =
        String.format(
            "/api/metalakes/%s/catalogs/%s/schemas/%s/filesets/%s/location",
            metalakeName, catalogName, schemaName, filesetName);
    try (GravitinoVirtualFileSystem fs =
        (GravitinoVirtualFileSystem) managedFilesetPath.getFileSystem(conf)) {

      FileLocationResponse fileLocationResponse = new FileLocationResponse(localPath.toString());
      Map<String, String> queryParams = new HashMap<>();
      queryParams.put("sub_path", RESTUtils.encodeString(""));
      try {
        buildMockResource(Method.GET, locationPath, queryParams, null, fileLocationResponse, SC_OK);
      } catch (JsonProcessingException e) {
        throw new RuntimeException(e);
      }

      assertEquals(32 * 1024 * 1024, fs.getDefaultBlockSize(managedFilesetPath));
    }
  }

  @Test
  public void testConvertFileStatusPathPrefix() throws IOException {
    String filesetName = "testConvertFileStatusPathPrefix";
    Path managedFilesetPath =
        FileSystemTestUtils.createFilesetPath(catalogName, schemaName, filesetName, true);
    try (GravitinoVirtualFileSystem fs =
        (GravitinoVirtualFileSystem) managedFilesetPath.getFileSystem(conf)) {
      FileStatus fileStatus =
          new FileStatus(1024, false, 1, 32 * 1024 * 1024, 1024, new Path("hdfs://hive:9000/test"));
      // storage location end with "/"
      String storageLocation = "hdfs://hive:9000/";
      String virtualLocation = "gvfs://fileset/test_catalog/tmp/test_fileset";
      FileStatus convertedStatus =
          fs.convertFileStatusPathPrefix(fileStatus, storageLocation, virtualLocation);
      Path expectedPath = new Path("gvfs://fileset/test_catalog/tmp/test_fileset/test");
      assertEquals(expectedPath, convertedStatus.getPath());
    }
  }
}
