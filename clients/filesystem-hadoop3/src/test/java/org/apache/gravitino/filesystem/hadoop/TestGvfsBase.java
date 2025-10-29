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

import static org.apache.gravitino.client.GravitinoClientConfiguration.CLIENT_CONNECTION_TIMEOUT_MS;
import static org.apache.gravitino.client.GravitinoClientConfiguration.CLIENT_SOCKET_TIMEOUT_MS;
import static org.apache.gravitino.file.Fileset.LOCATION_NAME_UNKNOWN;
import static org.apache.gravitino.file.Fileset.PROPERTY_DEFAULT_LOCATION_NAME;
import static org.apache.gravitino.filesystem.hadoop.GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_BLOCK_SIZE_DEFAULT;
import static org.apache.gravitino.filesystem.hadoop.GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_CONFIG_PREFIX;
import static org.apache.gravitino.filesystem.hadoop.GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_REQUEST_HEADER_PREFIX;
import static org.apache.gravitino.filesystem.hadoop.GravitinoVirtualFileSystemUtils.extractIdentifier;
import static org.apache.gravitino.filesystem.hadoop.GravitinoVirtualFileSystemUtils.getConfigMap;
import static org.apache.hc.core5.http.HttpStatus.SC_NOT_FOUND;
import static org.apache.hc.core5.http.HttpStatus.SC_OK;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyShort;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.github.benmanes.caffeine.cache.Cache;
import com.google.common.collect.ImmutableMap;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.SocketTimeoutException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Version;
import org.apache.gravitino.dto.AuditDTO;
import org.apache.gravitino.dto.CatalogDTO;
import org.apache.gravitino.dto.credential.CredentialDTO;
import org.apache.gravitino.dto.file.FilesetDTO;
import org.apache.gravitino.dto.responses.CatalogResponse;
import org.apache.gravitino.dto.responses.CredentialResponse;
import org.apache.gravitino.dto.responses.ErrorResponse;
import org.apache.gravitino.dto.responses.FileLocationResponse;
import org.apache.gravitino.dto.responses.FilesetResponse;
import org.apache.gravitino.dto.responses.VersionResponse;
import org.apache.gravitino.exceptions.NoSuchCatalogException;
import org.apache.gravitino.exceptions.NoSuchFilesetException;
import org.apache.gravitino.exceptions.NoSuchLocationNameException;
import org.apache.gravitino.exceptions.RESTException;
import org.apache.gravitino.file.Fileset;
import org.apache.gravitino.rest.RESTUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hc.core5.http.Method;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;
import org.mockserver.matchers.Times;
import org.mockserver.model.HttpRequest;
import org.mockserver.verify.VerificationTimes;

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
    conf.set(
        GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_HOOK_CLASS,
        MockGVFSHook.class.getCanonicalName());
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
  public void testOpsException() throws IOException, NoSuchFieldException, IllegalAccessException {
    Assumptions.assumeTrue(getClass() == TestGvfsBase.class);
    Configuration newConf = new Configuration(conf);
    newConf.set(
        GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_HOOK_CLASS,
        NoOpHook.class.getCanonicalName());
    try (GravitinoVirtualFileSystem fs =
        (GravitinoVirtualFileSystem) new Path("gvfs://fileset/").getFileSystem(newConf)) {
      BaseGVFSOperations mockOps = Mockito.mock(BaseGVFSOperations.class);
      // inject the mockOps
      Field operationsField = GravitinoVirtualFileSystem.class.getDeclaredField("operations");
      operationsField.setAccessible(true);
      operationsField.set(fs, mockOps);

      // test setWorkingDirectory
      Mockito.doThrow(new NoSuchFilesetException("fileset not found"))
          .when(mockOps)
          .setWorkingDirectory(any());
      assertThrows(
          RuntimeException.class, () -> fs.setWorkingDirectory(new Path("gvfs://fileset/")));

      // test open
      Mockito.doThrow(new NoSuchFilesetException("fileset not found"))
          .when(mockOps)
          .open(any(), anyInt());
      assertThrows(FileNotFoundException.class, () -> fs.open(new Path("gvfs://fileset/"), 1024));

      // test create
      Mockito.doThrow(new NoSuchCatalogException("fileset catalog not found"))
          .when(mockOps)
          .create(any(), any(), anyBoolean(), anyInt(), anyShort(), anyLong(), any());
      Exception exception =
          assertThrows(IOException.class, () -> fs.create(new Path("gvfs://fileset/"), true));
      assertTrue(
          exception.getMessage().contains("please check the fileset metadata in Gravitino"),
          "The expected message is: " + exception.getMessage());

      // test append
      Mockito.doThrow(new NoSuchLocationNameException("location name not found"))
          .when(mockOps)
          .append(any(), anyInt(), any());
      assertThrows(FileNotFoundException.class, () -> fs.append(new Path("gvfs://fileset/"), 1024));

      // test rename
      Mockito.doThrow(new NoSuchFilesetException("fileset not found"))
          .when(mockOps)
          .rename(any(), any());
      assertThrows(
          FileNotFoundException.class,
          () -> fs.rename(new Path("gvfs://fileset/"), new Path("gvfs://fileset/new")));

      // test delete
      Mockito.doThrow(new NoSuchFilesetException("fileset not found"))
          .when(mockOps)
          .delete(any(), anyBoolean());
      assertEquals(false, fs.delete(new Path("gvfs://fileset/"), true));

      // test getFileStatus
      Mockito.doThrow(new NoSuchFilesetException("fileset not found"))
          .when(mockOps)
          .getFileStatus(any());
      assertThrows(
          FileNotFoundException.class, () -> fs.getFileStatus(new Path("gvfs://fileset/")));

      // test listStatus
      Mockito.doThrow(new NoSuchFilesetException("fileset not found"))
          .when(mockOps)
          .listStatus(any());
      assertThrows(FileNotFoundException.class, () -> fs.listStatus(new Path("gvfs://fileset/")));

      // test listStatus
      Mockito.doThrow(new NoSuchFilesetException("fileset not found"))
          .when(mockOps)
          .listStatus(any());
      assertThrows(FileNotFoundException.class, () -> fs.listStatus(new Path("gvfs://fileset/")));

      // test mkdirs
      Mockito.doThrow(new NoSuchFilesetException("fileset not found"))
          .when(mockOps)
          .mkdirs(any(), any());
      exception = assertThrows(IOException.class, () -> fs.mkdirs(new Path("gvfs://fileset/")));
      assertTrue(
          exception.getMessage().contains("please check the fileset metadata in Gravitino"),
          "The expected message is: " + exception.getMessage());

      // test getDefaultReplication
      Mockito.doThrow(new NoSuchFilesetException("fileset not found"))
          .when(mockOps)
          .getDefaultReplication(any());
      assertEquals(1, fs.getDefaultReplication(new Path("gvfs://fileset/")));

      // test getDefaultBlockSize
      Mockito.doThrow(new NoSuchFilesetException("fileset not found"))
          .when(mockOps)
          .getDefaultBlockSize(any());
      Mockito.doReturn(FS_GRAVITINO_BLOCK_SIZE_DEFAULT).when(mockOps).defaultBlockSize();
      assertEquals(
          FS_GRAVITINO_BLOCK_SIZE_DEFAULT, fs.getDefaultBlockSize(new Path("gvfs://fileset/")));

      // test addDelegationTokens
      Mockito.doThrow(new NoSuchFilesetException("fileset not found"))
          .when(mockOps)
          .addDelegationTokens(any(), any());
      assertThrows(NoSuchFilesetException.class, () -> fs.addDelegationTokens("renewer", null));
    }
  }

  @Test
  public void testRequestHeaders()
      throws NoSuchFieldException, IllegalAccessException, IOException {
    String envKey = "GRAVITINO_TEST_HEADER";
    String envValue = "v1";
    String headerKey1 = "k1";
    // prepare the env variable
    Map<String, String> env = System.getenv();
    Field field = env.getClass().getDeclaredField("m");
    field.setAccessible(true);
    Map<String, String> writableEnv = (Map<String, String>) field.get(env);
    writableEnv.put(envKey, envValue);

    // test the request headers
    String headerKey2 = "k2";
    String headerValue2 = "v2";
    Configuration configuration = new Configuration(conf);
    configuration.set(
        FS_GRAVITINO_CLIENT_REQUEST_HEADER_PREFIX + headerKey1, "${env." + envKey + "}");
    configuration.set(FS_GRAVITINO_CLIENT_REQUEST_HEADER_PREFIX + headerKey2, headerValue2);

    mockServer().clear(request().withPath("/api/version"));
    HttpRequest req =
        request()
            .withHeader(headerKey1, envValue)
            .withHeader(headerKey2, headerValue2)
            .withPath("/api/version");
    mockServer()
        .when(req, Times.once())
        .respond(
            response()
                .withStatusCode(SC_OK)
                .withBody(getJsonString(new VersionResponse(Version.getCurrentVersionDTO()))));

    try (FileSystem fs = new Path("gvfs://fileset/").getFileSystem(configuration)) {
      // Trigger lazy initialization by accessing a path (throws RESTException for mock server 404)
      assertThrows(
          RESTException.class,
          () -> fs.exists(new Path("gvfs://fileset/catalog/schema/fileset/file.txt")));
      // Verify the request was made with correct headers during client initialization
      mockServer().verify(req, VerificationTimes.once());
    }
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
      queryParams.put("sub_path", "");
      buildMockResource(Method.GET, locationPath, queryParams, null, fileLocationResponse, SC_OK);
      buildMockResourceForCredential(filesetName, localPath.toString());

      FileSystemTestUtils.mkdirs(managedFilesetPath, gravitinoFileSystem);

      // Verify the internal cache contains a FileSystem for the local scheme
      Cache<BaseGVFSOperations.FileSystemCacheKey, FileSystem> cache =
          ((GravitinoVirtualFileSystem) gravitinoFileSystem)
              .getOperations()
              .internalFileSystemCache();

      // The cache should have one entry for the local filesystem
      assertEquals(1, cache.asMap().size());

      // Get the cached filesystem (should be a local filesystem)
      FileSystem proxyLocalFs = cache.asMap().values().iterator().next();
      assertNotNull(proxyLocalFs);

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
      queryParams.put("sub_path", "");
      buildMockResource(Method.GET, locationPath1, queryParams, null, fileLocationResponse, SC_OK);
      buildMockResourceForCredential("fileset1", localPath1.toString());
      FileSystemTestUtils.mkdirs(filesetPath1, fs);

      // expired by time
      Awaitility.await()
          .atMost(5, TimeUnit.SECONDS)
          .pollInterval(1, TimeUnit.SECONDS)
          .untilAsserted(
              () ->
                  assertEquals(
                      0,
                      ((GravitinoVirtualFileSystem) fs)
                          .getOperations()
                          .internalFileSystemCache()
                          .asMap()
                          .size()));

      // Verify the cache is empty after eviction
      assertTrue(
          ((GravitinoVirtualFileSystem) fs)
              .getOperations()
              .internalFileSystemCache()
              .asMap()
              .isEmpty());
    }
  }

  @Test
  public void testCacheReuseAcrossMultipleFilesets() throws IOException {
    // Create two different filesets that point to the same local directory
    String fileset1Name = "fileset_cache_reuse_1";
    String fileset2Name = "fileset_cache_reuse_2";

    // Both filesets point to the same underlying local path
    Path localPath =
        FileSystemTestUtils.createLocalDirPrefix(catalogName, schemaName, "shared_cache");

    Path filesetPath1 =
        FileSystemTestUtils.createFilesetPath(catalogName, schemaName, fileset1Name, true);
    Path filesetPath2 =
        FileSystemTestUtils.createFilesetPath(catalogName, schemaName, fileset2Name, true);

    String locationPath1 =
        String.format(
            "/api/metalakes/%s/catalogs/%s/schemas/%s/filesets/%s/location",
            metalakeName, catalogName, schemaName, fileset1Name);
    String locationPath2 =
        String.format(
            "/api/metalakes/%s/catalogs/%s/schemas/%s/filesets/%s/location",
            metalakeName, catalogName, schemaName, fileset2Name);

    try (FileSystem fs = filesetPath1.getFileSystem(conf)) {
      // Mock server responses for both filesets pointing to the same location
      FileLocationResponse fileLocationResponse = new FileLocationResponse(localPath.toString());
      Map<String, String> queryParams = new HashMap<>();
      queryParams.put("sub_path", "");

      buildMockResource(Method.GET, locationPath1, queryParams, null, fileLocationResponse, SC_OK);
      buildMockResource(Method.GET, locationPath2, queryParams, null, fileLocationResponse, SC_OK);
      buildMockResourceForCredential(fileset1Name, localPath.toString());
      buildMockResourceForCredential(fileset2Name, localPath.toString());

      // Access first fileset
      FileSystemTestUtils.mkdirs(filesetPath1, fs);

      Cache<BaseGVFSOperations.FileSystemCacheKey, FileSystem> cache =
          ((GravitinoVirtualFileSystem) fs).getOperations().internalFileSystemCache();

      // Should have one cached filesystem for the local scheme
      assertEquals(1, cache.asMap().size());
      FileSystem cachedFs1 = cache.asMap().values().iterator().next();
      assertNotNull(cachedFs1);

      // Access second fileset pointing to the same location
      FileSystemTestUtils.mkdirs(filesetPath2, fs);

      // Should still have only one cached filesystem (reused)
      assertEquals(1, cache.asMap().size());
      FileSystem cachedFs2 = cache.asMap().values().iterator().next();

      // Both should be the same instance since they share scheme/authority/user
      assertSame(cachedFs1, cachedFs2, "FileSystem instances should be reused for same storage");
    }
  }

  @ParameterizedTest
  @CsvSource({
    "true, testCreate",
    "false, testCreate",
    "true, testCreate%2Fabc",
    "false, testCreate%2Fabc"
  })
  public void testCreate(boolean withScheme, String filesetName) throws IOException {
    Path managedFilesetPath =
        FileSystemTestUtils.createFilesetPath(catalogName, schemaName, filesetName, true);
    Path localPath = FileSystemTestUtils.createLocalDirPrefix(catalogName, schemaName, filesetName);
    String locationPath =
        String.format(
            "/api/metalakes/%s/catalogs/%s/schemas/%s/filesets/%s/location",
            metalakeName, catalogName, schemaName, RESTUtils.encodeString(filesetName));
    try (FileSystem gravitinoFileSystem = managedFilesetPath.getFileSystem(conf);
        FileSystem localFileSystem = localPath.getFileSystem(conf)) {
      FileSystemTestUtils.mkdirs(localPath, localFileSystem);
      assertTrue(localFileSystem.exists(localPath));
      // test gvfs normal create
      mockFilesetDTO(
          metalakeName,
          catalogName,
          schemaName,
          filesetName,
          Fileset.Type.MANAGED,
          ImmutableMap.of("location1", localPath.toString()),
          ImmutableMap.of(PROPERTY_DEFAULT_LOCATION_NAME, "location1"));
      FileLocationResponse fileLocationResponse = new FileLocationResponse(localPath + "/test.txt");
      Map<String, String> queryParams = new HashMap<>();
      queryParams.put("sub_path", "/test.txt");
      buildMockResource(Method.GET, locationPath, queryParams, null, fileLocationResponse, SC_OK);
      buildMockResourceForCredential(filesetName, localPath + "/test.txt");

      Path localFilePath = new Path(localPath + "/test.txt");
      assertFalse(localFileSystem.exists(localFilePath));
      Path filePath = new Path(managedFilesetPath + "/test.txt");
      FileSystemTestUtils.create(filePath, gravitinoFileSystem);
      assertTrue(localFileSystem.exists(localFilePath));
      localFileSystem.delete(localFilePath, true);
      // test gvfs preCreate and postCreate are called
      assertTrue(getHook(gravitinoFileSystem).preCreateCalled);
      assertTrue(getHook(gravitinoFileSystem).postCreateCalled);

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
  @CsvSource({
    "true, testAppend",
    "false, testAppend",
    "true, testAppend%2Fabc",
    "false, testAppend%2Fabc"
  })
  @Disabled("Append operation is not supported in LocalFileSystem. We can't test it now.")
  public void testAppend(boolean withScheme, String filesetName) throws IOException {
    Path managedFilesetPath =
        FileSystemTestUtils.createFilesetPath(catalogName, schemaName, filesetName, true);
    Path localPath = FileSystemTestUtils.createLocalDirPrefix(catalogName, schemaName, filesetName);
    String locationPath =
        String.format(
            "/api/metalakes/%s/catalogs/%s/schemas/%s/filesets/%s/location",
            metalakeName, catalogName, schemaName, RESTUtils.encodeString(filesetName));
    try (FileSystem gravitinoFileSystem = managedFilesetPath.getFileSystem(conf);
        FileSystem localFileSystem = localPath.getFileSystem(conf)) {
      FileSystemTestUtils.mkdirs(localPath, localFileSystem);
      assertTrue(localFileSystem.exists(localPath));

      // test managed fileset append
      FileLocationResponse fileLocationResponse = new FileLocationResponse(localPath + "/test.txt");
      Map<String, String> queryParams = new HashMap<>();
      queryParams.put("sub_path", "/test.txt");
      buildMockResource(Method.GET, locationPath, queryParams, null, fileLocationResponse, SC_OK);
      buildMockResourceForCredential(filesetName, localPath + "/test.txt");

      Path appendFile = new Path(managedFilesetPath + "/test.txt");
      Path localAppendFile = new Path(localPath + "/test.txt");
      FileSystemTestUtils.create(localAppendFile, localFileSystem);
      assertTrue(localFileSystem.exists(localAppendFile));
      FileSystemTestUtils.append(appendFile, gravitinoFileSystem);

      // test gvfs preAppend and postAppend are called
      assertTrue(getHook(gravitinoFileSystem).preAppendCalled);
      assertTrue(getHook(gravitinoFileSystem).postAppendCalled);

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
  @CsvSource({
    "true, testRename",
    "false, testRename",
    "true, testRename%2Fabc",
    "false, testRename%2Fabc"
  })
  public void testRename(boolean withScheme, String filesetName) throws IOException {
    Path managedFilesetPath =
        FileSystemTestUtils.createFilesetPath(catalogName, schemaName, filesetName, true);
    Path localPath = FileSystemTestUtils.createLocalDirPrefix(catalogName, schemaName, filesetName);
    String locationPath =
        String.format(
            "/api/metalakes/%s/catalogs/%s/schemas/%s/filesets/%s/location",
            metalakeName, catalogName, schemaName, RESTUtils.encodeString(filesetName));
    try (FileSystem gravitinoFileSystem = managedFilesetPath.getFileSystem(conf);
        FileSystem localFileSystem = localPath.getFileSystem(conf)) {
      FileSystemTestUtils.mkdirs(localPath, localFileSystem);
      assertTrue(localFileSystem.exists(localPath));

      // test managed fileset rename
      FileLocationResponse fileLocationResponse =
          new FileLocationResponse(localPath + "/rename_src");
      Map<String, String> queryParams = new HashMap<>();
      queryParams.put("sub_path", "/rename_src");
      buildMockResource(Method.GET, locationPath, queryParams, null, fileLocationResponse, SC_OK);

      FileLocationResponse fileLocationResponse1 =
          new FileLocationResponse(localPath + "/rename_dst2");
      Map<String, String> queryParams1 = new HashMap<>();
      queryParams1.put("sub_path", "/rename_dst2");
      buildMockResource(Method.GET, locationPath, queryParams1, null, fileLocationResponse1, SC_OK);
      buildMockResourceForCredential(filesetName, localPath + "/rename_dst2");

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

      // test gvfs preRename and postRename are called
      assertTrue(getHook(gravitinoFileSystem).preRenameCalled);
      assertTrue(getHook(gravitinoFileSystem).postRenameCalled);

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
  @CsvSource({
    "true, testDelete",
    "false, testDelete",
    "true, testDelete%2Fabc",
    "false, testDelete%2Fabc"
  })
  public void testDelete(boolean withScheme, String filesetName) throws IOException {
    Path managedFilesetPath =
        FileSystemTestUtils.createFilesetPath(catalogName, schemaName, filesetName, true);
    Path localPath = FileSystemTestUtils.createLocalDirPrefix(catalogName, schemaName, filesetName);
    String locationPath =
        String.format(
            "/api/metalakes/%s/catalogs/%s/schemas/%s/filesets/%s/location",
            metalakeName, catalogName, schemaName, RESTUtils.encodeString(filesetName));
    try (FileSystem gravitinoFileSystem = managedFilesetPath.getFileSystem(conf);
        FileSystem localFileSystem = localPath.getFileSystem(conf)) {
      FileSystemTestUtils.mkdirs(localPath, localFileSystem);
      assertTrue(localFileSystem.exists(localPath));

      // test managed fileset delete
      FileLocationResponse fileLocationResponse =
          new FileLocationResponse(localPath + "/test_delete");
      Map<String, String> queryParams = new HashMap<>();
      queryParams.put("sub_path", "/test_delete");
      buildMockResource(Method.GET, locationPath, queryParams, null, fileLocationResponse, SC_OK);
      buildMockResourceForCredential(filesetName, localPath + "/test_delete");

      Path dirPath = new Path(managedFilesetPath + "/test_delete");
      Path localDirPath = new Path(localPath + "/test_delete");
      localFileSystem.mkdirs(localDirPath);
      assertTrue(localFileSystem.exists(localDirPath));
      gravitinoFileSystem.delete(dirPath, true);
      assertFalse(localFileSystem.exists(localDirPath));

      // test gvfs preDelete and postDelete called
      assertTrue(getHook(gravitinoFileSystem).preDeleteCalled);
      assertTrue(getHook(gravitinoFileSystem).postDeleteCalled);

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

  @ParameterizedTest
  @ValueSource(strings = {"testGetFileStatus", "testGetFileStatus%2Fabc"})
  public void testGetStatus(String filesetName) throws IOException {
    Path managedFilesetPath =
        FileSystemTestUtils.createFilesetPath(catalogName, schemaName, filesetName, true);
    Path localPath = FileSystemTestUtils.createLocalDirPrefix(catalogName, schemaName, filesetName);
    String locationPath =
        String.format(
            "/api/metalakes/%s/catalogs/%s/schemas/%s/filesets/%s/location",
            metalakeName, catalogName, schemaName, RESTUtils.encodeString(filesetName));
    try (FileSystem gravitinoFileSystem = managedFilesetPath.getFileSystem(conf);
        FileSystem localFileSystem = localPath.getFileSystem(conf)) {
      FileSystemTestUtils.mkdirs(localPath, localFileSystem);
      assertTrue(localFileSystem.exists(localPath));

      FileLocationResponse fileLocationResponse = new FileLocationResponse(localPath.toString());
      Map<String, String> queryParams = new HashMap<>();
      queryParams.put("sub_path", "");
      buildMockResource(Method.GET, locationPath, queryParams, null, fileLocationResponse, SC_OK);
      buildMockResourceForCredential(filesetName, localPath.toString());

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

      // test gvfs preGetStatus and postGetStatus called
      assertTrue(getHook(gravitinoFileSystem).preGetFileStatusCalled);
      assertTrue(getHook(gravitinoFileSystem).postGetFileStatusCalled);
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"testListStatus", "testListStatus%2Fabc"})
  public void testListStatus(String filesetName) throws IOException {
    Path managedFilesetPath =
        FileSystemTestUtils.createFilesetPath(catalogName, schemaName, filesetName, true);
    Path localPath = FileSystemTestUtils.createLocalDirPrefix(catalogName, schemaName, filesetName);
    String locationPath =
        String.format(
            "/api/metalakes/%s/catalogs/%s/schemas/%s/filesets/%s/location",
            metalakeName, catalogName, schemaName, RESTUtils.encodeString(filesetName));
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
      queryParams.put("sub_path", "");
      buildMockResource(Method.GET, locationPath, queryParams, null, fileLocationResponse, SC_OK);
      buildMockResourceForCredential(filesetName, localPath.toString());

      List<FileStatus> gravitinoStatuses =
          new ArrayList<>(Arrays.asList(gravitinoFileSystem.listStatus(managedFilesetPath)));
      gravitinoStatuses.sort(Comparator.comparing(FileStatus::getPath));
      assertEquals(5, gravitinoStatuses.size());

      // test gvfs preListStatus and postListStatus are called
      assertTrue(getHook(gravitinoFileSystem).preListStatusCalled);
      assertTrue(getHook(gravitinoFileSystem).postListStatusCalled);

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

  @ParameterizedTest
  @ValueSource(strings = {"testMkdirs", "testMkdirs%2Fabc"})
  public void testMkdirs(String filesetName) throws IOException {
    Path managedFilesetPath =
        FileSystemTestUtils.createFilesetPath(catalogName, schemaName, filesetName, true);
    Path localPath = FileSystemTestUtils.createLocalDirPrefix(catalogName, schemaName, filesetName);
    String locationPath =
        String.format(
            "/api/metalakes/%s/catalogs/%s/schemas/%s/filesets/%s/location",
            metalakeName, catalogName, schemaName, RESTUtils.encodeString(filesetName));
    try (FileSystem gravitinoFileSystem = managedFilesetPath.getFileSystem(conf);
        FileSystem localFileSystem = localPath.getFileSystem(conf)) {
      FileSystemTestUtils.mkdirs(localPath, localFileSystem);
      assertTrue(localFileSystem.exists(localPath));
      assertTrue(localFileSystem.getFileStatus(localPath).isDirectory());

      FileLocationResponse fileLocationResponse =
          new FileLocationResponse(localPath + "/test_mkdirs");
      Map<String, String> queryParams = new HashMap<>();
      queryParams.put("sub_path", "/test_mkdirs");
      buildMockResource(Method.GET, locationPath, queryParams, null, fileLocationResponse, SC_OK);
      buildMockResourceForCredential(filesetName, localPath + "/test_mkdirs");

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

      // test gvfs preMkdirs and postMkdirs called
      assertTrue(getHook(gravitinoFileSystem).preMkdirsCalled);
      assertTrue(getHook(gravitinoFileSystem).postMkdirsCalled);
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
          extractIdentifier(metalakeName, "gvfs://fileset/catalog1/schema1/fileset1");
      assertEquals(NameIdentifier.of(metalakeName, "catalog1", "schema1", "fileset1"), identifier);

      NameIdentifier identifier2 =
          extractIdentifier(metalakeName, "gvfs://fileset/catalog1/schema1/fileset1/");
      assertEquals(NameIdentifier.of(metalakeName, "catalog1", "schema1", "fileset1"), identifier2);

      NameIdentifier identifier3 =
          extractIdentifier(metalakeName, "gvfs://fileset/catalog1/schema1/fileset1/files");
      assertEquals(NameIdentifier.of(metalakeName, "catalog1", "schema1", "fileset1"), identifier3);

      NameIdentifier identifier4 =
          extractIdentifier(metalakeName, "gvfs://fileset/catalog1/schema1/fileset1/dir/dir");
      assertEquals(NameIdentifier.of(metalakeName, "catalog1", "schema1", "fileset1"), identifier4);

      NameIdentifier identifier5 =
          extractIdentifier(metalakeName, "gvfs://fileset/catalog1/schema1/fileset1/dir/dir/");
      assertEquals(NameIdentifier.of(metalakeName, "catalog1", "schema1", "fileset1"), identifier5);

      NameIdentifier identifier6 = extractIdentifier(metalakeName, "/catalog1/schema1/fileset1");
      assertEquals(NameIdentifier.of(metalakeName, "catalog1", "schema1", "fileset1"), identifier6);

      NameIdentifier identifier7 = extractIdentifier(metalakeName, "/catalog1/schema1/fileset1/");
      assertEquals(NameIdentifier.of(metalakeName, "catalog1", "schema1", "fileset1"), identifier7);

      NameIdentifier identifier8 =
          extractIdentifier(metalakeName, "/catalog1/schema1/fileset1/dir");
      assertEquals(NameIdentifier.of(metalakeName, "catalog1", "schema1", "fileset1"), identifier8);

      NameIdentifier identifier9 =
          extractIdentifier(metalakeName, "/catalog1/schema1/fileset1/dir/dir/");
      assertEquals(NameIdentifier.of(metalakeName, "catalog1", "schema1", "fileset1"), identifier9);

      NameIdentifier identifier10 =
          extractIdentifier(metalakeName, "/catalog1/schema1/fileset1/dir/dir");
      assertEquals(
          NameIdentifier.of(metalakeName, "catalog1", "schema1", "fileset1"), identifier10);

      StringBuilder longUri = new StringBuilder("gvfs://fileset/catalog1/schema1/fileset1");
      for (int i = 0; i < 1500; i++) {
        longUri.append("/dir");
      }
      NameIdentifier identifier11 = extractIdentifier(metalakeName, longUri.toString());
      assertEquals(
          NameIdentifier.of(metalakeName, "catalog1", "schema1", "fileset1"), identifier11);

      NameIdentifier identifier12 =
          extractIdentifier(metalakeName, longUri.delete(0, 14).toString());
      assertEquals(
          NameIdentifier.of(metalakeName, "catalog1", "schema1", "fileset1"), identifier12);

      NameIdentifier identifier13 =
          extractIdentifier(metalakeName, "gvfs://fileset/catalog1/schema1/abc%2Fdef%2Fghi");
      assertEquals(
          NameIdentifier.of(metalakeName, "catalog1", "schema1", "abc%2Fdef%2Fghi"), identifier13);

      assertThrows(
          IllegalArgumentException.class,
          () -> extractIdentifier(metalakeName, "gvfs://fileset/catalog1/"));
      assertThrows(
          IllegalArgumentException.class,
          () -> extractIdentifier(metalakeName, "hdfs://fileset/catalog1/schema1/fileset1"));
      assertThrows(
          IllegalArgumentException.class,
          () -> extractIdentifier(metalakeName, "/catalog1/schema1/"));
      assertThrows(
          IllegalArgumentException.class,
          () -> extractIdentifier(metalakeName, "gvfs://fileset/catalog1/schema1/fileset1//"));
      assertThrows(
          IllegalArgumentException.class,
          () -> extractIdentifier(metalakeName, "/catalog1/schema1/fileset1/dir//"));
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
      queryParams.put("sub_path", "");
      buildMockResource(Method.GET, locationPath, queryParams, null, fileLocationResponse, SC_OK);
      buildMockResourceForCredential(filesetName, localPath.toString());

      assertEquals(1, fs.getDefaultReplication(managedFilesetPath));
      assertTrue(getHook(fs).preGetDefaultReplicationCalled);
      assertTrue(getHook(fs).postGetDefaultReplicationCalled);
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
      queryParams.put("sub_path", "");
      buildMockResource(Method.GET, locationPath, queryParams, null, fileLocationResponse, SC_OK);
      buildMockResourceForCredential(filesetName, localPath.toString());

      assertEquals(32 * 1024 * 1024, fs.getDefaultBlockSize(managedFilesetPath));
      assertTrue(getHook(fs).preGetDefaultBlockSizeCalled);
      assertTrue(getHook(fs).postGetDefaultBlockSizeCalled);
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
          fs.getOperations()
              .convertFileStatusPathPrefix(fileStatus, storageLocation, virtualLocation);
      Path expectedPath = new Path("gvfs://fileset/test_catalog/tmp/test_fileset/test");
      assertEquals(expectedPath, convertedStatus.getPath());
    }
  }

  @Test
  public void testWhenFilesetNotCreated() throws IOException {
    String filesetName = "testWhenFilesetNotCreated";
    Path managedFilesetPath =
        FileSystemTestUtils.createFilesetPath(catalogName, schemaName, filesetName, true);
    Path localPath = FileSystemTestUtils.createLocalDirPrefix(catalogName, schemaName, filesetName);
    String locationPath =
        String.format(
            "/api/metalakes/%s/catalogs/%s/schemas/%s/filesets/%s/location",
            metalakeName, catalogName, schemaName, filesetName);
    try (GravitinoVirtualFileSystem fs =
        (GravitinoVirtualFileSystem) managedFilesetPath.getFileSystem(conf)) {

      Map<String, String> queryParams = new HashMap<>();
      queryParams.put("sub_path", "");
      ErrorResponse errResp =
          ErrorResponse.notFound(NoSuchFilesetException.class.getSimpleName(), "fileset not found");
      buildMockResource(Method.GET, locationPath, queryParams, null, errResp, SC_NOT_FOUND);
      buildMockResourceForCredential(filesetName, localPath.toString());

      Path testPath = new Path(managedFilesetPath + "/test.txt");
      assertThrows(RuntimeException.class, () -> fs.setWorkingDirectory(testPath));
      assertThrows(FileNotFoundException.class, () -> fs.open(testPath));
      assertThrows(IOException.class, () -> fs.create(testPath));
      assertThrows(FileNotFoundException.class, () -> fs.append(testPath));

      Path testPath1 = new Path(managedFilesetPath + "/test1.txt");
      assertThrows(FileNotFoundException.class, () -> fs.rename(testPath, testPath1));

      assertFalse(fs.delete(testPath, true));

      assertThrows(FileNotFoundException.class, () -> fs.getFileStatus(testPath));
      assertThrows(FileNotFoundException.class, () -> fs.listStatus(testPath));

      assertThrows(IOException.class, () -> fs.mkdirs(testPath));

      assertEquals(1, fs.getDefaultReplication(testPath));
      assertEquals(FS_GRAVITINO_BLOCK_SIZE_DEFAULT, fs.getDefaultBlockSize(testPath));
    }
  }

  @Test
  public void testGravitinoClientConfig() {
    Configuration configuration = new Configuration(conf);
    // test valid client property
    configuration.set(FS_GRAVITINO_CLIENT_CONFIG_PREFIX + "connectionTimeoutMs", "8000");
    configuration.set(FS_GRAVITINO_CLIENT_CONFIG_PREFIX + "socketTimeoutMs", "4000");
    Map<String, String> clientConfig =
        GravitinoVirtualFileSystemUtils.extractClientConfig(getConfigMap(configuration));
    Assertions.assertEquals(clientConfig.get(CLIENT_CONNECTION_TIMEOUT_MS), "8000");
    Assertions.assertEquals(clientConfig.get(CLIENT_SOCKET_TIMEOUT_MS), "4000");

    // test invalid client property
    configuration.set(FS_GRAVITINO_CLIENT_CONFIG_PREFIX + "xxxx", "2000");
    Throwable throwable =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> {
              try (FileSystem fs = new Path("gvfs://fileset/").getFileSystem(configuration)) {
                // Trigger lazy initialization by accessing a path
                fs.exists(new Path("gvfs://fileset/catalog/schema/fileset/file.txt"));
              }
            });
    Assertions.assertEquals(
        "Invalid property for client: gravitino.client.xxxx", throwable.getMessage());
  }

  @Test
  public void testSocketTimeout() throws IOException {

    Configuration configuration = new Configuration(conf);
    configuration.set(FS_GRAVITINO_CLIENT_CONFIG_PREFIX + "socketTimeoutMs", "2000");

    mockServer().clear(request().withPath("/api/version"));
    HttpRequest req = request().withPath("/api/version");
    mockServer()
        .when(req, Times.once())
        .respond(
            response()
                .withStatusCode(SC_OK)
                .withBody(getJsonString(new VersionResponse(Version.getCurrentVersionDTO())))
                .withDelay(TimeUnit.MILLISECONDS, 5000));

    Throwable throwable =
        Assertions.assertThrows(
            RESTException.class,
            () -> {
              try (FileSystem fs = new Path("gvfs://fileset/").getFileSystem(configuration)) {
                // Trigger lazy initialization by accessing a path
                fs.exists(new Path("gvfs://fileset/catalog/schema/fileset/file.txt"));
              }
            });
    Assertions.assertInstanceOf(SocketTimeoutException.class, throwable.getCause());
    Assertions.assertEquals("Read timed out", throwable.getCause().getMessage());
  }

  @Test
  public void testAutoCreateLocation() throws IOException, JsonProcessingException {
    Assumptions.assumeTrue(getClass() == TestGvfsBase.class);
    String catalogNameWithFsOpsDisabled = "catalog_fs_ops_disabled";
    String schemaNameLocal = "schema_auto_create";
    String filesetName = "fileset_auto_create";

    // Mock a catalog with disable-filesystem-ops=true
    CatalogDTO catalogWithFsOpsDisabled =
        CatalogDTO.builder()
            .withName(catalogNameWithFsOpsDisabled)
            .withType(CatalogDTO.Type.FILESET)
            .withProvider(provider)
            .withComment("test catalog")
            .withProperties(ImmutableMap.of("disable-filesystem-ops", "true"))
            .withAudit(
                AuditDTO.builder().withCreator("creator").withCreateTime(Instant.now()).build())
            .build();
    CatalogResponse catalogResponse = new CatalogResponse(catalogWithFsOpsDisabled);
    buildMockResource(
        Method.GET,
        "/api/metalakes/" + metalakeName + "/catalogs/" + catalogNameWithFsOpsDisabled,
        null,
        catalogResponse,
        SC_OK);

    Path managedFilesetPath =
        FileSystemTestUtils.createFilesetPath(
            catalogNameWithFsOpsDisabled, schemaNameLocal, filesetName, true);
    Path localPath =
        FileSystemTestUtils.createLocalDirPrefix(
            catalogNameWithFsOpsDisabled, schemaNameLocal, filesetName);
    String locationPath =
        String.format(
            "/api/metalakes/%s/catalogs/%s/schemas/%s/filesets/%s/location",
            metalakeName, catalogNameWithFsOpsDisabled, schemaNameLocal, filesetName);

    // Mock the fileset
    mockFilesetDTO(
        metalakeName,
        catalogNameWithFsOpsDisabled,
        schemaNameLocal,
        filesetName,
        Fileset.Type.MANAGED,
        ImmutableMap.of("location1", localPath.toString()),
        ImmutableMap.of(PROPERTY_DEFAULT_LOCATION_NAME, "location1"));

    // Test with autoCreateLocation = false
    Configuration configWithoutAutoCreate = new Configuration(conf);
    configWithoutAutoCreate.setBoolean(
        GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_AUTO_CREATE_LOCATION, false);

    try (FileSystem gravitinoFileSystem =
            managedFilesetPath.getFileSystem(configWithoutAutoCreate);
        FileSystem localFileSystem = localPath.getFileSystem(conf)) {

      // Setup mock responses
      FileLocationResponse fileLocationResponse = new FileLocationResponse(localPath.toString());
      Map<String, String> queryParams = new HashMap<>();
      queryParams.put("sub_path", "");
      buildMockResource(Method.GET, locationPath, queryParams, null, fileLocationResponse, SC_OK);
      buildMockResourceForCredential(filesetName, localPath.toString());

      // Delete local path if it exists
      if (localFileSystem.exists(localPath)) {
        localFileSystem.delete(localPath, true);
      }

      // Verify location does not exist
      assertFalse(localFileSystem.exists(localPath));

      // Try to list the fileset - this triggers the auto-creation check
      // When autoCreateLocation=false and directory doesn't exist, it should throw
      // FileNotFoundException
      assertThrows(
          FileNotFoundException.class,
          () -> gravitinoFileSystem.listStatus(managedFilesetPath),
          "Should throw FileNotFoundException when location doesn't exist and autoCreateLocation=false");

      // Verify location was NOT auto-created when autoCreateLocation=false
      assertFalse(
          localFileSystem.exists(localPath),
          "Location should NOT be auto-created when autoCreateLocation=false");
    }

    // Test with autoCreateLocation = true (default)
    Configuration configWithAutoCreate = new Configuration(conf);
    // Don't set the config, use default which is true

    try (FileSystem gravitinoFileSystem = managedFilesetPath.getFileSystem(configWithAutoCreate);
        FileSystem localFileSystem = localPath.getFileSystem(conf)) {

      // Setup mock responses (need to rebuild since we created a new filesystem)
      FileLocationResponse fileLocationResponse = new FileLocationResponse(localPath.toString());
      Map<String, String> queryParams = new HashMap<>();
      queryParams.put("sub_path", "");
      buildMockResource(Method.GET, locationPath, queryParams, null, fileLocationResponse, SC_OK);
      buildMockResourceForCredential(filesetName, localPath.toString());

      // Delete local path if it exists
      if (localFileSystem.exists(localPath)) {
        localFileSystem.delete(localPath, true);
      }

      // Verify location does not exist initially
      assertFalse(localFileSystem.exists(localPath));

      // Try to list the fileset - with autoCreateLocation=true (default), it should create the
      // location
      gravitinoFileSystem.listStatus(managedFilesetPath);

      // Verify location WAS auto-created when autoCreateLocation=true (default)
      assertTrue(
          localFileSystem.exists(localPath),
          "Location SHOULD be auto-created when autoCreateLocation=true");

      // Clean up
      localFileSystem.delete(localPath, true);
    }
  }

  @Test
  public void testHookSetOperationsContext() throws IOException {
    String filesetName = "testHookSetOperationsContext";
    Path managedFilesetPath =
        FileSystemTestUtils.createFilesetPath(catalogName, schemaName, filesetName, true);
    Path localPath = FileSystemTestUtils.createLocalDirPrefix(catalogName, schemaName, filesetName);
    String locationPath =
        String.format(
            "/api/metalakes/%s/catalogs/%s/schemas/%s/filesets/%s/location",
            metalakeName, catalogName, schemaName, filesetName);

    try (GravitinoVirtualFileSystem fs =
        (GravitinoVirtualFileSystem) managedFilesetPath.getFileSystem(conf)) {

      // Verify that setOperationsContext was called during GVFS initialization
      MockGVFSHook hook = getHook(fs);
      assertTrue(
          hook.setOperationsContextCalled,
          "setOperationsContext should be called during initialization");
      assertNotNull(hook.operations, "Operations context should not be null");
      assertEquals(
          fs.getOperations(),
          hook.operations,
          "Hook should have reference to the same operations instance");

      // Verify the hook can access operations methods
      FileLocationResponse fileLocationResponse = new FileLocationResponse(localPath.toString());
      Map<String, String> queryParams = new HashMap<>();
      queryParams.put("sub_path", "");
      buildMockResource(Method.GET, locationPath, queryParams, null, fileLocationResponse, SC_OK);
      buildMockResourceForCredential(filesetName, localPath.toString());

      try (FileSystem localFileSystem = localPath.getFileSystem(conf)) {
        FileSystemTestUtils.mkdirs(localPath, localFileSystem);
        FileSystemTestUtils.mkdirs(managedFilesetPath, fs);

        // Verify hook's operations context is still valid after operations are performed
        assertNotNull(hook.operations, "Operations context should remain available");
        assertTrue(hook.preMkdirsCalled, "Hook should be invoked for operations");
        assertTrue(hook.postMkdirsCalled, "Hook should be invoked for operations");
      }
    }
  }

  @Test
  public void testFailureHandlingMethodsAreCalled()
      throws IOException, NoSuchFieldException, IllegalAccessException {
    Assumptions.assumeTrue(getClass() == TestGvfsBase.class);
    Configuration newConf = new Configuration(conf);
    newConf.set(
        GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_HOOK_CLASS,
        MockGVFSHook.class.getCanonicalName());

    try (GravitinoVirtualFileSystem fs =
        (GravitinoVirtualFileSystem) new Path("gvfs://fileset/").getFileSystem(newConf)) {
      BaseGVFSOperations mockOps = Mockito.mock(BaseGVFSOperations.class);

      // Inject the mockOps
      Field operationsField = GravitinoVirtualFileSystem.class.getDeclaredField("operations");
      operationsField.setAccessible(true);
      operationsField.set(fs, mockOps);

      MockGVFSHook hook = getHook(fs);

      // Test setWorkingDirectory failure
      Mockito.doThrow(new RuntimeException("setWorkingDirectory failed"))
          .when(mockOps)
          .setWorkingDirectory(any());
      assertThrows(
          RuntimeException.class, () -> fs.setWorkingDirectory(new Path("gvfs://fileset/")));
      assertTrue(
          hook.onSetWorkingDirectoryFailureCalled, "onSetWorkingDirectoryFailure should be called");

      // Test open failure
      Mockito.doThrow(new RuntimeException("open failed")).when(mockOps).open(any(), anyInt());
      assertThrows(RuntimeException.class, () -> fs.open(new Path("gvfs://fileset/"), 1024));
      assertTrue(hook.onOpenFailureCalled, "onOpenFailure should be called");

      // Test create failure
      Mockito.doThrow(new RuntimeException("create failed"))
          .when(mockOps)
          .create(any(), any(), anyBoolean(), anyInt(), anyShort(), anyLong(), any());
      assertThrows(RuntimeException.class, () -> fs.create(new Path("gvfs://fileset/"), true));
      assertTrue(hook.onCreateFailureCalled, "onCreateFailure should be called");

      // Test append failure
      Mockito.doThrow(new RuntimeException("append failed"))
          .when(mockOps)
          .append(any(), anyInt(), any());
      assertThrows(RuntimeException.class, () -> fs.append(new Path("gvfs://fileset/"), 1024));
      assertTrue(hook.onAppendFailureCalled, "onAppendFailure should be called");

      // Test rename failure
      Mockito.doThrow(new RuntimeException("rename failed")).when(mockOps).rename(any(), any());
      assertThrows(
          RuntimeException.class,
          () -> fs.rename(new Path("gvfs://fileset/"), new Path("gvfs://fileset/new")));
      assertTrue(hook.onRenameFailureCalled, "onRenameFailure should be called");

      // Test delete failure
      Mockito.doThrow(new RuntimeException("delete failed"))
          .when(mockOps)
          .delete(any(), anyBoolean());
      assertThrows(RuntimeException.class, () -> fs.delete(new Path("gvfs://fileset/"), true));
      assertTrue(hook.onDeleteFailureCalled, "onDeleteFailure should be called");

      // Test getFileStatus failure
      Mockito.doThrow(new RuntimeException("getFileStatus failed"))
          .when(mockOps)
          .getFileStatus(any());
      assertThrows(RuntimeException.class, () -> fs.getFileStatus(new Path("gvfs://fileset/")));
      assertTrue(hook.onGetFileStatusFailureCalled, "onGetFileStatusFailure should be called");

      // Test listStatus failure
      Mockito.doThrow(new RuntimeException("listStatus failed")).when(mockOps).listStatus(any());
      assertThrows(RuntimeException.class, () -> fs.listStatus(new Path("gvfs://fileset/")));
      assertTrue(hook.onListStatusFailureCalled, "onListStatusFailure should be called");

      // Test mkdirs failure
      Mockito.doThrow(new RuntimeException("mkdirs failed")).when(mockOps).mkdirs(any(), any());
      assertThrows(RuntimeException.class, () -> fs.mkdirs(new Path("gvfs://fileset/")));
      assertTrue(hook.onMkdirsFailureCalled, "onMkdirsFailure should be called");

      // Test getDefaultReplication failure
      Mockito.doThrow(new RuntimeException("getDefaultReplication failed"))
          .when(mockOps)
          .getDefaultReplication(any());
      assertThrows(
          RuntimeException.class, () -> fs.getDefaultReplication(new Path("gvfs://fileset/")));
      assertTrue(
          hook.onGetDefaultReplicationFailureCalled,
          "onGetDefaultReplicationFailure should be called");

      // Test getDefaultBlockSize failure
      Mockito.doThrow(new RuntimeException("getDefaultBlockSize failed"))
          .when(mockOps)
          .getDefaultBlockSize(any());
      assertThrows(
          RuntimeException.class, () -> fs.getDefaultBlockSize(new Path("gvfs://fileset/")));
      assertTrue(
          hook.onGetDefaultBlockSizeFailureCalled, "onGetDefaultBlockSizeFailure should be called");
    }
  }

  @Test
  public void testFailureHandlingWithSpecificExceptions()
      throws IOException, NoSuchFieldException, IllegalAccessException {
    Assumptions.assumeTrue(getClass() == TestGvfsBase.class);
    Configuration newConf = new Configuration(conf);
    newConf.set(
        GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_HOOK_CLASS,
        MockGVFSHook.class.getCanonicalName());

    try (GravitinoVirtualFileSystem fs =
        (GravitinoVirtualFileSystem) new Path("gvfs://fileset/").getFileSystem(newConf)) {
      BaseGVFSOperations mockOps = Mockito.mock(BaseGVFSOperations.class);

      // Inject the mockOps
      Field operationsField = GravitinoVirtualFileSystem.class.getDeclaredField("operations");
      operationsField.setAccessible(true);
      operationsField.set(fs, mockOps);

      MockGVFSHook hook = getHook(fs);

      // Test with FileNotFoundException
      Mockito.doThrow(new FileNotFoundException("File not found"))
          .when(mockOps)
          .open(any(), anyInt());
      assertThrows(FileNotFoundException.class, () -> fs.open(new Path("gvfs://fileset/"), 1024));
      assertTrue(
          hook.onOpenFailureCalled, "onOpenFailure should be called with FileNotFoundException");

      // Test with IOException
      Mockito.doThrow(new IOException("IO error"))
          .when(mockOps)
          .create(any(), any(), anyBoolean(), anyInt(), anyShort(), anyLong(), any());
      assertThrows(IOException.class, () -> fs.create(new Path("gvfs://fileset/"), true));
      assertTrue(hook.onCreateFailureCalled, "onCreateFailure should be called with IOException");

      // Test with SecurityException
      Mockito.doThrow(new SecurityException("Security violation"))
          .when(mockOps)
          .delete(any(), anyBoolean());
      assertThrows(SecurityException.class, () -> fs.delete(new Path("gvfs://fileset/"), true));
      assertTrue(
          hook.onDeleteFailureCalled, "onDeleteFailure should be called with SecurityException");
    }
  }

  @Test
  public void testFailureHandlingMethodParameters()
      throws IOException, NoSuchFieldException, IllegalAccessException {
    Assumptions.assumeTrue(getClass() == TestGvfsBase.class);
    Configuration newConf = new Configuration(conf);
    newConf.set(
        GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_HOOK_CLASS,
        MockGVFSHook.class.getCanonicalName());

    try (GravitinoVirtualFileSystem fs =
        (GravitinoVirtualFileSystem) new Path("gvfs://fileset/").getFileSystem(newConf)) {
      BaseGVFSOperations mockOps = Mockito.mock(BaseGVFSOperations.class);

      // Inject the mockOps
      Field operationsField = GravitinoVirtualFileSystem.class.getDeclaredField("operations");
      operationsField.setAccessible(true);
      operationsField.set(fs, mockOps);

      MockGVFSHook hook = getHook(fs);

      Path testPath = new Path("gvfs://fileset/test");
      RuntimeException testException = new RuntimeException("Test exception");

      // Test that failure methods receive correct parameters
      Mockito.doThrow(testException).when(mockOps).setWorkingDirectory(testPath);
      assertThrows(RuntimeException.class, () -> fs.setWorkingDirectory(testPath));
      assertTrue(
          hook.onSetWorkingDirectoryFailureCalled, "onSetWorkingDirectoryFailure should be called");

      // Test with specific parameters for create
      Mockito.doThrow(testException)
          .when(mockOps)
          .create(eq(testPath), any(), eq(true), eq(1024), eq((short) 1), eq(128L), any());
      assertThrows(RuntimeException.class, () -> fs.create(testPath, true, 1024, (short) 1, 128L));
      assertTrue(
          hook.onCreateFailureCalled, "onCreateFailure should be called with correct parameters");
    }
  }

  protected void buildMockResourceForCredential(String filesetName, String filesetLocation)
      throws JsonProcessingException {
    String filesetPath =
        String.format(
            "/api/metalakes/%s/catalogs/%s/schemas/%s/filesets/%s",
            metalakeName, catalogName, schemaName, RESTUtils.encodeString(filesetName));
    String credentialsPath =
        String.format(
            "/api/metalakes/%s/objects/fileset/%s.%s.%s/credentials",
            metalakeName, catalogName, schemaName, RESTUtils.encodeString(filesetName));
    FilesetResponse filesetResponse =
        new FilesetResponse(
            FilesetDTO.builder()
                .name(filesetName)
                .comment("comment")
                .type(Fileset.Type.MANAGED)
                .audit(AuditDTO.builder().build())
                .storageLocations(ImmutableMap.of(LOCATION_NAME_UNKNOWN, filesetLocation))
                .properties(ImmutableMap.of(PROPERTY_DEFAULT_LOCATION_NAME, LOCATION_NAME_UNKNOWN))
                .build());
    CredentialResponse credentialResponse = new CredentialResponse(new CredentialDTO[] {});

    buildMockResource(Method.GET, filesetPath, ImmutableMap.of(), null, filesetResponse, SC_OK);
    buildMockResource(
        Method.GET, credentialsPath, ImmutableMap.of(), null, credentialResponse, SC_OK);
  }

  private MockGVFSHook getHook(FileSystem gvfs) {
    return (MockGVFSHook) ((GravitinoVirtualFileSystem) gvfs).getHook();
  }
}
