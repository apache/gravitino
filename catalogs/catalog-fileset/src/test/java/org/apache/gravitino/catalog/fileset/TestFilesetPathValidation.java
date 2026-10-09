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
package org.apache.gravitino.catalog.fileset;

import static org.apache.gravitino.file.Fileset.PROPERTY_DEFAULT_LOCATION_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Collections;
import java.util.stream.Stream;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.audit.CallerContext;
import org.apache.gravitino.audit.FilesetAuditConstants;
import org.apache.gravitino.audit.FilesetDataOperation;
import org.apache.gravitino.exceptions.GravitinoRuntimeException;
import org.apache.gravitino.file.FileInfo;
import org.apache.gravitino.file.Fileset;
import org.apache.gravitino.secret.SecretManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

class TestFilesetPathValidation {
  private static final NameIdentifier FILESET =
      NameIdentifier.of("metalake", "catalog", "schema", "fileset");

  @TempDir File tempDir;

  private FilesetCatalogOperations operations;
  private Fileset fileset;

  @BeforeEach
  void setUp() {
    fileset = mock(Fileset.class);
    when(fileset.name()).thenReturn(FILESET.name());
    when(fileset.properties())
        .thenReturn(ImmutableMap.of(PROPERTY_DEFAULT_LOCATION_NAME, "primary"));
    when(fileset.storageLocations())
        .thenReturn(ImmutableMap.of("primary", "file:///storage/fileset"));
    operations =
        spy(new FilesetCatalogOperations(mock(EntityStore.class), mock(SecretManager.class)));
    doReturn(fileset).when(operations).loadFileset(FILESET);
  }

  @AfterEach
  void tearDown() throws IOException {
    CallerContext.CallerContextHolder.remove();
    operations.close();
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "..",
        "../outside",
        "/../outside",
        "/dir/../../outside",
        "dir/../file",
        " /../outside ",
        "/..\\outside",
        "\\..\\outside",
        "/dir\\..\\outside",
        "/file\u0000name",
        "\u0000file",
        "/file\u0000"
      })
  void testRejectUnsafeSubPaths(String subPath) {
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> operations.getFileLocation(FILESET, subPath, null));
    verify(operations, never()).getFileSystemWithCache(any(), any());
  }

  @ParameterizedTest
  @MethodSource("validLocations")
  void testPreserveValidLocations(String storageLocation, String subPath, String expected) {
    when(fileset.storageLocations()).thenReturn(ImmutableMap.of("primary", storageLocation));
    Assertions.assertEquals(expected, operations.getFileLocation(FILESET, subPath, null));
    verify(operations, never()).getFileSystemWithCache(any(), any());
  }

  @Test
  void testSelectedLocationDefinesBoundary() {
    when(fileset.storageLocations())
        .thenReturn(
            ImmutableMap.of(
                "primary", "s3a://bucket/primary", "archive", "hdfs://namenode:8020/archive"));
    Assertions.assertEquals(
        "s3a://bucket/primary/data", operations.getFileLocation(FILESET, "/data", null));
    Assertions.assertEquals(
        "hdfs://namenode:8020/archive/data",
        operations.getFileLocation(FILESET, "/data", "archive"));
    for (String location : fileset.storageLocations().keySet()) {
      Assertions.assertThrows(
          IllegalArgumentException.class,
          () -> operations.getFileLocation(FILESET, "/../outside", location));
    }
  }

  @Test
  void testRejectAuthorityChangeAtFilesystemRoot() {
    when(fileset.storageLocations()).thenReturn(ImmutableMap.of("primary", "/"));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> operations.getFileLocation(FILESET, "//other-host/outside", null));
  }

  @ParameterizedTest
  @EnumSource(FilesetDataOperation.class)
  void testOperationHeadersCannotBypassValidation(FilesetDataOperation operation) {
    CallerContext.CallerContextHolder.set(
        CallerContext.builder()
            .withContext(
                ImmutableMap.of(
                    FilesetAuditConstants.HTTP_HEADER_FILESET_DATA_OPERATION, operation.name()))
            .build());
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> operations.getFileLocation(FILESET, "/../outside", null));
  }

  @ParameterizedTest
  @ValueSource(strings = {"", "/", "/.", "/./", "//", "///"})
  void testRenameCannotTargetRootAliases(String subPath) {
    CallerContext.CallerContextHolder.set(
        CallerContext.builder()
            .withContext(
                ImmutableMap.of(
                    FilesetAuditConstants.HTTP_HEADER_FILESET_DATA_OPERATION,
                    FilesetDataOperation.RENAME.name()))
            .build());
    Assertions.assertThrows(
        GravitinoRuntimeException.class, () -> operations.getFileLocation(FILESET, subPath, null));
  }

  @Test
  void testListFilesCannotReachSiblingSentinel() throws IOException {
    File root = new File(tempDir, "root");
    File outside = new File(tempDir, "root-sibling");
    Files.createDirectories(root.toPath());
    Files.createDirectories(outside.toPath());
    Files.writeString(new File(outside, "sentinel.txt").toPath(), "outside the fileset");
    when(fileset.storageLocations())
        .thenReturn(ImmutableMap.of("primary", root.toURI().toString()));

    try (FileSystem fs = FileSystem.newInstanceLocal(new Configuration(false))) {
      doReturn(Collections.emptyMap())
          .when(operations)
          .mergeUpLevelConfigurations(any(), any(), any());
      doReturn(fs).when(operations).getFileSystemWithCache(any(), any());
      Assertions.assertEquals(1, fs.listStatus(new Path(outside.toURI())).length);
      Assertions.assertEquals(0, operations.listFiles(FILESET, null, "/").length);
      clearInvocations(operations);

      for (String subPath :
          new String[] {"/../root-sibling", "/../root-sibling/sentinel.txt", "/../missing"}) {
        Assertions.assertThrows(
            IllegalArgumentException.class, () -> operations.listFiles(FILESET, null, subPath));
      }
      verify(operations, never()).mergeUpLevelConfigurations(any(), any(), any());
      verify(operations, never()).getFileSystemWithCache(any(), any());
    }
  }

  @Test
  void testListFilesPreservesLiteralEncodedNames() throws IOException {
    File root = new File(tempDir, "root");
    File child = new File(root, "%2e%2e");
    Files.createDirectories(child.toPath());
    Files.writeString(new File(child, "a..b + caf\u00e9.txt").toPath(), "inside the fileset");
    when(fileset.storageLocations())
        .thenReturn(ImmutableMap.of("primary", root.toURI().toString()));

    try (FileSystem fs = FileSystem.newInstanceLocal(new Configuration(false))) {
      doReturn(Collections.emptyMap())
          .when(operations)
          .mergeUpLevelConfigurations(any(), any(), any());
      doReturn(fs).when(operations).getFileSystemWithCache(any(), any());
      FileInfo[] files = operations.listFiles(FILESET, null, "/%2e%2e");
      Assertions.assertEquals(1, files.length);
      Assertions.assertEquals("a..b + caf\u00e9.txt", files[0].name());
    }
  }

  private static Stream<Arguments> validLocations() {
    return Stream.of(
        Arguments.of("file:///storage/fileset", "", "file:///storage/fileset"),
        Arguments.of("file:///storage/fileset", "/", "file:///storage/fileset/"),
        Arguments.of("file:///storage/fileset/", "dir/file", "file:///storage/fileset/dir/file"),
        Arguments.of("file:///storage/fileset/.", "/child", "file:///storage/fileset/./child"),
        Arguments.of("file:///storage/fileset", "/dir/file", "file:///storage/fileset/dir/file"),
        Arguments.of("file:///storage/fileset", "/a..b", "file:///storage/fileset/a..b"),
        Arguments.of(
            "file:///storage/fileset", "/dir/./file", "file:///storage/fileset/dir/./file"),
        Arguments.of("file:///storage/fileset", "/%2e%2e", "file:///storage/fileset/%2e%2e"),
        Arguments.of("file:///storage/fileset", "/a + b", "file:///storage/fileset/a + b"),
        Arguments.of("file:///", "/child", "file:///child"),
        Arguments.of("/", "/child", "/child"),
        Arguments.of("/storage/fileset", "child", "/storage/fileset/child"),
        Arguments.of(
            "hdfs://namenode:8020/fileset", "/child", "hdfs://namenode:8020/fileset/child"),
        Arguments.of("s3a://bucket/prefix", "/child", "s3a://bucket/prefix/child"),
        Arguments.of("s3a://bucket", "/child", "s3a://bucket/child"),
        Arguments.of("s3a://bucket", "", "s3a://bucket"),
        Arguments.of(
            "abfs://container@account.dfs.core.windows.net/prefix",
            "/child",
            "abfs://container@account.dfs.core.windows.net/prefix/child"));
  }
}
