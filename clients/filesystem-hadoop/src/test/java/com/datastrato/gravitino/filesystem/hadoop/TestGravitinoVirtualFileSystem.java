/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.filesystem.hadoop;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.datastrato.gravitino.filesystem.hadoop.utils.FileSystemTestUtils;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.InvalidPathException;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestGravitinoVirtualFileSystem extends MockServerTestBase {
  private static final String GVFS_IMPL_CLASS = GravitinoVirtualFileSystem.class.getName();
  private static final String GVFS_ABSTRACT_IMPL_CLASS = Gvfs.class.getName();

  private FileSystem hadoopFileSystem = null;
  private FileSystem gravitinoFileSystem = null;
  private Configuration conf = null;
  private Path hdfsPath = null;
  private Path gravitinoPath = null;

  @BeforeAll
  public static void setup() {
    MockServerTestBase.setup();
  }

  @BeforeEach
  public void init() throws IOException {
    mockMetalakeDTO(metalakeName, "comment");
    mockCatalogDTO(catalogName, provider, "comment");
    hdfsPath =
        FileSystemTestUtils.createHdfsPrefix(metalakeName, catalogName, schemaName, filesetName);
    mockFilesetDTO(metalakeName, catalogName, schemaName, filesetName, hdfsPath.toString());

    Configuration configuration = HdfsMiniClusterTestBase.hadoopFileSystem().getConf();
    configuration.set("fs.gvfs.impl", GVFS_IMPL_CLASS);
    configuration.set("fs.AbstractFileSystem.gvfs.impl", GVFS_ABSTRACT_IMPL_CLASS);
    configuration.set("fs.gravitino.server.uri", MockServerTestBase.serverUri());
    conf = configuration;
    gravitinoPath =
        FileSystemTestUtils.createGvfsPrefix(metalakeName, catalogName, schemaName, filesetName);
    hadoopFileSystem = HdfsMiniClusterTestBase.hadoopFileSystem();
    gravitinoFileSystem = gravitinoPath.getFileSystem(conf);
  }

  @AfterEach
  public void destroy() throws IOException {
    if (hadoopFileSystem.exists(hdfsPath)) {
      hadoopFileSystem.delete(hdfsPath, true);
    }
    if (gravitinoFileSystem.exists(gravitinoPath)) {
      gravitinoFileSystem.delete(gravitinoPath, true);
    }
  }

  @Test
  public void testCloseFSCache() throws IOException {
    Configuration configuration = HdfsMiniClusterTestBase.hadoopFileSystem().getConf();
    assertEquals(
        "true",
        configuration.get(
            String.format(
                "fs.%s.impl.disable.cache", GravitinoVirtualFileSystemConfiguration.GVFS_SCHEME)));
    assertEquals("true", configuration.get(String.format("fs.%s.impl.disable.cache", "hdfs")));

    String testFilesetName = "fileset_3";
    Path hdfsPath =
        FileSystemTestUtils.createHdfsPrefix(
            metalakeName, catalogName, schemaName, testFilesetName);

    mockFilesetDTO(metalakeName, catalogName, schemaName, testFilesetName, hdfsPath.toString());

    Path diffGvfsPath =
        FileSystemTestUtils.createGvfsPrefix(
            metalakeName, catalogName, schemaName, testFilesetName);
    FileSystem gvfs = diffGvfsPath.getFileSystem(conf);
    assertNotEquals(gvfs, gravitinoFileSystem);

    Path diffHdfsPath =
        FileSystemTestUtils.createHdfsPrefix(
            metalakeName, catalogName, schemaName, testFilesetName);
    FileSystem hdfs = diffHdfsPath.getFileSystem(conf);
    assertNotEquals(hdfs, hadoopFileSystem);
  }

  @Test
  public void testCreate() throws IOException {
    // mock the verified fileset with diff name
    String createFilesetName = "fileset_create";
    Path createFilesetPath =
        FileSystemTestUtils.createGvfsPrefix(
            metalakeName, catalogName, schemaName, createFilesetName);
    Path createHdfsPath =
        FileSystemTestUtils.createHdfsPrefix(
            metalakeName, catalogName, schemaName, createFilesetName);
    mockFilesetDTO(
        metalakeName, catalogName, schemaName, createFilesetName, createHdfsPath.toString());
    FileSystemTestUtils.create(createFilesetPath, gravitinoFileSystem);
    assertTrue(gravitinoFileSystem.exists(createFilesetPath));
    gravitinoFileSystem.delete(createFilesetPath, true);

    // mock the unverified fileset with diff name
    String unverifiedCreateFilesetName = "fileset_create_unverified";
    Path unverifiedCreateFilesetPath =
        FileSystemTestUtils.createGvfsPrefix(
            metalakeName, catalogName, schemaName, unverifiedCreateFilesetName);
    assertThrows(
        RuntimeException.class,
        () -> FileSystemTestUtils.create(unverifiedCreateFilesetPath, gravitinoFileSystem));

    // mock the not correct prefix path
    Path hdfsPrefixPath =
        FileSystemTestUtils.createHdfsPrefix(metalakeName, catalogName, schemaName, "test");
    assertThrows(
        RuntimeException.class,
        () -> FileSystemTestUtils.create(hdfsPrefixPath, gravitinoFileSystem));

    // check if the file is created in the hdfs and gvfs can access it
    Path hdfsFilePath = new Path(hdfsPath.toString() + "/append.txt");
    Path gravitinoFilePath = new Path(gravitinoPath.toString() + "/append.txt");
    FileSystemTestUtils.create(hdfsFilePath, hadoopFileSystem);
    assertTrue(hadoopFileSystem.exists(hdfsFilePath));
    FileStatus hdfsFileStatus = hadoopFileSystem.getFileStatus(hdfsFilePath);
    assertTrue(gravitinoFileSystem.exists(gravitinoFilePath));
    hadoopFileSystem.delete(hdfsFilePath, true);

    // check if the file is created in the gvfs and hdfs can access it
    FileSystemTestUtils.create(gravitinoFilePath, gravitinoFileSystem);
    assertTrue(gravitinoFileSystem.exists(gravitinoFilePath));
    assertTrue(hadoopFileSystem.exists(hdfsFilePath));
    FileStatus gravitinoFileStatus = gravitinoFileSystem.getFileStatus(gravitinoFilePath);
    gravitinoFileSystem.delete(gravitinoFilePath, true);

    assertEquals(
        hdfsFileStatus.getPath().toString(),
        gravitinoFileStatus
            .getPath()
            .toString()
            .replaceFirst(
                GravitinoVirtualFileSystemConfiguration.GVFS_FILESET_PREFIX,
                hadoopFileSystem.getScheme() + "://" + hadoopFileSystem.getUri().getHost()));
  }

  @Test
  public void testAppend() throws IOException {
    Path diffAppendPath =
        FileSystemTestUtils.createGvfsPrefix(
            metalakeName, catalogName, schemaName, "fileset_append");
    mockFilesetDTO(
        metalakeName,
        catalogName,
        schemaName,
        "fileset_append",
        FileSystemTestUtils.createHdfsPrefix(
                metalakeName, catalogName, schemaName, "fileset_append")
            .toString());
    Path appendFile = new Path(diffAppendPath + "/test.txt");
    FileSystemTestUtils.create(appendFile, gravitinoFileSystem);
    FileSystemTestUtils.append(appendFile, gravitinoFileSystem);
    assertTrue(gravitinoFileSystem.exists(appendFile));
    gravitinoFileSystem.delete(appendFile, true);

    Path mockFilePath = new Path(hdfsPath.toString() + "/append.txt");
    Path gravitinoFilePath = new Path(gravitinoPath.toString() + "/append.txt");
    FileSystemTestUtils.create(mockFilePath, hadoopFileSystem);
    FileSystemTestUtils.append(mockFilePath, hadoopFileSystem);
    assertTrue(hadoopFileSystem.exists(mockFilePath));
    FileStatus mockFileStatus = hadoopFileSystem.getFileStatus(mockFilePath);
    byte[] mockInputContent = FileSystemTestUtils.read(mockFilePath, hadoopFileSystem);
    assertEquals(new String(mockInputContent), "Hello, World!");
    assertTrue(gravitinoFileSystem.exists(gravitinoFilePath));
    hadoopFileSystem.delete(mockFilePath, true);

    FileSystemTestUtils.create(gravitinoFilePath, gravitinoFileSystem);
    FileSystemTestUtils.append(gravitinoFilePath, gravitinoFileSystem);
    assertTrue(gravitinoFileSystem.exists(gravitinoFilePath));
    FileStatus gravitinoFileStatus = gravitinoFileSystem.getFileStatus(gravitinoFilePath);
    byte[] gravitinoInputContent = FileSystemTestUtils.read(gravitinoFilePath, gravitinoFileSystem);
    assertEquals(new String(gravitinoInputContent), "Hello, World!");
    gravitinoFileSystem.delete(gravitinoFilePath, true);

    assertEquals(new String(mockInputContent), new String(gravitinoInputContent));
    assertEquals(
        mockFileStatus.getPath().toString(),
        gravitinoFileStatus
            .getPath()
            .toString()
            .replaceFirst(
                GravitinoVirtualFileSystemConfiguration.GVFS_FILESET_PREFIX,
                hadoopFileSystem.getScheme() + "://" + hadoopFileSystem.getUri().getHost()));
  }

  @Test
  public void testRename() throws IOException {
    Path mockFilePath = new Path(hdfsPath.toString() + "/append.txt");
    Path gravitinoFilePath = new Path(gravitinoPath.toString() + "/append.txt");
    FileSystemTestUtils.create(mockFilePath, hadoopFileSystem);
    assertTrue(hadoopFileSystem.exists(mockFilePath));
    assertTrue(gravitinoFileSystem.exists(gravitinoFilePath));

    Path renameMockFilePath = new Path(hdfsPath.toString() + "/append1.txt");
    hadoopFileSystem.rename(mockFilePath, renameMockFilePath);
    assertFalse(hadoopFileSystem.exists(mockFilePath));
    assertTrue(hadoopFileSystem.exists(renameMockFilePath));
    FileStatus renameFileStatus = hadoopFileSystem.getFileStatus(renameMockFilePath);
    hadoopFileSystem.delete(renameMockFilePath, true);

    FileSystemTestUtils.create(gravitinoFilePath, gravitinoFileSystem);
    assertTrue(gravitinoFileSystem.exists(gravitinoFilePath));
    Path renameGravitinoFilePath = new Path(gravitinoPath.toString() + "/append1.txt");
    gravitinoFileSystem.rename(gravitinoFilePath, renameGravitinoFilePath);
    assertFalse(gravitinoFileSystem.exists(gravitinoFilePath));
    assertTrue(gravitinoFileSystem.exists(renameGravitinoFilePath));
    FileStatus renameGravitinoFileStatus =
        gravitinoFileSystem.getFileStatus(renameGravitinoFilePath);
    gravitinoFileSystem.delete(renameGravitinoFilePath, true);

    Path invalidSrcPath =
        FileSystemTestUtils.createGvfsPrefix(
            metalakeName, catalogName, schemaName, "invalid_src_name");
    Path validDstPath =
        FileSystemTestUtils.createGvfsPrefix(metalakeName, catalogName, schemaName, filesetName);
    assertThrows(
        RuntimeException.class, () -> gravitinoFileSystem.rename(invalidSrcPath, validDstPath));

    Path invalidDstPath =
        FileSystemTestUtils.createGvfsPrefix(
            metalakeName, catalogName, schemaName, "invalid_dst_name");
    assertThrows(
        RuntimeException.class, () -> gravitinoFileSystem.rename(gravitinoPath, invalidDstPath));

    assertEquals(
        renameFileStatus.getPath().toString(),
        renameGravitinoFileStatus
            .getPath()
            .toString()
            .replaceFirst(
                GravitinoVirtualFileSystemConfiguration.GVFS_FILESET_PREFIX,
                hadoopFileSystem.getScheme() + "://" + hadoopFileSystem.getUri().getHost()));
  }

  @Test
  public void testDelete() throws IOException {
    String deleteFilesetName = "fileset_delete";
    mockFilesetDTO(
        metalakeName,
        catalogName,
        schemaName,
        deleteFilesetName,
        FileSystemTestUtils.createHdfsPrefix(
                metalakeName, catalogName, schemaName, deleteFilesetName)
            .toString());

    Path diffDeletePath =
        FileSystemTestUtils.createHdfsPrefix(
            metalakeName, catalogName, schemaName, deleteFilesetName);
    FileSystem fs1 = diffDeletePath.getFileSystem(conf);
    FileSystemTestUtils.create(diffDeletePath, fs1);
    assertNotEquals(fs1, gravitinoFileSystem);
    assertThrows(
        InvalidPathException.class, () -> gravitinoFileSystem.delete(diffDeletePath, true));
    fs1.delete(diffDeletePath, true);

    Path mockFilePath = new Path(hdfsPath.toString() + "/testDelete.txt");
    Path gravitinoFilePath = new Path(gravitinoPath.toString() + "/testDelete.txt");
    FileSystemTestUtils.create(mockFilePath, hadoopFileSystem);
    assertTrue(hadoopFileSystem.exists(mockFilePath));
    assertTrue(gravitinoFileSystem.exists(gravitinoFilePath));
    hadoopFileSystem.delete(mockFilePath, true);
    assertFalse(hadoopFileSystem.exists(mockFilePath));
    assertFalse(gravitinoFileSystem.exists(gravitinoFilePath));

    FileSystemTestUtils.create(gravitinoFilePath, gravitinoFileSystem);
    assertTrue(gravitinoFileSystem.exists(gravitinoFilePath));
    gravitinoFileSystem.delete(gravitinoFilePath, true);
    assertFalse(gravitinoFileSystem.exists(gravitinoFilePath));
  }

  @Test
  public void testGetStatus() throws IOException {
    Path diffGetStatusPath =
        FileSystemTestUtils.createGvfsPrefix(
            metalakeName, catalogName, schemaName, "fileset_get_status");
    Path hdfsFilePath =
        FileSystemTestUtils.createHdfsPrefix(
            metalakeName, catalogName, schemaName, "fileset_get_status");
    mockFilesetDTO(
        metalakeName, catalogName, schemaName, "fileset_get_status", hdfsFilePath.toString());
    FileSystemTestUtils.create(diffGetStatusPath, gravitinoFileSystem);
    assertNotNull(gravitinoFileSystem.getFileStatus(diffGetStatusPath));
    gravitinoFileSystem.delete(diffGetStatusPath, true);

    Path mockFilePath = new Path(hdfsPath.toString() + "/testGet.txt");
    Path gravitinoFilePath = new Path(gravitinoPath.toString() + "/testGet.txt");
    FileSystemTestUtils.create(mockFilePath, hadoopFileSystem);
    assertTrue(hadoopFileSystem.exists(mockFilePath));
    assertTrue(gravitinoFileSystem.exists(gravitinoFilePath));
    FileStatus mockStatuses = hadoopFileSystem.getFileStatus(hdfsPath);
    hadoopFileSystem.delete(mockFilePath, true);
    assertFalse(hadoopFileSystem.exists(mockFilePath));
    assertFalse(gravitinoFileSystem.exists(gravitinoFilePath));

    FileSystemTestUtils.create(gravitinoFilePath, gravitinoFileSystem);
    assertTrue(gravitinoFileSystem.exists(gravitinoFilePath));
    FileStatus gravitinoStatuses = gravitinoFileSystem.getFileStatus(gravitinoPath);
    gravitinoFileSystem.delete(gravitinoFilePath, true);
    assertFalse(gravitinoFileSystem.exists(gravitinoFilePath));
    assertEquals(
        mockStatuses.getPath().toString(),
        gravitinoStatuses
            .getPath()
            .toString()
            .replaceFirst(
                GravitinoVirtualFileSystemConfiguration.GVFS_FILESET_PREFIX,
                hadoopFileSystem.getScheme() + "://" + hadoopFileSystem.getUri().getHost()));
  }

  @Test
  public void testListStatus() throws IOException {
    String listFilesetName = "fileset_status";
    mockFilesetDTO(
        metalakeName,
        catalogName,
        schemaName,
        listFilesetName,
        FileSystemTestUtils.createHdfsPrefix(metalakeName, catalogName, schemaName, listFilesetName)
            .toString());

    Path diffStatusPath =
        FileSystemTestUtils.createGvfsPrefix(
            metalakeName, catalogName, schemaName, listFilesetName);
    FileSystem fs2 = diffStatusPath.getFileSystem(conf);
    FileSystemTestUtils.create(diffStatusPath, fs2);
    assertNotEquals(fs2, gravitinoFileSystem);
    assertNotNull(gravitinoFileSystem.listStatus(diffStatusPath));
    fs2.delete(diffStatusPath, true);

    Path mockFilePath = new Path(hdfsPath.toString() + "/testList.txt");
    Path gravitinoFilePath = new Path(gravitinoPath.toString() + "/testList.txt");
    FileSystemTestUtils.create(mockFilePath, hadoopFileSystem);
    assertTrue(hadoopFileSystem.exists(mockFilePath));
    assertTrue(gravitinoFileSystem.exists(gravitinoFilePath));
    FileStatus[] mockStatuses = hadoopFileSystem.listStatus(hdfsPath);
    assertEquals(1, mockStatuses.length);
    hadoopFileSystem.delete(mockFilePath, true);
    assertFalse(hadoopFileSystem.exists(mockFilePath));
    assertFalse(gravitinoFileSystem.exists(gravitinoFilePath));

    FileSystemTestUtils.create(gravitinoFilePath, gravitinoFileSystem);
    assertTrue(gravitinoFileSystem.exists(gravitinoFilePath));
    FileStatus[] gravitinoStatuses = gravitinoFileSystem.listStatus(gravitinoPath);
    assertEquals(1, gravitinoStatuses.length);
    gravitinoFileSystem.delete(gravitinoFilePath, true);
    assertFalse(gravitinoFileSystem.exists(gravitinoFilePath));
    assertEquals(
        mockStatuses[0].getPath().toString(),
        gravitinoStatuses[0]
            .getPath()
            .toString()
            .replaceFirst(
                GravitinoVirtualFileSystemConfiguration.GVFS_FILESET_PREFIX,
                hadoopFileSystem.getScheme() + "://" + hadoopFileSystem.getUri().getHost()));
  }

  @Test
  public void testMkdir() throws IOException {
    Path diffMkdirPath =
        FileSystemTestUtils.createGvfsPrefix(
            metalakeName, catalogName, schemaName, "fileset_mkdir");
    mockFilesetDTO(
        metalakeName,
        catalogName,
        schemaName,
        "fileset_mkdir",
        FileSystemTestUtils.createHdfsPrefix(metalakeName, catalogName, schemaName, "fileset_mkdir")
            .toString());
    gravitinoFileSystem.mkdirs(diffMkdirPath, HdfsMiniClusterTestBase.fsPermission());
    assertTrue(gravitinoFileSystem.exists(diffMkdirPath));

    hadoopFileSystem.mkdirs(hdfsPath);
    assertTrue(hadoopFileSystem.exists(hdfsPath));
    assertTrue(gravitinoFileSystem.exists(gravitinoPath));
    FileStatus mockStatus = hadoopFileSystem.getFileStatus(hdfsPath);
    hadoopFileSystem.delete(hdfsPath, true);

    gravitinoFileSystem.mkdirs(gravitinoPath);
    assertTrue(gravitinoFileSystem.exists(gravitinoPath));
    FileStatus gravitinoStatus = gravitinoFileSystem.getFileStatus(gravitinoPath);
    gravitinoFileSystem.delete(gravitinoPath, true);
    assertEquals(
        mockStatus.getPath().toString(),
        gravitinoStatus
            .getPath()
            .toString()
            .replaceFirst(
                GravitinoVirtualFileSystemConfiguration.GVFS_FILESET_PREFIX,
                hadoopFileSystem.getScheme() + "://" + hadoopFileSystem.getUri().getHost()));
  }
}
