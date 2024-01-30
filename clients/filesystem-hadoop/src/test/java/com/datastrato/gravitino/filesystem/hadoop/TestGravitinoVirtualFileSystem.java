/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.filesystem.hadoop;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.InvalidPathException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.util.Progressable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestGravitinoVirtualFileSystem {
  private static MiniDFSCluster HDFS_CLUSTER;
  private static final int BUFFER_SIZE = 3;
  private static final String GVFS_IMPL_CLASS = GravitinoVirtualFileSystem.class.getName();
  private static final String GVFS_ABSTRACT_IMPL_CLASS = Gvfs.class.getName();
  private static final FsPermission MOCK_PERMISSION = FsPermission.createImmutable((short) 0777);
  private static final Progressable DEFAULT_PROGRESS = () -> {};
  private FileSystem mockFileSystem = null;
  private FileSystem gravitinoFileSystem = null;
  private Configuration conf = null;
  private Path mockPath = null;
  private Path gravitinoPath = null;

  @BeforeAll
  public static void setup() throws IOException {
    Configuration hdfsConf = new Configuration();
    HDFS_CLUSTER = new MiniDFSCluster.Builder(hdfsConf).nameNodePort(8020).numDataNodes(1).build();
  }

  @AfterAll
  public static void teardown() {
    if (HDFS_CLUSTER != null) {
      HDFS_CLUSTER.shutdown();
    }
  }

  @BeforeEach
  public void init() throws IOException {
    Configuration configuration = HDFS_CLUSTER.getFileSystem().getConf();
    configuration.set("fs.gvfs.impl", GVFS_IMPL_CLASS);
    configuration.set("fs.AbstractFileSystem.gvfs.impl", GVFS_ABSTRACT_IMPL_CLASS);
    conf = configuration;
    mockPath = new Path("hdfs://localhost/metalake_1/fileset_catalog_1/schema_1/fileset_test/xxx");
    gravitinoPath =
        new Path("gvfs://fileset/metalake_1/fileset_catalog_1/schema_1/fileset_test/xxx");
    mockFileSystem = HDFS_CLUSTER.getFileSystem();
    gravitinoFileSystem = gravitinoPath.getFileSystem(conf);
  }

  @AfterEach
  public void destroy() throws IOException {
    if (mockFileSystem.exists(mockPath)) {
      mockFileSystem.delete(mockPath, true);
    }
    if (gravitinoFileSystem.exists(gravitinoPath)) {
      gravitinoFileSystem.delete(gravitinoPath, true);
    }
  }

  @Test
  public void testCloseFSCache() throws IOException {
    Path diffPath = new Path("gvfs://fileset/metalake_1/fileset_catalog_1/schema_1/fileset_3");
    FileSystem fs = diffPath.getFileSystem(conf);
    assertNotEquals(fs, gravitinoFileSystem);
  }

  @Test
  public void testDiffPathOperation() throws IOException {
    // create
    Path diffCreatePath =
        new Path("gvfs://fileset/metalake_1/fileset_catalog_1/schema_1/fileset_create");
    assertThrows(InvalidPathException.class, () -> create(diffCreatePath, gravitinoFileSystem));

    // append
    Path diffAppendPath =
        new Path("gvfs://fileset/metalake_1/fileset_catalog_1/schema_1/fileset_append");
    assertThrows(InvalidPathException.class, () -> append(diffAppendPath, gravitinoFileSystem));

    // rename
    create(gravitinoPath, gravitinoFileSystem);
    assertTrue(gravitinoFileSystem.exists(gravitinoPath));
    Path renamePath =
        new Path("gvfs://fileset/metalake_1/fileset_catalog_1/schema_1/fileset_diffRename");
    assertThrows(
        InvalidPathException.class, () -> gravitinoFileSystem.rename(gravitinoPath, renamePath));
    gravitinoFileSystem.delete(gravitinoPath, true);

    // delete
    Path diffDeletePath =
        new Path("gvfs://fileset/metalake_1/fileset_catalog_1/schema_1/fileset_delete");
    FileSystem fs1 = diffDeletePath.getFileSystem(conf);
    create(diffDeletePath, fs1);
    assertNotEquals(fs1, gravitinoFileSystem);
    assertThrows(
        InvalidPathException.class, () -> gravitinoFileSystem.delete(diffDeletePath, true));
    fs1.delete(diffDeletePath, true);

    // list status
    Path diffStatusPath =
        new Path("gvfs://fileset/metalake_1/fileset_catalog_1/schema_1/fileset_status");
    FileSystem fs2 = diffStatusPath.getFileSystem(conf);
    create(diffStatusPath, fs2);
    assertNotEquals(fs2, gravitinoFileSystem);
    assertThrows(InvalidPathException.class, () -> gravitinoFileSystem.listStatus(diffStatusPath));
    fs2.delete(diffStatusPath, true);

    // mkdir
    Path diffMkdirPath =
        new Path("gvfs://fileset/metalake_1/fileset_catalog_1/schema_1/fileset_mkdir");
    assertThrows(
        InvalidPathException.class,
        () -> gravitinoFileSystem.mkdirs(diffMkdirPath, MOCK_PERMISSION));
  }

  @Test
  public void testCreate() throws IOException {
    Path mockFilePath = new Path(mockPath.toString() + "/append.txt");
    Path gravitinoFilePath = new Path(gravitinoPath.toString() + "/append.txt");
    create(mockFilePath, mockFileSystem);
    assertTrue(mockFileSystem.exists(mockFilePath));
    FileStatus mockStatus = mockFileSystem.getFileStatus(mockFilePath);
    assertTrue(gravitinoFileSystem.exists(gravitinoFilePath));
    mockFileSystem.delete(mockFilePath, true);

    create(gravitinoFilePath, gravitinoFileSystem);
    assertTrue(gravitinoFileSystem.exists(gravitinoFilePath));
    FileStatus gravitinoFileStatus = gravitinoFileSystem.getFileStatus(gravitinoFilePath);
    gravitinoFileSystem.delete(gravitinoFilePath, true);

    assertEquals(
        mockStatus.getPath().toString(),
        gravitinoFileStatus
            .getPath()
            .toString()
            .replaceFirst(
                GravitinoVirtualFileSystemConfiguration.GVFS_FILESET_PREFIX,
                mockFileSystem.getScheme() + "://" + mockFileSystem.getUri().getHost()));
  }

  @Test
  public void testAppend() throws IOException {
    Path mockFilePath = new Path(mockPath.toString() + "/append.txt");
    Path gravitinoFilePath = new Path(gravitinoPath.toString() + "/append.txt");
    create(mockFilePath, mockFileSystem);
    append(mockFilePath, mockFileSystem);
    assertTrue(mockFileSystem.exists(mockFilePath));
    FileStatus mockFileStatus = mockFileSystem.getFileStatus(mockFilePath);
    byte[] mockInputContent = read(mockFilePath, mockFileSystem);
    assertEquals(new String(mockInputContent), "Hello, World!");
    assertTrue(gravitinoFileSystem.exists(gravitinoFilePath));
    mockFileSystem.delete(mockFilePath, true);

    create(gravitinoFilePath, gravitinoFileSystem);
    append(gravitinoFilePath, gravitinoFileSystem);
    assertTrue(gravitinoFileSystem.exists(gravitinoFilePath));
    FileStatus gravitinoFileStatus = gravitinoFileSystem.getFileStatus(gravitinoFilePath);
    byte[] gravitinoInputContent = read(gravitinoFilePath, gravitinoFileSystem);
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
                mockFileSystem.getScheme() + "://" + mockFileSystem.getUri().getHost()));
  }

  @Test
  public void testRename() throws IOException {
    Path mockFilePath = new Path(mockPath.toString() + "/append.txt");
    Path gravitinoFilePath = new Path(gravitinoPath.toString() + "/append.txt");
    create(mockFilePath, mockFileSystem);
    assertTrue(mockFileSystem.exists(mockFilePath));
    assertTrue(gravitinoFileSystem.exists(gravitinoFilePath));

    Path renameMockFilePath = new Path(mockPath.toString() + "/append1.txt");
    mockFileSystem.rename(mockFilePath, renameMockFilePath);
    assertFalse(mockFileSystem.exists(mockFilePath));
    assertTrue(mockFileSystem.exists(renameMockFilePath));
    FileStatus renameFileStatus = mockFileSystem.getFileStatus(renameMockFilePath);
    mockFileSystem.delete(renameMockFilePath, true);

    create(gravitinoFilePath, gravitinoFileSystem);
    assertTrue(gravitinoFileSystem.exists(gravitinoFilePath));
    Path renameGravitinoFilePath = new Path(gravitinoPath.toString() + "/append1.txt");
    gravitinoFileSystem.rename(gravitinoFilePath, renameGravitinoFilePath);
    assertFalse(gravitinoFileSystem.exists(gravitinoFilePath));
    assertTrue(gravitinoFileSystem.exists(renameGravitinoFilePath));
    FileStatus renameGravitinoFileStatus =
        gravitinoFileSystem.getFileStatus(renameGravitinoFilePath);
    gravitinoFileSystem.delete(renameGravitinoFilePath, true);

    assertEquals(
        renameFileStatus.getPath().toString(),
        renameGravitinoFileStatus
            .getPath()
            .toString()
            .replaceFirst(
                GravitinoVirtualFileSystemConfiguration.GVFS_FILESET_PREFIX,
                mockFileSystem.getScheme() + "://" + mockFileSystem.getUri().getHost()));
  }

  @Test
  public void testDelete() throws IOException {
    Path mockFilePath = new Path(mockPath.toString() + "/testDelete.txt");
    Path gravitinoFilePath = new Path(gravitinoPath.toString() + "/testDelete.txt");
    create(mockFilePath, mockFileSystem);
    assertTrue(mockFileSystem.exists(mockFilePath));
    assertTrue(gravitinoFileSystem.exists(gravitinoFilePath));
    mockFileSystem.delete(mockFilePath, true);
    assertFalse(mockFileSystem.exists(mockFilePath));
    assertFalse(gravitinoFileSystem.exists(gravitinoFilePath));

    create(gravitinoFilePath, gravitinoFileSystem);
    assertTrue(gravitinoFileSystem.exists(gravitinoFilePath));
    gravitinoFileSystem.delete(gravitinoFilePath, true);
    assertFalse(gravitinoFileSystem.exists(gravitinoFilePath));
  }

  @Test
  public void testGetStatus() throws IOException {
    Path mockFilePath = new Path(mockPath.toString() + "/testGet.txt");
    Path gravitinoFilePath = new Path(gravitinoPath.toString() + "/testGet.txt");
    create(mockFilePath, mockFileSystem);
    assertTrue(mockFileSystem.exists(mockFilePath));
    assertTrue(gravitinoFileSystem.exists(gravitinoFilePath));
    FileStatus mockStatuses = mockFileSystem.getFileStatus(mockPath);
    mockFileSystem.delete(mockFilePath, true);
    assertFalse(mockFileSystem.exists(mockFilePath));
    assertFalse(gravitinoFileSystem.exists(gravitinoFilePath));

    create(gravitinoFilePath, gravitinoFileSystem);
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
                mockFileSystem.getScheme() + "://" + mockFileSystem.getUri().getHost()));
  }

  @Test
  public void testListStatus() throws IOException {
    Path mockFilePath = new Path(mockPath.toString() + "/testList.txt");
    Path gravitinoFilePath = new Path(gravitinoPath.toString() + "/testList.txt");
    create(mockFilePath, mockFileSystem);
    assertTrue(mockFileSystem.exists(mockFilePath));
    assertTrue(gravitinoFileSystem.exists(gravitinoFilePath));
    FileStatus[] mockStatuses = mockFileSystem.listStatus(mockPath);
    assertEquals(1, mockStatuses.length);
    mockFileSystem.delete(mockFilePath, true);
    assertFalse(mockFileSystem.exists(mockFilePath));
    assertFalse(gravitinoFileSystem.exists(gravitinoFilePath));

    create(gravitinoFilePath, gravitinoFileSystem);
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
                mockFileSystem.getScheme() + "://" + mockFileSystem.getUri().getHost()));
  }

  @Test
  public void testMkdir() throws IOException {
    mockFileSystem.mkdirs(mockPath);
    assertTrue(mockFileSystem.exists(mockPath));
    assertTrue(gravitinoFileSystem.exists(gravitinoPath));
    FileStatus mockStatus = mockFileSystem.getFileStatus(mockPath);
    mockFileSystem.delete(mockPath, true);

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
                mockFileSystem.getScheme() + "://" + mockFileSystem.getUri().getHost()));
  }

  private void create(Path path, FileSystem fileSystem) throws IOException {
    short replication = 1;
    long blockSize = 1048576L;
    boolean overwrite = true;
    FSDataOutputStream outputStream =
        fileSystem.create(
            path,
            MOCK_PERMISSION,
            overwrite,
            BUFFER_SIZE,
            replication,
            blockSize,
            DEFAULT_PROGRESS);
    outputStream.close();
  }

  private void append(Path path, FileSystem fileSystem) throws IOException {
    FSDataOutputStream mockOutputStream = fileSystem.append(path, BUFFER_SIZE);
    // Hello, World!
    byte[] mockBytes = new byte[] {72, 101, 108, 108, 111, 44, 32, 87, 111, 114, 108, 100, 33};
    mockOutputStream.write(mockBytes);
    mockOutputStream.close();
  }

  private byte[] read(Path path, FileSystem fileSystem) throws IOException {
    FSDataInputStream inputStream = fileSystem.open(path, BUFFER_SIZE);
    int bytesRead;
    byte[] buffer = new byte[1024];
    ByteArrayOutputStream byteOutputStream = new ByteArrayOutputStream();
    while ((bytesRead = inputStream.read(buffer)) != -1) {
      byteOutputStream.write(buffer, 0, bytesRead);
    }
    return byteOutputStream.toByteArray();
  }
}
