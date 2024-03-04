/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.filesystem.hadoop;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.filesystem.hadoop.utils.FileSystemTestUtils;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
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
  private Path gvfsPath = null;

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

    gvfsPath =
        FileSystemTestUtils.createGvfsPrefix(metalakeName, catalogName, schemaName, filesetName);
    hadoopFileSystem = HdfsMiniClusterTestBase.hadoopFileSystem();
    gravitinoFileSystem = gvfsPath.getFileSystem(conf);
  }

  @AfterEach
  public void destroy() throws IOException {
    if (hadoopFileSystem.exists(hdfsPath)) {
      hadoopFileSystem.delete(hdfsPath, true);
    }
    if (gravitinoFileSystem.exists(gvfsPath)) {
      gravitinoFileSystem.delete(gvfsPath, true);
    }
  }

  @Test
  public void testFSCache() throws IOException {
    Configuration conf1 = HdfsMiniClusterTestBase.hadoopFileSystem().getConf();
    assertEquals(
        "true",
        conf1.get(
            String.format(
                "fs.%s.impl.disable.cache", GravitinoVirtualFileSystemConfiguration.GVFS_SCHEME)));

    Configuration conf2 = gravitinoFileSystem.getConf();
    assertEquals(
        "true",
        conf2.get(
            String.format(
                "fs.%s.impl.disable.cache", GravitinoVirtualFileSystemConfiguration.GVFS_SCHEME)));

    // test gvfs, should not get the same fs
    String anotherFileset = "fileset_2";
    Path hdfsPath =
        FileSystemTestUtils.createHdfsPrefix(metalakeName, catalogName, schemaName, anotherFileset);

    mockFilesetDTO(metalakeName, catalogName, schemaName, anotherFileset, hdfsPath.toString());

    Path diffGvfsPath =
        FileSystemTestUtils.createGvfsPrefix(metalakeName, catalogName, schemaName, anotherFileset);
    FileSystem anotherGvfs = diffGvfsPath.getFileSystem(conf);
    assertNotEquals(anotherGvfs, gravitinoFileSystem);

    // test proxy hdfs, should not get the same fs
    FileSystem proxyHdfs =
        Objects.requireNonNull(
                ((GravitinoVirtualFileSystem) gravitinoFileSystem)
                    .getFilesetCache()
                    .getIfPresent(
                        NameIdentifier.of(metalakeName, catalogName, schemaName, filesetName)))
            .getFileSystem();

    Path diffHdfsPath =
        FileSystemTestUtils.createHdfsPrefix(metalakeName, catalogName, schemaName, anotherFileset);
    FileSystem hdfs = diffHdfsPath.getFileSystem(conf);

    assertNotEquals(hdfs, proxyHdfs);
  }

  @Test
  public void testCreate() throws IOException {
    FileSystemTestUtils.create(gvfsPath, gravitinoFileSystem);
    assertTrue(gravitinoFileSystem.exists(gvfsPath));
    gravitinoFileSystem.delete(gvfsPath, true);

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

    // test fileset mount the single file
    Path hdfsSingleFilePath = FileSystemTestUtils.createHdfsFilePath("/files/test.txt");
    Path filesetMountPath =
        FileSystemTestUtils.createGvfsPrefix(
            metalakeName, catalogName, schemaName, "fileset_single_file");
    mockFilesetDTO(
        metalakeName,
        catalogName,
        schemaName,
        "fileset_single_file",
        hdfsSingleFilePath.toString());
    FileSystemTestUtils.create(filesetMountPath, gravitinoFileSystem);
    assertTrue(gravitinoFileSystem.exists(filesetMountPath));
    assertTrue(gravitinoFileSystem.isFile(filesetMountPath));
    gravitinoFileSystem.delete(filesetMountPath, true);
  }

  @Test
  public void testAppend() throws IOException {
    // test fileset append
    Path appendFile = new Path(gvfsPath + "/test.txt");
    FileSystemTestUtils.create(appendFile, gravitinoFileSystem);
    FileSystemTestUtils.append(appendFile, gravitinoFileSystem);
    assertTrue(gravitinoFileSystem.exists(appendFile));
    assertTrue(gravitinoFileSystem.isFile(appendFile));
    gravitinoFileSystem.delete(appendFile, true);

    // mock the unverified fileset with diff name
    String unverifiedAppendFilesetName = "fileset_append_unverified";
    Path unverifiedAppendFilesetPath =
        FileSystemTestUtils.createGvfsPrefix(
            metalakeName, catalogName, schemaName, unverifiedAppendFilesetName);
    assertThrows(
        RuntimeException.class,
        () -> FileSystemTestUtils.append(unverifiedAppendFilesetPath, gravitinoFileSystem));

    // mock the not correct prefix path
    Path hdfsPrefixPath =
        FileSystemTestUtils.createHdfsPrefix(metalakeName, catalogName, schemaName, "test");
    assertThrows(
        RuntimeException.class,
        () -> FileSystemTestUtils.append(hdfsPrefixPath, gravitinoFileSystem));

    // test fileset mount the single file
    Path hdfsSingleFilePath = FileSystemTestUtils.createHdfsFilePath("/files/test.txt");
    Path filesetMountPath =
        FileSystemTestUtils.createGvfsPrefix(
            metalakeName, catalogName, schemaName, "fileset_single_file");
    mockFilesetDTO(
        metalakeName,
        catalogName,
        schemaName,
        "fileset_single_file",
        hdfsSingleFilePath.toString());
    FileSystemTestUtils.create(filesetMountPath, gravitinoFileSystem);
    FileSystemTestUtils.append(filesetMountPath, gravitinoFileSystem);
    assertTrue(gravitinoFileSystem.exists(filesetMountPath));
    assertTrue(gravitinoFileSystem.isFile(filesetMountPath));
    gravitinoFileSystem.delete(filesetMountPath, true);
  }

  @Test
  public void testRename() throws IOException {
    Path srcRenamePath = new Path(gvfsPath + "/rename_src");
    gravitinoFileSystem.mkdirs(srcRenamePath);
    assertTrue(gravitinoFileSystem.isDirectory(srcRenamePath));
    assertTrue(gravitinoFileSystem.exists(srcRenamePath));

    // cannot rename the identifier
    Path dstRenamePath1 =
        FileSystemTestUtils.createGvfsPrefix(metalakeName, catalogName, schemaName, "rename_dst1");
    assertThrows(
        RuntimeException.class, () -> gravitinoFileSystem.rename(srcRenamePath, dstRenamePath1));

    Path dstRenamePath2 = new Path(gvfsPath.toString() + "/rename_dst2");
    gravitinoFileSystem.rename(srcRenamePath, dstRenamePath2);
    assertFalse(gravitinoFileSystem.exists(srcRenamePath));
    assertTrue(gravitinoFileSystem.exists(dstRenamePath2));
    gravitinoFileSystem.delete(dstRenamePath2, true);

    // test invalid src path
    Path invalidSrcPath =
        FileSystemTestUtils.createGvfsPrefix(
            metalakeName, catalogName, schemaName, "invalid_src_name");
    Path validDstPath =
        FileSystemTestUtils.createGvfsPrefix(metalakeName, catalogName, schemaName, filesetName);
    assertThrows(
        RuntimeException.class, () -> gravitinoFileSystem.rename(invalidSrcPath, validDstPath));

    // test invalid dst path
    Path invalidDstPath =
        FileSystemTestUtils.createGvfsPrefix(
            metalakeName, catalogName, schemaName, "invalid_dst_name");
    assertThrows(
        RuntimeException.class, () -> gravitinoFileSystem.rename(gvfsPath, invalidDstPath));

    // test fileset mount the single file
    Path hdfsSingleFilePath = FileSystemTestUtils.createHdfsFilePath("/files/test.txt");
    FileSystemTestUtils.create(hdfsSingleFilePath, hadoopFileSystem);
    FileSystemTestUtils.append(hdfsSingleFilePath, hadoopFileSystem);
    assertTrue(hadoopFileSystem.exists(hdfsSingleFilePath));
    assertTrue(hadoopFileSystem.isFile(hdfsSingleFilePath));

    Path filesetPath =
        FileSystemTestUtils.createGvfsPrefix(metalakeName, catalogName, schemaName, "fileset_tst1");
    mockFilesetDTO(
        metalakeName, catalogName, schemaName, "fileset_tst1", hdfsSingleFilePath.toString());
    FileSystemTestUtils.create(filesetPath, gravitinoFileSystem);
    assertTrue(gravitinoFileSystem.exists(filesetPath));
    assertTrue(gravitinoFileSystem.isFile(filesetPath));
    Path dstPath = new Path(filesetPath + "/files/test1.txt");
    assertThrows(RuntimeException.class, () -> gravitinoFileSystem.rename(filesetPath, dstPath));
  }

  @Test
  public void testDelete() throws IOException {
    FileSystemTestUtils.create(gvfsPath, gravitinoFileSystem);
    gravitinoFileSystem.delete(gvfsPath, true);
    assertFalse(gravitinoFileSystem.exists(gvfsPath));

    // mock the unverified fileset with diff name
    String unverifiedDeleteFilesetName = "fileset_append_unverified";
    Path unverifiedDeleteFilesetPath =
        FileSystemTestUtils.createGvfsPrefix(
            metalakeName, catalogName, schemaName, unverifiedDeleteFilesetName);
    assertThrows(
        RuntimeException.class,
        () -> gravitinoFileSystem.delete(unverifiedDeleteFilesetPath, true));

    // mock the not correct prefix path
    Path hdfsPrefixPath =
        FileSystemTestUtils.createHdfsPrefix(metalakeName, catalogName, schemaName, "test");
    assertThrows(RuntimeException.class, () -> gravitinoFileSystem.delete(hdfsPrefixPath, true));

    // test fileset mount the single file
    Path hdfsSingleFilePath = FileSystemTestUtils.createHdfsFilePath("/files/test.txt");
    Path filesetMountPath =
        FileSystemTestUtils.createGvfsPrefix(
            metalakeName, catalogName, schemaName, "fileset_single_file");
    mockFilesetDTO(
        metalakeName,
        catalogName,
        schemaName,
        "fileset_single_file",
        hdfsSingleFilePath.toString());
    FileSystemTestUtils.create(filesetMountPath, gravitinoFileSystem);
    assertTrue(gravitinoFileSystem.exists(filesetMountPath));
    gravitinoFileSystem.delete(filesetMountPath, true);
    assertFalse(gravitinoFileSystem.exists(filesetMountPath));
  }

  @Test
  public void testGetStatus() throws IOException {
    FileSystemTestUtils.create(gvfsPath, gravitinoFileSystem);
    assertTrue(gravitinoFileSystem.exists(gvfsPath));
    assertTrue(hadoopFileSystem.exists(hdfsPath));
    FileStatus gravitinoStatuses = gravitinoFileSystem.getFileStatus(gvfsPath);
    FileStatus hdfsStatuses = hadoopFileSystem.getFileStatus(hdfsPath);
    gravitinoFileSystem.delete(gvfsPath, true);
    assertFalse(gravitinoFileSystem.exists(gvfsPath));
    assertEquals(
        hdfsStatuses.getPath().toString(),
        gravitinoStatuses
            .getPath()
            .toString()
            .replaceFirst(
                GravitinoVirtualFileSystemConfiguration.GVFS_FILESET_PREFIX,
                hadoopFileSystem.getScheme() + "://" + hadoopFileSystem.getUri().getHost() + "/"));
  }

  @Test
  public void testListStatus() throws IOException {
    gravitinoFileSystem.mkdirs(gvfsPath, HdfsMiniClusterTestBase.fsPermission());
    assertTrue(gravitinoFileSystem.exists(gvfsPath));
    assertTrue(gravitinoFileSystem.isDirectory(gvfsPath));
    assertTrue(hadoopFileSystem.exists(hdfsPath));

    for (int i = 0; i < 5; i++) {
      Path subPath = new Path(gvfsPath + "/sub" + i);
      gravitinoFileSystem.mkdirs(subPath, HdfsMiniClusterTestBase.fsPermission());
      assertTrue(gravitinoFileSystem.exists(subPath));
      assertTrue(gravitinoFileSystem.isDirectory(subPath));
    }

    List<FileStatus> gravitinoStatuses =
        new ArrayList<>(Arrays.asList(gravitinoFileSystem.listStatus(gvfsPath)));
    gravitinoStatuses.sort(Comparator.comparing(FileStatus::getPath));
    assertEquals(5, gravitinoStatuses.size());

    List<FileStatus> hdfsStatuses =
        new ArrayList<>(Arrays.asList(hadoopFileSystem.listStatus(hdfsPath)));
    hdfsStatuses.sort(Comparator.comparing(FileStatus::getPath));
    assertEquals(5, hdfsStatuses.size());

    for (int i = 0; i < 5; i++) {
      assertEquals(
          hdfsStatuses.get(i).getPath().toString(),
          gravitinoStatuses
              .get(i)
              .getPath()
              .toString()
              .replaceFirst(
                  GravitinoVirtualFileSystemConfiguration.GVFS_FILESET_PREFIX,
                  hadoopFileSystem.getScheme()
                      + "://"
                      + hadoopFileSystem.getUri().getHost()
                      + "/"));
      gravitinoFileSystem.delete(gravitinoStatuses.get(i).getPath(), true);
    }
  }

  @Test
  public void testMkdirs() throws IOException {
    gravitinoFileSystem.mkdirs(gvfsPath, HdfsMiniClusterTestBase.fsPermission());
    assertTrue(gravitinoFileSystem.exists(gvfsPath));
    assertTrue(gravitinoFileSystem.isDirectory(gvfsPath));

    FileStatus gravitinoStatus = gravitinoFileSystem.getFileStatus(gvfsPath);
    FileStatus hdfsStatus = hadoopFileSystem.getFileStatus(hdfsPath);
    gravitinoFileSystem.delete(gvfsPath, true);
    assertFalse(gravitinoFileSystem.exists(gvfsPath));

    assertEquals(
        hdfsStatus.getPath().toString(),
        gravitinoStatus
            .getPath()
            .toString()
            .replaceFirst(
                GravitinoVirtualFileSystemConfiguration.GVFS_FILESET_PREFIX,
                hadoopFileSystem.getScheme() + "://" + hadoopFileSystem.getUri().getHost() + "/"));
  }
}
