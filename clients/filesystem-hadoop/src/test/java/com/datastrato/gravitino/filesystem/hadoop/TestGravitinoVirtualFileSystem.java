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
import java.nio.charset.StandardCharsets;
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
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

public class TestGravitinoVirtualFileSystem extends MockServerTestBase {
  private static final String GVFS_IMPL_CLASS = GravitinoVirtualFileSystem.class.getName();
  private static final String GVFS_ABSTRACT_IMPL_CLASS = Gvfs.class.getName();

  private FileSystem localFileSystem = null;
  private FileSystem gravitinoFileSystem = null;
  private Configuration conf = null;
  private Path localPath = null;
  private Path gvfsPath = null;

  @BeforeAll
  public static void setup() {
    MockServerTestBase.setup();
  }

  @BeforeEach
  public void init() throws IOException {
    mockMetalakeDTO(metalakeName, "comment");
    mockCatalogDTO(catalogName, provider, "comment");
    localPath = FileSystemTestUtils.createLocalPrefix(catalogName, schemaName, filesetName);
    mockFilesetDTO(metalakeName, catalogName, schemaName, filesetName, localPath.toString());
    gvfsPath = FileSystemTestUtils.createGvfsPrefix(catalogName, schemaName, filesetName);

    Configuration configuration = new Configuration();
    configuration.set("fs.gvfs.impl", GVFS_IMPL_CLASS);
    configuration.set("fs.AbstractFileSystem.gvfs.impl", GVFS_ABSTRACT_IMPL_CLASS);
    configuration.set(
        GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_SERVER_URI_KEY,
        MockServerTestBase.serverUri());
    configuration.set(
        GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_METALAKE_KEY, metalakeName);
    conf = configuration;

    localFileSystem = localPath.getFileSystem(conf);
    gravitinoFileSystem = gvfsPath.getFileSystem(conf);
  }

  @AfterEach
  public void destroy() throws IOException {
    if (localFileSystem.exists(localPath)) {
      localFileSystem.delete(localPath, true);
    }
    Path catalogLocalPath = new Path("file:/tmp/" + catalogName);
    if (localFileSystem.exists(catalogLocalPath)) {
      localFileSystem.delete(catalogLocalPath, true);
    }
    if (gravitinoFileSystem.exists(gvfsPath)) {
      gravitinoFileSystem.delete(gvfsPath, true);
    }
  }

  @Test
  public void testFSCache() throws IOException {
    Configuration conf1 = localFileSystem.getConf();
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
    Path localPath = FileSystemTestUtils.createLocalPrefix(catalogName, schemaName, anotherFileset);

    mockFilesetDTO(metalakeName, catalogName, schemaName, anotherFileset, localPath.toString());

    Path diffGvfsPath =
        FileSystemTestUtils.createGvfsPrefix(catalogName, schemaName, anotherFileset);
    FileSystem anotherGvfs = diffGvfsPath.getFileSystem(conf);
    assertNotEquals(anotherGvfs, gravitinoFileSystem);

    // test proxyed actual local, should not get the same fs
    FileSystem proxyLocal =
        Objects.requireNonNull(
                ((GravitinoVirtualFileSystem) gravitinoFileSystem)
                    .getFilesetCache()
                    .getIfPresent(
                        NameIdentifier.of(metalakeName, catalogName, schemaName, filesetName)))
            .getFileSystem();

    Path diffLocalPath =
        FileSystemTestUtils.createLocalPrefix(catalogName, schemaName, anotherFileset);
    FileSystem local = diffLocalPath.getFileSystem(conf);

    assertNotEquals(local, proxyLocal);
  }

  @Test
  public void testCreate() throws IOException {
    FileSystemTestUtils.create(gvfsPath, gravitinoFileSystem);
    assertTrue(gravitinoFileSystem.exists(gvfsPath));
    gravitinoFileSystem.delete(gvfsPath, true);

    // mock the unverified fileset with diff name
    String unverifiedCreateFilesetName = "fileset_create_unverified";
    Path unverifiedCreateFilesetPath =
        FileSystemTestUtils.createGvfsPrefix(catalogName, schemaName, unverifiedCreateFilesetName);
    assertThrows(
        RuntimeException.class,
        () -> FileSystemTestUtils.create(unverifiedCreateFilesetPath, gravitinoFileSystem));

    // mock the not correct prefix path
    Path localPrefixPath = FileSystemTestUtils.createLocalPrefix(catalogName, schemaName, "test");
    assertThrows(
        RuntimeException.class,
        () -> FileSystemTestUtils.create(localPrefixPath, gravitinoFileSystem));

    // test fileset mount the single file
    Path localSingleFilePath =
        FileSystemTestUtils.createLocalFilePath(catalogName + "/files/test.txt");
    Path filesetMountPath =
        FileSystemTestUtils.createGvfsPrefix(catalogName, schemaName, "fileset_single_file");
    mockFilesetDTO(
        metalakeName,
        catalogName,
        schemaName,
        "fileset_single_file",
        localSingleFilePath.toString());
    FileSystemTestUtils.create(filesetMountPath, gravitinoFileSystem);
    assertTrue(gravitinoFileSystem.exists(filesetMountPath));
    assertTrue(gravitinoFileSystem.getFileStatus(filesetMountPath).isFile());
    gravitinoFileSystem.delete(filesetMountPath, true);
    assertFalse(localFileSystem.exists(localSingleFilePath));
  }

  @Test
  @Disabled("Append operation is not supported in LocalFileSystem. We can't test it now.")
  public void testAppend() throws IOException {
    // test fileset append
    Path appendFile = new Path(gvfsPath + "/test.txt");
    FileSystemTestUtils.create(appendFile, gravitinoFileSystem);
    FileSystemTestUtils.append(appendFile, gravitinoFileSystem);
    assertTrue(gravitinoFileSystem.exists(appendFile));
    assertTrue(gravitinoFileSystem.getFileStatus(appendFile).isFile());
    assertEquals(
        "Hello, World!",
        new String(
            FileSystemTestUtils.read(appendFile, gravitinoFileSystem), StandardCharsets.UTF_8));
    gravitinoFileSystem.delete(appendFile, true);

    // mock the unverified fileset with diff name
    String unverifiedAppendFilesetName = "fileset_append_unverified";
    Path unverifiedAppendFilesetPath =
        FileSystemTestUtils.createGvfsPrefix(catalogName, schemaName, unverifiedAppendFilesetName);
    assertThrows(
        RuntimeException.class,
        () -> FileSystemTestUtils.append(unverifiedAppendFilesetPath, gravitinoFileSystem));

    // mock the not correct prefix path
    Path localPrefixPath = FileSystemTestUtils.createLocalPrefix(catalogName, schemaName, "test");
    assertThrows(
        RuntimeException.class,
        () -> FileSystemTestUtils.append(localPrefixPath, gravitinoFileSystem));

    // test fileset mount the single file
    Path localSingleFilePath =
        FileSystemTestUtils.createLocalFilePath(catalogName + "/files/test.txt");
    Path filesetMountPath =
        FileSystemTestUtils.createGvfsPrefix(catalogName, schemaName, "fileset_single_file");
    mockFilesetDTO(
        metalakeName,
        catalogName,
        schemaName,
        "fileset_single_file",
        localSingleFilePath.toString());
    FileSystemTestUtils.create(filesetMountPath, gravitinoFileSystem);
    FileSystemTestUtils.append(filesetMountPath, gravitinoFileSystem);
    assertTrue(gravitinoFileSystem.exists(filesetMountPath));
    assertTrue(gravitinoFileSystem.getFileStatus(filesetMountPath).isFile());
    gravitinoFileSystem.delete(filesetMountPath, true);
    assertFalse(localFileSystem.exists(localSingleFilePath));
  }

  @Test
  public void testRename() throws IOException {
    Path srcRenamePath = new Path(gvfsPath + "/rename_src");
    gravitinoFileSystem.mkdirs(srcRenamePath);
    assertTrue(gravitinoFileSystem.getFileStatus(srcRenamePath).isDirectory());
    assertTrue(gravitinoFileSystem.exists(srcRenamePath));

    // cannot rename the identifier
    Path dstRenamePath1 =
        FileSystemTestUtils.createGvfsPrefix(catalogName, schemaName, "rename_dst1");
    assertThrows(
        RuntimeException.class, () -> gravitinoFileSystem.rename(srcRenamePath, dstRenamePath1));

    Path dstRenamePath2 = new Path(gvfsPath.toString() + "/rename_dst2");
    gravitinoFileSystem.rename(srcRenamePath, dstRenamePath2);
    assertFalse(gravitinoFileSystem.exists(srcRenamePath));
    assertTrue(gravitinoFileSystem.exists(dstRenamePath2));
    gravitinoFileSystem.delete(dstRenamePath2, true);

    // test invalid src path
    Path invalidSrcPath =
        FileSystemTestUtils.createGvfsPrefix(catalogName, schemaName, "invalid_src_name");
    Path validDstPath = FileSystemTestUtils.createGvfsPrefix(catalogName, schemaName, filesetName);
    assertThrows(
        RuntimeException.class, () -> gravitinoFileSystem.rename(invalidSrcPath, validDstPath));

    // test invalid dst path
    Path invalidDstPath =
        FileSystemTestUtils.createGvfsPrefix(catalogName, schemaName, "invalid_dst_name");
    assertThrows(
        RuntimeException.class, () -> gravitinoFileSystem.rename(gvfsPath, invalidDstPath));

    // test fileset mount the single file
    Path localSingleFilePath =
        FileSystemTestUtils.createLocalFilePath(catalogName + "/files/test.txt");
    FileSystemTestUtils.create(localSingleFilePath, localFileSystem);
    assertTrue(localFileSystem.exists(localSingleFilePath));
    assertTrue(localFileSystem.getFileStatus(localSingleFilePath).isFile());

    Path filesetPath =
        FileSystemTestUtils.createGvfsPrefix(catalogName, schemaName, "fileset_tst1");
    mockFilesetDTO(
        metalakeName, catalogName, schemaName, "fileset_tst1", localSingleFilePath.toString());
    FileSystemTestUtils.create(filesetPath, gravitinoFileSystem);
    assertTrue(gravitinoFileSystem.exists(filesetPath));
    assertTrue(gravitinoFileSystem.getFileStatus(filesetPath).isFile());
    Path dstPath = new Path(filesetPath + "/files/test1.txt");
    assertThrows(RuntimeException.class, () -> gravitinoFileSystem.rename(filesetPath, dstPath));
    localFileSystem.delete(localSingleFilePath, true);
    assertFalse(localFileSystem.exists(localSingleFilePath));
  }

  @Test
  public void testDelete() throws IOException {
    FileSystemTestUtils.create(gvfsPath, gravitinoFileSystem);
    gravitinoFileSystem.delete(gvfsPath, true);
    assertFalse(gravitinoFileSystem.exists(gvfsPath));

    // mock the unverified fileset with diff name
    String unverifiedDeleteFilesetName = "fileset_append_unverified";
    Path unverifiedDeleteFilesetPath =
        FileSystemTestUtils.createGvfsPrefix(catalogName, schemaName, unverifiedDeleteFilesetName);
    assertThrows(
        RuntimeException.class,
        () -> gravitinoFileSystem.delete(unverifiedDeleteFilesetPath, true));

    // mock the not correct prefix path
    Path localPrefixPath = FileSystemTestUtils.createLocalPrefix(catalogName, schemaName, "test");
    assertThrows(RuntimeException.class, () -> gravitinoFileSystem.delete(localPrefixPath, true));

    // test fileset mount the single file
    Path localSingleFilePath =
        FileSystemTestUtils.createLocalFilePath(catalogName + "/files/test.txt");
    Path filesetMountPath =
        FileSystemTestUtils.createGvfsPrefix(catalogName, schemaName, "fileset_single_file");
    mockFilesetDTO(
        metalakeName,
        catalogName,
        schemaName,
        "fileset_single_file",
        localSingleFilePath.toString());
    FileSystemTestUtils.create(filesetMountPath, gravitinoFileSystem);
    assertTrue(gravitinoFileSystem.exists(filesetMountPath));
    gravitinoFileSystem.delete(filesetMountPath, true);
    assertFalse(gravitinoFileSystem.exists(filesetMountPath));
    assertFalse(localFileSystem.exists(localSingleFilePath));
  }

  @Test
  public void testGetStatus() throws IOException {
    FileSystemTestUtils.create(gvfsPath, gravitinoFileSystem);
    assertTrue(gravitinoFileSystem.exists(gvfsPath));
    assertTrue(localFileSystem.exists(localPath));
    FileStatus gravitinoStatuses = gravitinoFileSystem.getFileStatus(gvfsPath);
    FileStatus localStatuses = localFileSystem.getFileStatus(localPath);
    gravitinoFileSystem.delete(gvfsPath, true);
    assertFalse(gravitinoFileSystem.exists(gvfsPath));
    assertEquals(
        localStatuses.getPath().toString(),
        gravitinoStatuses
            .getPath()
            .toString()
            .replaceFirst(
                GravitinoVirtualFileSystemConfiguration.GVFS_FILESET_PREFIX,
                localFileSystem.getScheme() + ":/tmp/"));
  }

  @Test
  public void testListStatus() throws IOException {
    FileSystemTestUtils.mkdirs(gvfsPath, gravitinoFileSystem);
    assertTrue(gravitinoFileSystem.exists(gvfsPath));
    assertTrue(gravitinoFileSystem.getFileStatus(gvfsPath).isDirectory());
    assertTrue(localFileSystem.exists(localPath));

    for (int i = 0; i < 5; i++) {
      Path subPath = new Path(gvfsPath + "/sub" + i);
      FileSystemTestUtils.mkdirs(subPath, gravitinoFileSystem);
      assertTrue(gravitinoFileSystem.exists(subPath));
      assertTrue(gravitinoFileSystem.getFileStatus(subPath).isDirectory());
    }

    List<FileStatus> gravitinoStatuses =
        new ArrayList<>(Arrays.asList(gravitinoFileSystem.listStatus(gvfsPath)));
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
                  localFileSystem.getScheme() + ":/tmp/"));
      gravitinoFileSystem.delete(gravitinoStatuses.get(i).getPath(), true);
    }
  }

  @Test
  public void testMkdirs() throws IOException {
    FileSystemTestUtils.mkdirs(gvfsPath, gravitinoFileSystem);
    assertTrue(gravitinoFileSystem.exists(gvfsPath));
    assertTrue(gravitinoFileSystem.getFileStatus(gvfsPath).isDirectory());

    FileStatus gravitinoStatus = gravitinoFileSystem.getFileStatus(gvfsPath);
    FileStatus localStatus = localFileSystem.getFileStatus(localPath);
    gravitinoFileSystem.delete(gvfsPath, true);
    assertFalse(gravitinoFileSystem.exists(gvfsPath));

    assertEquals(
        localStatus.getPath().toString(),
        gravitinoStatus
            .getPath()
            .toString()
            .replaceFirst(
                GravitinoVirtualFileSystemConfiguration.GVFS_FILESET_PREFIX,
                localFileSystem.getScheme() + ":/tmp/"));
  }
}
