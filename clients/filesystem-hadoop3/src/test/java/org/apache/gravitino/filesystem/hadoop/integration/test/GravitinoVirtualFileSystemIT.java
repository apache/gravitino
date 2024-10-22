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
package org.apache.gravitino.filesystem.hadoop.integration.test;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.common.collect.Maps;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.file.Fileset;
import org.apache.gravitino.integration.test.container.ContainerSuite;
import org.apache.gravitino.integration.test.container.HiveContainer;
import org.apache.gravitino.integration.test.util.BaseIT;
import org.apache.gravitino.integration.test.util.GravitinoITUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tag("gravitino-docker-test")
public class GravitinoVirtualFileSystemIT extends BaseIT {
  private static final Logger LOG = LoggerFactory.getLogger(GravitinoVirtualFileSystemIT.class);
  protected static final ContainerSuite containerSuite = ContainerSuite.getInstance();
  protected String metalakeName = GravitinoITUtils.genRandomName("gvfs_it_metalake");
  protected String catalogName = GravitinoITUtils.genRandomName("catalog");
  protected String schemaName = GravitinoITUtils.genRandomName("schema");
  protected GravitinoMetalake metalake;
  protected Configuration conf = new Configuration();
  protected int defaultBockSize = 128 * 1024 * 1024;
  protected int defaultReplication = 3;

  @BeforeAll
  public void startUp() throws Exception {
    containerSuite.startHiveContainer();
    Assertions.assertFalse(client.metalakeExists(metalakeName));
    metalake = client.createMetalake(metalakeName, "metalake comment", Collections.emptyMap());
    Assertions.assertTrue(client.metalakeExists(metalakeName));

    Map<String, String> properties = Maps.newHashMap();
    Catalog catalog =
        metalake.createCatalog(
            catalogName, Catalog.Type.FILESET, "hadoop", "catalog comment", properties);
    Assertions.assertTrue(metalake.catalogExists(catalogName));

    catalog.asSchemas().createSchema(schemaName, "schema comment", properties);
    Assertions.assertTrue(catalog.asSchemas().schemaExists(schemaName));

    conf.set("fs.gvfs.impl", "org.apache.gravitino.filesystem.hadoop.GravitinoVirtualFileSystem");
    conf.set("fs.AbstractFileSystem.gvfs.impl", "org.apache.gravitino.filesystem.hadoop.Gvfs");
    conf.set("fs.gvfs.impl.disable.cache", "true");
    conf.set("fs.gravitino.server.uri", serverUri);
    conf.set("fs.gravitino.client.metalake", metalakeName);
  }

  @AfterAll
  public void tearDown() throws IOException {
    Catalog catalog = metalake.loadCatalog(catalogName);
    catalog.asSchemas().dropSchema(schemaName, true);
    metalake.dropCatalog(catalogName, true);
    client.dropMetalake(metalakeName, true);

    if (client != null) {
      client.close();
      client = null;
    }

    Path hdfsPath = new Path(baseHdfsPath());
    try (FileSystem fs = hdfsPath.getFileSystem(conf)) {
      if (fs.exists(hdfsPath)) {
        fs.delete(hdfsPath, true);
      }
    }

    try {
      closer.close();
    } catch (Exception e) {
      LOG.error("Exception in closing CloseableGroup", e);
    }
  }

  protected Configuration convertGvfsConfigToRealFileSystemConfig(Configuration gvfsConf) {
    return gvfsConf;
  }

  @Test
  public void testCreate() throws IOException {
    // create fileset
    String filesetName = GravitinoITUtils.genRandomName("test_fileset_create");
    NameIdentifier filesetIdent = NameIdentifier.of(schemaName, filesetName);
    Catalog catalog = metalake.loadCatalog(catalogName);
    String storageLocation = genStorageLocation(filesetName);
    catalog
        .asFilesetCatalog()
        .createFileset(
            filesetIdent,
            "fileset comment",
            Fileset.Type.MANAGED,
            storageLocation,
            new HashMap<>());
    Assertions.assertTrue(catalog.asFilesetCatalog().filesetExists(filesetIdent));

    // test gvfs create
    Path hdfsPath = new Path(storageLocation);
    try (FileSystem fs = hdfsPath.getFileSystem(convertGvfsConfigToRealFileSystemConfig(conf))) {
      Assertions.assertTrue(fs.exists(hdfsPath));
      Path gvfsPath = genGvfsPath(filesetName);
      try (FileSystem gvfs = gvfsPath.getFileSystem(conf)) {
        Assertions.assertTrue(gvfs.exists(gvfsPath));
        String fileName = "test.txt";
        Path createPath = new Path(gvfsPath + "/" + fileName);
        // GCS need to close the stream to create the file manually.
        gvfs.create(createPath).close();
        Assertions.assertTrue(gvfs.exists(createPath));
        Assertions.assertTrue(gvfs.getFileStatus(createPath).isFile());
        Assertions.assertTrue(fs.exists(new Path(storageLocation + "/" + fileName)));
      }
    }

    catalog.asFilesetCatalog().dropFileset(filesetIdent);
  }

  @Test
  public void testAppend() throws IOException {
    // create fileset
    String filesetName = GravitinoITUtils.genRandomName("test_fileset_append");
    NameIdentifier filesetIdent = NameIdentifier.of(schemaName, filesetName);
    Catalog catalog = metalake.loadCatalog(catalogName);
    String storageLocation = genStorageLocation(filesetName);
    catalog
        .asFilesetCatalog()
        .createFileset(
            filesetIdent,
            "fileset comment",
            Fileset.Type.MANAGED,
            storageLocation,
            new HashMap<>());
    Assertions.assertTrue(catalog.asFilesetCatalog().filesetExists(filesetIdent));

    // test gvfs append
    Path hdfsPath = new Path(storageLocation);
    try (FileSystem fs = hdfsPath.getFileSystem(convertGvfsConfigToRealFileSystemConfig(conf))) {
      Assertions.assertTrue(fs.exists(hdfsPath));
      Path gvfsPath = genGvfsPath(filesetName);
      String fileName = "test.txt";
      Path appendPath = new Path(gvfsPath + "/" + fileName);

      try (FileSystem gvfs = gvfsPath.getFileSystem(conf)) {
        Assertions.assertTrue(gvfs.exists(gvfsPath));
        gvfs.create(appendPath).close();
        Assertions.assertTrue(gvfs.exists(appendPath));
        Assertions.assertTrue(gvfs.getFileStatus(appendPath).isFile());
        Assertions.assertTrue(fs.exists(new Path(storageLocation + "/" + fileName)));
      }

      try (FileSystem gvfs = gvfsPath.getFileSystem(conf)) {
        try (FSDataOutputStream outputStream = gvfs.append(appendPath, 3)) {
          // Hello, World!
          byte[] wordsBytes =
              new byte[] {72, 101, 108, 108, 111, 44, 32, 87, 111, 114, 108, 100, 33};
          outputStream.write(wordsBytes);
        }
      }

      try (FileSystem gvfs = gvfsPath.getFileSystem(conf)) {
        try (FSDataInputStream inputStream = gvfs.open(appendPath, 3)) {
          int bytesRead;
          byte[] buffer = new byte[1024];
          try (ByteArrayOutputStream byteOutputStream = new ByteArrayOutputStream()) {
            while ((bytesRead = inputStream.read(buffer)) != -1) {
              byteOutputStream.write(buffer, 0, bytesRead);
            }
            assertEquals(
                "Hello, World!",
                new String(byteOutputStream.toByteArray(), StandardCharsets.UTF_8));
          }
        }
      }
    }

    catalog.asFilesetCatalog().dropFileset(filesetIdent);
  }

  @Test
  public void testDelete() throws IOException {
    // create fileset
    String filesetName = GravitinoITUtils.genRandomName("test_fileset_delete");
    NameIdentifier filesetIdent = NameIdentifier.of(schemaName, filesetName);
    Catalog catalog = metalake.loadCatalog(catalogName);
    String storageLocation = genStorageLocation(filesetName);
    catalog
        .asFilesetCatalog()
        .createFileset(
            filesetIdent,
            "fileset comment",
            Fileset.Type.MANAGED,
            storageLocation,
            new HashMap<>());
    Assertions.assertTrue(catalog.asFilesetCatalog().filesetExists(filesetIdent));

    // test gvfs delete
    Path hdfsPath = new Path(storageLocation);
    try (FileSystem fs = hdfsPath.getFileSystem(convertGvfsConfigToRealFileSystemConfig(conf))) {
      Assertions.assertTrue(fs.exists(hdfsPath));
      Path gvfsPath = genGvfsPath(filesetName);
      String fileName = "test.txt";
      Path deletePath = new Path(gvfsPath + "/" + fileName);
      try (FileSystem gvfs = gvfsPath.getFileSystem(conf)) {
        Assertions.assertTrue(gvfs.exists(gvfsPath));
        gvfs.create(deletePath).close();
        Assertions.assertTrue(gvfs.exists(deletePath));
        Assertions.assertTrue(gvfs.getFileStatus(deletePath).isFile());
        Assertions.assertTrue(fs.exists(new Path(storageLocation + "/" + fileName)));
      }
      try (FileSystem gvfs = gvfsPath.getFileSystem(conf)) {
        gvfs.delete(deletePath, true);
        Assertions.assertFalse(gvfs.exists(deletePath));
        Assertions.assertFalse(fs.exists(new Path(storageLocation + "/" + fileName)));
      }
    }

    catalog.asFilesetCatalog().dropFileset(filesetIdent);
  }

  @Test
  public void testGetStatus() throws IOException {
    // create fileset
    String filesetName = GravitinoITUtils.genRandomName("test_fileset_get_status");
    NameIdentifier filesetIdent = NameIdentifier.of(schemaName, filesetName);
    Catalog catalog = metalake.loadCatalog(catalogName);
    String storageLocation = genStorageLocation(filesetName);
    catalog
        .asFilesetCatalog()
        .createFileset(
            filesetIdent,
            "fileset comment",
            Fileset.Type.MANAGED,
            storageLocation,
            new HashMap<>());
    Assertions.assertTrue(catalog.asFilesetCatalog().filesetExists(filesetIdent));

    // test gvfs get status
    Path hdfsPath = new Path(storageLocation);
    try (FileSystem fs = hdfsPath.getFileSystem(convertGvfsConfigToRealFileSystemConfig(conf))) {
      Assertions.assertTrue(fs.exists(hdfsPath));
      Path gvfsPath = genGvfsPath(filesetName);
      String fileName = "test.txt";
      Path statusPath = new Path(gvfsPath + "/" + fileName);
      try (FileSystem gvfs = gvfsPath.getFileSystem(conf)) {
        Assertions.assertTrue(gvfs.exists(gvfsPath));
        gvfs.create(statusPath).close();
        Assertions.assertTrue(gvfs.exists(statusPath));
        Assertions.assertTrue(gvfs.getFileStatus(statusPath).isFile());
        Assertions.assertTrue(fs.exists(new Path(storageLocation + "/" + fileName)));
        FileStatus gvfsStatus = gvfs.getFileStatus(statusPath);
        FileStatus hdfsStatus = fs.getFileStatus(new Path(storageLocation + "/" + fileName));
        Assertions.assertEquals(
            hdfsStatus.getPath().toString(),
            gvfsStatus
                .getPath()
                .toString()
                .replaceFirst(genGvfsPath(filesetName).toString(), storageLocation));
      }
    }

    catalog.asFilesetCatalog().dropFileset(filesetIdent);
  }

  @Test
  public void testListStatus() throws IOException {
    // create fileset
    String filesetName = GravitinoITUtils.genRandomName("test_fileset_list_status");
    NameIdentifier filesetIdent = NameIdentifier.of(schemaName, filesetName);
    Catalog catalog = metalake.loadCatalog(catalogName);
    String storageLocation = genStorageLocation(filesetName);
    catalog
        .asFilesetCatalog()
        .createFileset(
            filesetIdent,
            "fileset comment",
            Fileset.Type.MANAGED,
            storageLocation,
            new HashMap<>());
    Assertions.assertTrue(catalog.asFilesetCatalog().filesetExists(filesetIdent));

    // test gvfs list status
    Path hdfsPath = new Path(storageLocation);
    try (FileSystem fs = hdfsPath.getFileSystem(convertGvfsConfigToRealFileSystemConfig(conf))) {
      Assertions.assertTrue(fs.exists(hdfsPath));
      Path gvfsPath = genGvfsPath(filesetName);
      for (int i = 0; i < 10; i++) {
        String fileName = "test_" + i + ".txt";
        Path statusPath = new Path(gvfsPath + "/" + fileName);
        try (FileSystem gvfs = gvfsPath.getFileSystem(conf)) {
          Assertions.assertTrue(gvfs.exists(gvfsPath));
          gvfs.create(statusPath).close();
          Assertions.assertTrue(gvfs.exists(statusPath));
          Assertions.assertTrue(gvfs.getFileStatus(statusPath).isFile());
          Assertions.assertTrue(fs.exists(new Path(storageLocation + "/" + fileName)));
        }
      }

      try (FileSystem gvfs = gvfsPath.getFileSystem(conf)) {
        List<FileStatus> gvfsStatus = new ArrayList<>(Arrays.asList(gvfs.listStatus(gvfsPath)));
        gvfsStatus.sort(Comparator.comparing(FileStatus::getPath));
        assertEquals(10, gvfsStatus.size());

        List<FileStatus> hdfsStatus = new ArrayList<>(Arrays.asList(fs.listStatus(hdfsPath)));
        hdfsStatus.sort(Comparator.comparing(FileStatus::getPath));
        assertEquals(10, hdfsStatus.size());

        for (int i = 0; i < 10; i++) {
          assertEquals(
              hdfsStatus.get(i).getPath().toString(),
              gvfsStatus
                  .get(i)
                  .getPath()
                  .toString()
                  .replaceFirst(genGvfsPath(filesetName).toString(), storageLocation));
        }
      }
    }

    catalog.asFilesetCatalog().dropFileset(filesetIdent);
  }

  @Test
  public void testMkdirs() throws IOException {
    // create fileset
    String filesetName = GravitinoITUtils.genRandomName("test_fileset_mkdirs");
    NameIdentifier filesetIdent = NameIdentifier.of(schemaName, filesetName);
    Catalog catalog = metalake.loadCatalog(catalogName);
    String storageLocation = genStorageLocation(filesetName);
    catalog
        .asFilesetCatalog()
        .createFileset(
            filesetIdent,
            "fileset comment",
            Fileset.Type.MANAGED,
            storageLocation,
            new HashMap<>());
    Assertions.assertTrue(catalog.asFilesetCatalog().filesetExists(filesetIdent));

    // test gvfs mkdirs
    Path hdfsPath = new Path(storageLocation);
    try (FileSystem fs = hdfsPath.getFileSystem(convertGvfsConfigToRealFileSystemConfig(conf))) {
      Assertions.assertTrue(fs.exists(hdfsPath));
      Path gvfsPath = genGvfsPath(filesetName);
      try (FileSystem gvfs = gvfsPath.getFileSystem(conf)) {
        Assertions.assertTrue(gvfs.exists(gvfsPath));
        String dirName = "test";
        Path dirPath = new Path(gvfsPath + "/" + dirName);
        gvfs.mkdirs(dirPath);
        Assertions.assertTrue(gvfs.exists(dirPath));
        Assertions.assertTrue(gvfs.getFileStatus(dirPath).isDirectory());
        Assertions.assertTrue(fs.exists(new Path(storageLocation + "/" + dirName)));
      }
    }

    catalog.asFilesetCatalog().dropFileset(filesetIdent);
  }

  @Test
  public void testRename() throws IOException {
    // create fileset
    String filesetName = GravitinoITUtils.genRandomName("test_fileset_rename");
    NameIdentifier filesetIdent = NameIdentifier.of(schemaName, filesetName);
    Catalog catalog = metalake.loadCatalog(catalogName);
    String storageLocation = genStorageLocation(filesetName);
    catalog
        .asFilesetCatalog()
        .createFileset(
            filesetIdent,
            "fileset comment",
            Fileset.Type.MANAGED,
            storageLocation,
            new HashMap<>());
    Assertions.assertTrue(catalog.asFilesetCatalog().filesetExists(filesetIdent));

    // test gvfs rename
    Path hdfsPath = new Path(storageLocation);
    try (FileSystem fs = hdfsPath.getFileSystem(convertGvfsConfigToRealFileSystemConfig(conf))) {
      Assertions.assertTrue(fs.exists(hdfsPath));
      Path gvfsPath = genGvfsPath(filesetName);
      String srcName = "test_src";
      Path srcPath = new Path(gvfsPath + "/" + srcName);

      try (FileSystem gvfs = gvfsPath.getFileSystem(conf)) {
        Assertions.assertTrue(gvfs.exists(gvfsPath));
        gvfs.mkdirs(srcPath);
        Assertions.assertTrue(gvfs.exists(srcPath));
        Assertions.assertTrue(gvfs.getFileStatus(srcPath).isDirectory());
        Assertions.assertTrue(fs.exists(new Path(storageLocation + "/" + srcName)));
      }

      try (FileSystem gvfs = gvfsPath.getFileSystem(conf)) {
        String dstName = "test_dst";
        Path dstPath = new Path(gvfsPath + "/" + dstName);
        gvfs.rename(srcPath, dstPath);
        Assertions.assertTrue(gvfs.exists(dstPath));
        Assertions.assertFalse(gvfs.exists(srcPath));
        Assertions.assertTrue(gvfs.getFileStatus(dstPath).isDirectory());
        Assertions.assertTrue(fs.exists(new Path(storageLocation + "/" + dstName)));
        Assertions.assertFalse(fs.exists(new Path(storageLocation + "/" + srcName)));
      }
    }

    catalog.asFilesetCatalog().dropFileset(filesetIdent);
  }

  @Test
  public void testGetDefaultReplications() throws IOException {
    String filesetName = GravitinoITUtils.genRandomName("test_get_default_replications");
    NameIdentifier filesetIdent = NameIdentifier.of(schemaName, filesetName);
    Catalog catalog = metalake.loadCatalog(catalogName);
    String storageLocation = genStorageLocation(filesetName);
    catalog
        .asFilesetCatalog()
        .createFileset(
            filesetIdent,
            "fileset comment",
            Fileset.Type.MANAGED,
            storageLocation,
            new HashMap<>());
    Assertions.assertTrue(catalog.asFilesetCatalog().filesetExists(filesetIdent));
    Path gvfsPath = genGvfsPath(filesetName);
    try (FileSystem gvfs = gvfsPath.getFileSystem(conf)) {
      assertEquals(defaultReplication, gvfs.getDefaultReplication(gvfsPath));
    }

    catalog.asFilesetCatalog().dropFileset(filesetIdent);
  }

  @Test
  public void testGetDefaultBlockSizes() throws IOException {
    String filesetName = GravitinoITUtils.genRandomName("test_get_default_block_sizes");
    NameIdentifier filesetIdent = NameIdentifier.of(schemaName, filesetName);
    Catalog catalog = metalake.loadCatalog(catalogName);
    String storageLocation = genStorageLocation(filesetName);
    catalog
        .asFilesetCatalog()
        .createFileset(
            filesetIdent,
            "fileset comment",
            Fileset.Type.MANAGED,
            storageLocation,
            new HashMap<>());
    Assertions.assertTrue(catalog.asFilesetCatalog().filesetExists(filesetIdent));
    Path gvfsPath = genGvfsPath(filesetName);
    try (FileSystem gvfs = gvfsPath.getFileSystem(conf)) {
      assertEquals(defaultBockSize, gvfs.getDefaultBlockSize(gvfsPath));
    }

    catalog.asFilesetCatalog().dropFileset(filesetIdent);
  }

  protected String genStorageLocation(String fileset) {
    return String.format("%s/%s", baseHdfsPath(), fileset);
  }

  private String baseHdfsPath() {
    return String.format(
        "hdfs://%s:%d/%s/%s",
        containerSuite.getHiveContainer().getContainerIpAddress(),
        HiveContainer.HDFS_DEFAULTFS_PORT,
        catalogName,
        schemaName);
  }

  protected Path genGvfsPath(String fileset) {
    return new Path(String.format("gvfs://fileset/%s/%s/%s", catalogName, schemaName, fileset));
  }
}
