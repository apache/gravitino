/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.gravitino.integration.test.client;

import com.google.common.collect.Maps;
import java.util.Collections;
import java.util.Map;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.StringIdentifier;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.file.BaseFilesetDataOperationCtx;
import org.apache.gravitino.file.ClientType;
import org.apache.gravitino.file.Fileset;
import org.apache.gravitino.file.FilesetContext;
import org.apache.gravitino.file.FilesetDataOperation;
import org.apache.gravitino.file.FilesetDataOperationCtx;
import org.apache.gravitino.integration.test.container.ContainerSuite;
import org.apache.gravitino.integration.test.container.HiveContainer;
import org.apache.gravitino.integration.test.util.AbstractIT;
import org.apache.gravitino.integration.test.util.GravitinoITUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tag("gravitino-docker-test")
public class FilesetIT extends AbstractIT {
  private static final Logger LOG = LoggerFactory.getLogger(FilesetIT.class);

  private static final ContainerSuite containerSuite = ContainerSuite.getInstance();

  private static final String metalakeName = GravitinoITUtils.genRandomName("fileset_it_metalake");

  private static GravitinoMetalake metalake;

  private static String hmsUri;

  @BeforeAll
  public static void startUp() {
    containerSuite.startHiveContainer();
    hmsUri =
        String.format(
            "thrift://%s:%d",
            containerSuite.getHiveContainer().getContainerIpAddress(),
            HiveContainer.HIVE_METASTORE_PORT);

    Assertions.assertFalse(client.metalakeExists(metalakeName));
    metalake = client.createMetalake(metalakeName, "metalake comment", Collections.emptyMap());
    Assertions.assertTrue(client.metalakeExists(metalakeName));
  }

  @AfterAll
  public static void tearDown() {
    client.dropMetalake(metalakeName);

    if (client != null) {
      client.close();
      client = null;
    }

    try {
      closer.close();
    } catch (Exception e) {
      LOG.error("Exception in closing CloseableGroup", e);
    }
  }

  @Test
  public void testGetFilesetContext() {
    String catalogName = GravitinoITUtils.genRandomName("catalog");
    Assertions.assertFalse(metalake.catalogExists(catalogName));

    Map<String, String> properties = Maps.newHashMap();
    properties.put("metastore.uris", hmsUri);
    Catalog catalog =
        metalake.createCatalog(
            catalogName, Catalog.Type.FILESET, "hadoop", "catalog comment", properties);
    Assertions.assertTrue(metalake.catalogExists(catalogName));

    String schemaName = GravitinoITUtils.genRandomName("schema");
    Assertions.assertFalse(catalog.asSchemas().schemaExists(schemaName));
    catalog.asSchemas().createSchema(schemaName, "schema comment", Maps.newHashMap());
    Assertions.assertTrue(catalog.asSchemas().schemaExists(schemaName));

    String filesetName = GravitinoITUtils.genRandomName("fileset");
    NameIdentifier filesetIdent =
        NameIdentifier.of(metalakeName, catalogName, schemaName, filesetName);
    Assertions.assertFalse(catalog.asFilesetCatalog().filesetExists(filesetIdent));
    Fileset expectedFileset =
        catalog
            .asFilesetCatalog()
            .createFileset(
                filesetIdent,
                "fileset comment",
                Fileset.Type.MANAGED,
                generateLocation(catalogName, schemaName, filesetName),
                Maps.newHashMap());
    Assertions.assertTrue(catalog.asFilesetCatalog().filesetExists(filesetIdent));

    FilesetDataOperationCtx ctx =
        BaseFilesetDataOperationCtx.builder()
            .withSubPath("/test.par")
            .withOperation(FilesetDataOperation.CREATE)
            .withClientType(ClientType.HADOOP_GVFS)
            .build();
    FilesetContext context = catalog.asFilesetCatalog().getFilesetContext(filesetIdent, ctx);

    Fileset actualFileset = context.fileset();
    Assertions.assertEquals(expectedFileset.name(), actualFileset.name());
    Assertions.assertEquals(expectedFileset.comment(), actualFileset.comment());
    Assertions.assertEquals(expectedFileset.type(), actualFileset.type());
    Assertions.assertEquals(expectedFileset.storageLocation(), actualFileset.storageLocation());
    Assertions.assertFalse(actualFileset.properties().containsKey(StringIdentifier.ID_KEY));

    Assertions.assertEquals(expectedFileset.storageLocation() + "/test.par", context.actualPath());
  }

  private static String generateLocation(
      String catalogName, String schemaName, String filesetName) {
    return String.format(
        "hdfs://%s:%d/user/hadoop/%s/%s/%s",
        containerSuite.getHiveContainer().getContainerIpAddress(),
        HiveContainer.HDFS_DEFAULTFS_PORT,
        catalogName,
        schemaName,
        filesetName);
  }
}
