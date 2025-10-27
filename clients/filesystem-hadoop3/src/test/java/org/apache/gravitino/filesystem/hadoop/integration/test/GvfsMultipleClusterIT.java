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

import static org.apache.gravitino.file.Fileset.LOCATION_NAME_UNKNOWN;
import static org.apache.gravitino.file.Fileset.PROPERTY_DEFAULT_LOCATION_NAME;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Collections;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.file.Fileset;
import org.apache.gravitino.integration.test.container.ContainerSuite;
import org.apache.gravitino.integration.test.container.HiveContainer;
import org.apache.gravitino.integration.test.util.BaseIT;
import org.apache.gravitino.integration.test.util.GravitinoITUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.authentication.util.KerberosName;
import org.apache.hadoop.security.authentication.util.KerberosUtil;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tag("gravitino-docker-test")
public class GvfsMultipleClusterIT extends BaseIT {
  private static final Logger LOG = LoggerFactory.getLogger(GvfsMultipleClusterIT.class);
  protected static final ContainerSuite containerSuite = ContainerSuite.getInstance();
  protected HiveContainer hiveContainer;
  protected HiveContainer kerberosHiveContainer;

  protected String metalakeName = GravitinoITUtils.genRandomName("gvfs_it_metalake");
  protected String catalogName = GravitinoITUtils.genRandomName("catalog");
  protected String schemaName = GravitinoITUtils.genRandomName("schema");
  protected GravitinoMetalake metalake;
  protected Configuration conf = new Configuration();
  protected Map<String, String> properties = Maps.newHashMap();
  protected String configResourcesPath;

  @BeforeAll
  public void startUp() throws Exception {
    containerSuite.startHiveContainer();
    hiveContainer = containerSuite.getHiveContainer();

    containerSuite.startKerberosHiveContainer();
    kerberosHiveContainer = containerSuite.getKerberosHiveContainer();

    setupKerberosEnv();

    Assertions.assertFalse(client.metalakeExists(metalakeName));
    metalake = client.createMetalake(metalakeName, "metalake comment", Collections.emptyMap());
    Assertions.assertTrue(client.metalakeExists(metalakeName));

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

  private void setupKerberosEnv() throws Exception {
    File baseDir = new File(System.getProperty("java.io.tmpdir"));
    File file = Files.createTempDirectory(baseDir.toPath(), "test").toFile();
    file.deleteOnExit();
    String tmpDir = file.getAbsolutePath();
    this.configResourcesPath = tmpDir;

    // Keytab of the Gravitino SDK client
    String keytabPath = tmpDir + "/admin.keytab";
    kerberosHiveContainer.getContainer().copyFileFromContainer("/etc/admin.keytab", keytabPath);

    String tmpKrb5Path = tmpDir + "/krb5.conf_tmp";
    String krb5Path = tmpDir + "/krb5.conf";
    kerberosHiveContainer.getContainer().copyFileFromContainer("/etc/krb5.conf", tmpKrb5Path);

    // Modify the krb5.conf and change the kdc and admin_server to the container IP
    String ip = containerSuite.getKerberosHiveContainer().getContainerIpAddress();
    String content = FileUtils.readFileToString(new File(tmpKrb5Path), StandardCharsets.UTF_8);
    content = content.replace("kdc = localhost:88", "kdc = " + ip + ":88");
    content = content.replace("admin_server = localhost", "admin_server = " + ip + ":749");
    FileUtils.write(new File(krb5Path), content, StandardCharsets.UTF_8);

    LOG.info("Kerberos kdc config:\n{}, path: {}", content, krb5Path);
    System.setProperty("java.security.krb5.conf", krb5Path);
    System.setProperty("sun.security.krb5.debug", "true");

    // create hdfs-site.xml and core-site.xml
    // read hdfs-site.xml from resources
    String hdfsSiteXml = readResourceFile("hd_kbs_conf/hdfs-site.xml");
    FileUtils.write(new File(tmpDir + "/hdfs-site.xml"), hdfsSiteXml, StandardCharsets.UTF_8);
    String coreSiteXml = readResourceFile("hd_kbs_conf/core-site.xml");
    coreSiteXml = coreSiteXml.replace("XXX_KEYTAB_XXX", keytabPath);
    coreSiteXml = coreSiteXml.replace("XXX_KRB_CONF_XXX", krb5Path);
    FileUtils.write(new File(tmpDir + "/core-site.xml"), coreSiteXml, StandardCharsets.UTF_8);

    LOG.info("Kerberos config resources created in {}", tmpDir);
    refreshKerberosConfig();
    KerberosName.resetDefaultRealm();

    LOG.info("Kerberos default realm: {}", KerberosUtil.getDefaultRealm());
  }

  private static String readResourceFile(String resourcePath) throws IOException {
    return new String(
        GvfsMultipleClusterIT.class
            .getClassLoader()
            .getResourceAsStream(resourcePath)
            .readAllBytes(),
        StandardCharsets.UTF_8);
  }

  private static void refreshKerberosConfig() {
    Class<?> classRef;
    try {
      if (System.getProperty("java.vendor").contains("IBM")) {
        classRef = Class.forName("com.ibm.security.krb5.internal.Config");
      } else {
        classRef = Class.forName("sun.security.krb5.Config");
      }

      Method refershMethod = classRef.getMethod("refresh");
      refershMethod.invoke(null);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @AfterAll
  public void tearDown() throws IOException {
    if (metalake == null) {
      return;
    }

    Catalog catalog = metalake.loadCatalog(catalogName);
    catalog.asSchemas().dropSchema(schemaName, true);
    metalake.dropCatalog(catalogName, true);
    client.dropMetalake(metalakeName, true);

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

  protected Path genGvfsPath(String fileset) {
    return new Path(String.format("gvfs://fileset/%s/%s/%s", catalogName, schemaName, fileset));
  }

  private String baseHdfsPath(String ip, String filesetName) {
    return String.format(
        "hdfs://%s:%d/tmp/%s/%s/%s",
        ip, HiveContainer.HDFS_DEFAULTFS_PORT, catalogName, schemaName, filesetName);
  }

  @Test
  public void testFsOperation() throws IOException {
    // create a fileset with normal cluster
    String normalFilesetName = GravitinoITUtils.genRandomName("fileset_normal");
    NameIdentifier normalFilesetIdent = NameIdentifier.of(schemaName, normalFilesetName);
    Catalog catalog = metalake.loadCatalog(catalogName);
    String location = baseHdfsPath(hiveContainer.getContainerIpAddress(), normalFilesetName);
    catalog
        .asFilesetCatalog()
        .createMultipleLocationFileset(
            normalFilesetIdent,
            "fileset comment",
            Fileset.Type.MANAGED,
            ImmutableMap.of(LOCATION_NAME_UNKNOWN, location),
            ImmutableMap.of(PROPERTY_DEFAULT_LOCATION_NAME, LOCATION_NAME_UNKNOWN));
    Assertions.assertTrue(catalog.asFilesetCatalog().filesetExists(normalFilesetIdent));

    // create a fileset with kerberos cluster
    String kerberosFilesetName = GravitinoITUtils.genRandomName("fileset_kerberos");
    NameIdentifier kerberosFilesetIdent = NameIdentifier.of(schemaName, kerberosFilesetName);
    location = baseHdfsPath(kerberosHiveContainer.getContainerIpAddress(), kerberosFilesetName);
    String configResources =
        configResourcesPath + "/core-site.xml," + configResourcesPath + "/hdfs-site.xml";
    catalog
        .asFilesetCatalog()
        .createMultipleLocationFileset(
            kerberosFilesetIdent,
            "fileset comment",
            Fileset.Type.MANAGED,
            ImmutableMap.of(LOCATION_NAME_UNKNOWN, location),
            ImmutableMap.of(
                PROPERTY_DEFAULT_LOCATION_NAME,
                LOCATION_NAME_UNKNOWN,
                "hdfs.config.resources",
                configResources));
    Assertions.assertTrue(catalog.asFilesetCatalog().filesetExists(kerberosFilesetIdent));

    Path normalGvfsPath = genGvfsPath(normalFilesetName);
    Path kerberosGvfsPath = genGvfsPath(kerberosFilesetName);
    try (FileSystem gvfs = normalGvfsPath.getFileSystem(conf)) {
      if (!gvfs.exists(normalGvfsPath)) {
        gvfs.mkdirs(normalGvfsPath);
      }
      if (!gvfs.exists(kerberosGvfsPath)) {
        gvfs.mkdirs(kerberosGvfsPath);
      }

      gvfs.create(new Path(normalGvfsPath + "/file1.txt")).close();
      gvfs.create(new Path(kerberosGvfsPath + "/file1.txt")).close();
      gvfs.create(new Path(normalGvfsPath + "/file2.txt")).close();
      gvfs.create(new Path(kerberosGvfsPath + "/file2.txt")).close();
    }

    // catalog.asFilesetCatalog().dropFileset(normalFilesetIdent);
    // catalog.asFilesetCatalog().dropFileset(kerberosFilesetIdent);
  }
}
