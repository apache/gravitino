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
package org.apache.gravitino.iceberg.integration.test;

import static org.apache.gravitino.integration.test.util.HttpUtils.isHttpServerUp;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.gravitino.iceberg.common.IcebergCatalogBackend;
import org.apache.gravitino.iceberg.common.IcebergConfig;
import org.apache.gravitino.integration.test.container.HiveContainer;
import org.apache.gravitino.integration.test.util.GravitinoITUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.api.condition.EnabledIf;

@Tag("gravitino-docker-test")
@TestInstance(Lifecycle.PER_CLASS)
public class IcebergRestKerberosHiveCatalogIT extends IcebergRESTHiveCatalogIT {

  private static final String HIVE_METASTORE_CLIENT_PRINCIPAL = "cli@HADOOPKRB";
  private static final String HIVE_METASTORE_CLIENT_KEYTAB = "/client.keytab";

  private static String tempDir;

  public IcebergRestKerberosHiveCatalogIT() {
    super();
  }

  @BeforeAll
  void prepareSQLContext() {
    // use rest catalog
    sql("USE rest");
    purgeAllIcebergTestNamespaces();
  }

  @Override
  void initEnv() {
    containerSuite.startKerberosHiveContainer();
    try {

      // Init kerberos configurations;
      File baseDir = new File(System.getProperty("java.io.tmpdir"));
      File file = Files.createTempDirectory(baseDir.toPath(), "test").toFile();
      file.deleteOnExit();
      tempDir = file.getAbsolutePath();

      HiveContainer kerberosHiveContainer = containerSuite.getKerberosHiveContainer();
      kerberosHiveContainer
          .getContainer()
          .copyFileFromContainer("/etc/admin.keytab", tempDir + HIVE_METASTORE_CLIENT_KEYTAB);

      String tmpKrb5Path = tempDir + "/krb5.conf_tmp";
      String krb5Path = tempDir + "/krb5.conf";
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

    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  Map<String, String> getCatalogConfig() {
    Map<String, String> configMap = new HashMap<>();

    configMap.put("gravitino.iceberg-rest.authentication.type", "kerberos");
    configMap.put(
        "gravitino.iceberg-rest.authentication.kerberos.principal",
        HIVE_METASTORE_CLIENT_PRINCIPAL);
    configMap.put(
        "gravitino.iceberg-rest.hive.metastore.kerberos.principal",
        "hive/_HOST@HADOOPKRB"
            .replace("_HOST", containerSuite.getKerberosHiveContainer().getHostName()));

    configMap.put(
        "gravitino.iceberg-rest.authentication.kerberos.keytab-uri",
        tempDir + HIVE_METASTORE_CLIENT_KEYTAB);
    configMap.put("gravitino.iceberg-rest.hive.metastore.sasl.enabled", "true");

    configMap.put(
        IcebergConfig.ICEBERG_CONFIG_PREFIX + IcebergConfig.CATALOG_BACKEND.getKey(),
        IcebergCatalogBackend.HIVE.toString().toLowerCase());

    configMap.put(
        IcebergConfig.ICEBERG_CONFIG_PREFIX + IcebergConfig.CATALOG_URI.getKey(),
        String.format(
            "thrift://%s:%d",
            containerSuite.getKerberosHiveContainer().getContainerIpAddress(),
            HiveContainer.HIVE_METASTORE_PORT));

    configMap.put(
        IcebergConfig.ICEBERG_CONFIG_PREFIX + IcebergConfig.CATALOG_WAREHOUSE.getKey(),
        GravitinoITUtils.genRandomName(
            String.format(
                "hdfs://%s:%d/user/hive/warehouse-hive",
                containerSuite.getKerberosHiveContainer().getContainerIpAddress(),
                HiveContainer.HDFS_DEFAULTFS_PORT)));
    return configMap;
  }

  @Test
  void testCreateNamespace() {}

  @Test
  void testListNamespace() {
    int port = getServerPort();
    String url = String.format("http://localhost:%s/iceberg/v1/namespaces", port);

    Assertions.assertTrue(isHttpServerUp(url));
  }

  @Test
  void testDropNameSpace() {}

  @Test
  void testNameSpaceProperties() {}

  @Test
  void testDML() {}

  @Test
  void testCreateTable() {}

  @Test
  void testDropTable() {}

  @Test
  void testListTable() {}

  @Test
  void testRenameTable() {}

  @Test
  void testSetTableProperties() {}

  @Test
  void testAddColumns() {}

  @Test
  void testRenameColumns() {}

  @Test
  void testDropColumns() {}

  @Test
  void testUpdateColumnType() {}

  @Test
  void testUpdateColumnPosition() {}

  @Test
  void testAlterPartitions() {}

  @Test
  void testAlterSortBy() {}

  @Test
  void testAlterPartitionBy() {}

  @Test
  void testAlterIdentifier() {}

  @Test
  @EnabledIf("catalogTypeNotMemory")
  void testSnapshot() {}

  @Test
  @EnabledIf("catalogTypeNotMemory")
  void testRegisterTable() {}
}
