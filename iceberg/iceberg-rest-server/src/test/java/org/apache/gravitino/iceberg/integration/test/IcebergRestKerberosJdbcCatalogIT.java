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

import java.io.File;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.apache.commons.io.FileUtils;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergCatalogBackend;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergConstants;
import org.apache.gravitino.iceberg.common.IcebergConfig;
import org.apache.gravitino.integration.test.container.ContainerSuite;
import org.apache.gravitino.integration.test.container.HiveContainer;
import org.apache.gravitino.integration.test.util.GravitinoITUtils;
import org.apache.gravitino.integration.test.util.ITUtils;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.api.condition.EnabledIf;

@Tag("gravitino-docker-test")
@TestInstance(Lifecycle.PER_CLASS)
@EnabledIf("isEmbedded")
public class IcebergRestKerberosJdbcCatalogIT extends IcebergRESTServiceIT {

  private static final ContainerSuite containerSuite = ContainerSuite.getInstance();

  private static final String HDFS_CLIENT_PRINCIPAL = "cli@HADOOPKRB";
  private static final String HDFS_CLIENT_KEYTAB = "/client.keytab";

  private static String tempDir;

  public IcebergRestKerberosJdbcCatalogIT() {
    catalogType = IcebergCatalogBackend.JDBC;
  }

  @Override
  void initEnv() {
    containerSuite.startKerberosHiveContainer();
    try {
      File baseDir = new File(System.getProperty("java.io.tmpdir"));
      File file = Files.createTempDirectory(baseDir.toPath(), "test").toFile();
      file.deleteOnExit();
      tempDir = file.getAbsolutePath();

      HiveContainer kerberosHiveContainer = containerSuite.getKerberosHiveContainer();
      kerberosHiveContainer
          .getContainer()
          .copyFileFromContainer("/etc/admin.keytab", tempDir + HDFS_CLIENT_KEYTAB);

      String tmpKrb5Path = tempDir + "/krb5.conf_tmp";
      String krb5Path = tempDir + "/krb5.conf";
      kerberosHiveContainer.getContainer().copyFileFromContainer("/etc/krb5.conf", tmpKrb5Path);

      String ip = containerSuite.getKerberosHiveContainer().getContainerIpAddress();
      String content = FileUtils.readFileToString(new File(tmpKrb5Path), StandardCharsets.UTF_8);
      content = content.replace("kdc = localhost:88", "kdc = " + ip + ":88");
      content = content.replace("admin_server = localhost", "admin_server = " + ip + ":749");
      FileUtils.write(new File(krb5Path), content, StandardCharsets.UTF_8);

      LOG.info("Kerberos kdc config:\n{}, path: {}", content, krb5Path);
      System.setProperty("java.security.krb5.conf", krb5Path);
      System.setProperty("sun.security.krb5.debug", "true");
      System.setProperty("java.security.krb5.realm", "HADOOPKRB");
      System.setProperty("java.security.krb5.kdc", ip);

      refreshKerberosConfig();
      resetDefaultRealm();

      containerSuite
          .getKerberosHiveContainer()
          .executeInContainer("hadoop", "fs", "-chown", "-R", "cli", "/user/hive/");

    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  Map<String, String> getCatalogConfig() {
    Map<String, String> configMap = new HashMap<>();

    configMap.put("gravitino.iceberg-rest.authentication.type", "kerberos");
    configMap.put(
        "gravitino.iceberg-rest.authentication.kerberos.principal", HDFS_CLIENT_PRINCIPAL);
    configMap.put(
        "gravitino.iceberg-rest.authentication.kerberos.keytab-uri", tempDir + HDFS_CLIENT_KEYTAB);
    configMap.put("gravitino.iceberg-rest.hadoop.security.authentication", "kerberos");
    configMap.put(
        "gravitino.iceberg-rest.dfs.namenode.kerberos.principal",
        "hdfs/_HOST@HADOOPKRB"
            .replace("_HOST", containerSuite.getKerberosHiveContainer().getHostName()));

    configMap.put(
        IcebergConfig.ICEBERG_CONFIG_PREFIX + IcebergConfig.CATALOG_BACKEND.getKey(),
        IcebergCatalogBackend.JDBC.toString().toLowerCase());
    configMap.put(
        IcebergConfig.ICEBERG_CONFIG_PREFIX + IcebergConfig.JDBC_DRIVER.getKey(),
        "org.sqlite.JDBC");
    configMap.put(
        IcebergConfig.ICEBERG_CONFIG_PREFIX + IcebergConfig.CATALOG_URI.getKey(),
        "jdbc:sqlite::memory:");
    configMap.put(
        IcebergConfig.ICEBERG_CONFIG_PREFIX + IcebergConstants.ICEBERG_JDBC_USER, "iceberg");
    configMap.put(
        IcebergConfig.ICEBERG_CONFIG_PREFIX + IcebergConstants.ICEBERG_JDBC_PASSWORD, "iceberg");
    configMap.put(
        IcebergConfig.ICEBERG_CONFIG_PREFIX + IcebergConfig.JDBC_INIT_TABLES.getKey(), "true");
    configMap.put(IcebergConfig.ICEBERG_CONFIG_PREFIX + "jdbc.schema-version", "V1");
    configMap.put(
        IcebergConfig.ICEBERG_CONFIG_PREFIX + IcebergConfig.CATALOG_WAREHOUSE.getKey(),
        GravitinoITUtils.genRandomName(
            String.format(
                "hdfs://%s:%d/user/hive/warehouse-jdbc-kerberos",
                containerSuite.getKerberosHiveContainer().getContainerIpAddress(),
                HiveContainer.HDFS_DEFAULTFS_PORT)));
    return configMap;
  }

  private static boolean isEmbedded() {
    String mode =
        System.getProperty(ITUtils.TEST_MODE) == null
            ? ITUtils.EMBEDDED_TEST_MODE
            : System.getProperty(ITUtils.TEST_MODE);

    return Objects.equals(mode, ITUtils.EMBEDDED_TEST_MODE);
  }

  private static void refreshKerberosConfig() {
    Class<?> classRef;
    try {
      if (System.getProperty("java.vendor").contains("IBM")) {
        classRef = Class.forName("com.ibm.security.krb5.internal.Config");
      } else {
        classRef = Class.forName("sun.security.krb5.Config");
      }

      Method refreshMethod = classRef.getMethod("refresh");
      refreshMethod.invoke(null);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private void resetDefaultRealm() {
    try {
      String kerberosNameClass = "org.apache.hadoop.security.authentication.util.KerberosName";
      Class<?> cl = Class.forName(kerberosNameClass);
      cl.getMethod("resetDefaultRealm").invoke(null);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
