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

package org.apache.gravitino.hive.client;

import java.io.File;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Properties;
import org.apache.commons.io.FileUtils;
import org.apache.gravitino.integration.test.container.HiveContainer;
import org.apache.hadoop.security.authentication.util.KerberosName;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestInstance;

/** Kerberos-enabled Hive2 HMS tests. */
@Tag("gravitino-docker-test")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestHive2HMSWithKerberos extends TestHive2HMS {

  private static final String CLIENT_PRINCIPAL = "cli@HADOOPKRB";
  private File tempDir;
  private String keytabPath;
  private String krb5ConfPath;

  @BeforeAll
  @Override
  public void startHiveContainer() {
    testPrefix = "hive2_kerberos";
    catalogName = "";
    containerSuite.startKerberosHiveContainer();
    hiveContainer = containerSuite.getKerberosHiveContainer();

    metastoreUri =
        String.format(
            "thrift://%s:%d",
            hiveContainer.getContainerIpAddress(), HiveContainer.HIVE_METASTORE_PORT);
    hdfsBasePath =
        String.format(
            "hdfs://%s:%d/tmp/gravitino_test",
            hiveContainer.getContainerIpAddress(), HiveContainer.HDFS_DEFAULTFS_PORT);

    prepareKerberosConfig();

    // Initialize client with Kerberos-aware properties.
    hiveClient = new HiveClientFactory(createHiveProperties(), testPrefix).createHiveClient();
  }

  protected void prepareKerberosConfig() {
    try {
      tempDir = Files.createTempDirectory(testPrefix).toFile();
      tempDir.deleteOnExit();

      String ip = hiveContainer.getContainerIpAddress();

      // Copy client keytab (admin keytab is used for metastore client)
      keytabPath = new File(tempDir, "admin.keytab").getAbsolutePath();
      hiveContainer.getContainer().copyFileFromContainer("/etc/admin.keytab", keytabPath);

      // Copy and patch krb5.conf
      String tmpKrb5Path = new File(tempDir, "krb5.conf.tmp").getAbsolutePath();
      krb5ConfPath = new File(tempDir, "krb5.conf").getAbsolutePath();
      hiveContainer.getContainer().copyFileFromContainer("/etc/krb5.conf", tmpKrb5Path);
      String content = FileUtils.readFileToString(new File(tmpKrb5Path), StandardCharsets.UTF_8);
      content = content.replace("kdc = localhost:88", "kdc = " + ip + ":88");
      content = content.replace("admin_server = localhost", "admin_server = " + ip + ":749");
      FileUtils.write(new File(krb5ConfPath), content, StandardCharsets.UTF_8);

      System.setProperty("java.security.krb5.conf", krb5ConfPath);
      System.setProperty("hadoop.security.authentication", "kerberos");

      refreshKerberosConfig();
      KerberosName.resetDefaultRealm();
    } catch (Exception e) {
      throw new RuntimeException("Failed to prepare kerberos configuration for Hive2 HMS tests", e);
    }
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

  @Override
  protected Properties createHiveProperties() {
    Properties properties = super.createHiveProperties();
    properties.setProperty("hive.metastore.sasl.enabled", "true");
    properties.setProperty(
        "hive.metastore.kerberos.principal",
        String.format("hive/%s@HADOOPKRB", hiveContainer.getHostName()));
    properties.setProperty("authentication.kerberos.principal", CLIENT_PRINCIPAL);
    properties.setProperty("authentication.kerberos.keytab-uri", keytabPath);
    properties.setProperty("authentication.impersonation-enable", "true");
    properties.setProperty("hadoop.security.authentication", "kerberos");
    return properties;
  }

  @AfterAll
  @Override
  public void stopHiveContainer() throws Exception {
    super.stopHiveContainer();
    try {
      if (tempDir != null && tempDir.exists()) {
        FileUtils.deleteDirectory(tempDir);
      }
    } catch (Exception e) {
      throw new RuntimeException("Failed to clean up temporary files for Kerberos config", e);
    }
  }
}
