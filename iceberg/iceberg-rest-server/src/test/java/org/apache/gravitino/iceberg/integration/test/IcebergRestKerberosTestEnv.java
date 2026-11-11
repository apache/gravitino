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
import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.gravitino.integration.test.container.ContainerSuite;
import org.apache.gravitino.integration.test.container.HiveContainer;
import org.apache.gravitino.integration.test.util.ITUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.spark.SparkConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Shared Kerberos test environment setup for Iceberg REST integration tests. */
public final class IcebergRestKerberosTestEnv {

  private static final Logger LOG = LoggerFactory.getLogger(IcebergRestKerberosTestEnv.class);

  private static final String KRB5_CONF_PROPERTY = "java.security.krb5.conf";
  private static final String KRB5_DEBUG_PROPERTY = "sun.security.krb5.debug";
  private static final String KRB5_REALM_PROPERTY = "java.security.krb5.realm";
  private static final String KRB5_KDC_PROPERTY = "java.security.krb5.kdc";

  private static final String[] MANAGED_SYSTEM_PROPERTIES = {
    KRB5_CONF_PROPERTY, KRB5_DEBUG_PROPERTY, KRB5_REALM_PROPERTY, KRB5_KDC_PROPERTY
  };

  /** Local path suffix for the copied client keytab under the temp directory. */
  public static final String CLIENT_KEYTAB = "/client.keytab";

  private static String tempDir;

  /**
   * Configures Spark to authenticate to Kerberos-protected HDFS.
   *
   * <p>In deploy mode the Iceberg REST server runs in a separate JVM, so Spark must perform its own
   * Kerberos login to write data files to HDFS during DML tests.
   *
   * @param sparkConf Spark configuration to update
   * @param clientPrincipal Kerberos principal for the Spark client
   * @param hdfsHostName HDFS namenode host name used to build the namenode Kerberos principal
   */
  public static void configureSparkKerberos(
      SparkConf sparkConf, String clientPrincipal, String hdfsHostName) {
    if (tempDir == null) {
      throw new IllegalStateException(
          "Kerberos test environment is not initialized, call init() first");
    }
    String keytabPath = tempDir + CLIENT_KEYTAB;
    String hdfsPrincipal = "hdfs/_HOST@HADOOPKRB".replace("_HOST", hdfsHostName);
    try {
      Configuration configuration = new Configuration();
      configuration.set("hadoop.security.authentication", "kerberos");
      configuration.set("dfs.namenode.kerberos.principal", hdfsPrincipal);
      UserGroupInformation.setConfiguration(configuration);
      if (!UserGroupInformation.isLoginKeytabBased()) {
        UserGroupInformation.loginUserFromKeytab(clientPrincipal, keytabPath);
      }
    } catch (IOException e) {
      throw new RuntimeException("Failed to login Spark client with Kerberos", e);
    }

    sparkConf
        .set("spark.hadoop.security.authentication", "kerberos")
        .set("spark.hadoop.dfs.namenode.kerberos.principal", hdfsPrincipal)
        .set("spark.kerberos.keytab", keytabPath)
        .set("spark.kerberos.principal", clientPrincipal);

    String krb5Path = System.getProperty(KRB5_CONF_PROPERTY);
    if (krb5Path != null) {
      StringBuilder opts = new StringBuilder();
      opts.append(String.format("-Djava.security.krb5.conf=%s", krb5Path));
      String realm = System.getProperty(KRB5_REALM_PROPERTY);
      if (realm != null) {
        opts.append(String.format(" -Djava.security.krb5.realm=%s", realm));
      }
      String kdc = System.getProperty(KRB5_KDC_PROPERTY);
      if (kdc != null) {
        opts.append(String.format(" -Djava.security.krb5.kdc=%s", kdc));
      }
      sparkConf.set("spark.driver.extraJavaOptions", opts.toString());
      sparkConf.set("spark.executor.extraJavaOptions", opts.toString());
    }
  }

  private static final String KEYTAB_CONTAINER_PATH = "/etc/admin.keytab";

  private static Map<String, String> savedSystemProperties;
  private static int initRefCount;

  private IcebergRestKerberosTestEnv() {}

  /** Returns whether integration tests are running in deploy mode. */
  public static boolean isDeployMode() {
    String mode =
        System.getProperty(ITUtils.TEST_MODE) == null
            ? ITUtils.EMBEDDED_TEST_MODE
            : System.getProperty(ITUtils.TEST_MODE);
    return ITUtils.DEPLOY_TEST_MODE.equals(mode);
  }

  /**
   * Starts the Kerberos Hive container and configures the JVM krb5 settings for tests.
   *
   * @param containerSuite shared docker container suite
   * @return temp directory containing krb5.conf and client keytab
   */
  public static String init(ContainerSuite containerSuite) {
    if (initRefCount == 0) {
      savedSystemProperties = saveSystemProperties(MANAGED_SYSTEM_PROPERTIES);
    }
    initRefCount++;

    containerSuite.startKerberosHiveContainer();
    try {
      File baseDir = new File(System.getProperty("java.io.tmpdir"));
      File file = Files.createTempDirectory(baseDir.toPath(), "test").toFile();
      file.deleteOnExit();
      String tempDir = file.getAbsolutePath();

      HiveContainer kerberosHiveContainer = containerSuite.getKerberosHiveContainer();
      kerberosHiveContainer
          .getContainer()
          .copyFileFromContainer(KEYTAB_CONTAINER_PATH, tempDir + CLIENT_KEYTAB);

      String tmpKrb5Path = tempDir + "/krb5.conf_tmp";
      String krb5Path = tempDir + "/krb5.conf";
      kerberosHiveContainer.getContainer().copyFileFromContainer("/etc/krb5.conf", tmpKrb5Path);

      String ip = kerberosHiveContainer.getContainerIpAddress();
      String content = FileUtils.readFileToString(new File(tmpKrb5Path), StandardCharsets.UTF_8);
      content = content.replace("kdc = localhost:88", "kdc = " + ip + ":88");
      content = content.replace("admin_server = localhost", "admin_server = " + ip + ":749");
      FileUtils.write(new File(krb5Path), content, StandardCharsets.UTF_8);

      LOG.info("Kerberos kdc config:\n{}, path: {}", content, krb5Path);
      System.setProperty(KRB5_CONF_PROPERTY, krb5Path);
      if (Boolean.getBoolean(KRB5_DEBUG_PROPERTY)) {
        System.setProperty(KRB5_DEBUG_PROPERTY, "true");
      } else {
        System.clearProperty(KRB5_DEBUG_PROPERTY);
      }
      System.setProperty(KRB5_REALM_PROPERTY, "HADOOPKRB");
      System.setProperty(KRB5_KDC_PROPERTY, ip);

      refreshKerberosConfig();
      resetDefaultRealm();

      kerberosHiveContainer.executeInContainer(
          "hadoop", "fs", "-chown", "-R", "cli", "/user/hive/");

      IcebergRestKerberosTestEnv.tempDir = tempDir;
      return tempDir;
    } catch (Exception e) {
      reset();
      throw new RuntimeException(e);
    }
  }

  /** Restores JVM Kerberos system properties changed by {@link #init(ContainerSuite)}. */
  public static void reset() {
    if (initRefCount == 0) {
      return;
    }
    initRefCount--;
    if (initRefCount > 0) {
      return;
    }

    if (savedSystemProperties != null) {
      restoreSystemProperties(savedSystemProperties);
      savedSystemProperties = null;
      tempDir = null;
      try {
        refreshKerberosConfig();
        resetDefaultRealm();
      } catch (RuntimeException e) {
        LOG.warn("Failed to refresh Kerberos configuration after test cleanup", e);
      }
    }
  }

  /** Refreshes the JVM Kerberos configuration after updating krb5.conf. */
  public static void refreshKerberosConfig() {
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

  /** Resets Hadoop KerberosName default realm after JVM krb5 properties change. */
  public static void resetDefaultRealm() {
    try {
      String kerberosNameClass = "org.apache.hadoop.security.authentication.util.KerberosName";
      Class<?> cl = Class.forName(kerberosNameClass);
      cl.getMethod("resetDefaultRealm").invoke(null);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static Map<String, String> saveSystemProperties(String... keys) {
    Map<String, String> saved = new HashMap<>();
    for (String key : keys) {
      saved.put(key, System.getProperty(key));
    }
    return saved;
  }

  private static void restoreSystemProperties(Map<String, String> saved) {
    saved.forEach(
        (key, value) -> {
          if (value == null) {
            System.clearProperty(key);
          } else {
            System.setProperty(key, value);
          }
        });
  }
}
