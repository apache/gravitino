/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.lakehouse.paimon.integration.test;

import static com.datastrato.gravitino.catalog.lakehouse.paimon.authentication.AuthenticationConfig.AUTH_TYPE_KEY;
import static com.datastrato.gravitino.catalog.lakehouse.paimon.authentication.kerberos.KerberosConfig.KEY_TAB_URI_KEY;
import static com.datastrato.gravitino.catalog.lakehouse.paimon.authentication.kerberos.KerberosConfig.PRINCIPAL_KEY;
import static com.datastrato.gravitino.connector.BaseCatalog.CATALOG_BYPASS_PREFIX;

import com.datastrato.gravitino.catalog.lakehouse.paimon.PaimonCatalogPropertiesMetadata;
import com.datastrato.gravitino.integration.test.container.ContainerSuite;
import com.datastrato.gravitino.integration.test.container.HiveContainer;
import com.google.common.collect.Maps;
import java.io.File;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.util.KerberosName;
import org.apache.hadoop.security.authentication.util.KerberosUtil;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tag("gravitino-docker-it")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class CatalogPaimonKerberosFilesystemIT extends CatalogPaimonBaseIT {

  private static final Logger LOG =
      LoggerFactory.getLogger(CatalogPaimonKerberosFilesystemIT.class);

  private static final ContainerSuite containerSuite = ContainerSuite.getInstance();

  private static final String FILESYSTEM_CLIENT_PRINCIPAL = "cli@HADOOPKRB";
  private static final String FILESYSTEM_CLIENT_KEYTAB = "/client.keytab";

  private static String TMP_DIR;

  private static HiveContainer kerberosHiveContainer;

  //  private static String URIS;
  private static String TYPE;
  private static String WAREHOUSE;

  @Override
  protected void startHiveContainer() {
    containerSuite.startKerberosHiveContainer();
    kerberosHiveContainer = containerSuite.getKerberosHiveContainer();
  }

  @Override
  protected Map<String, String> initPaimonCatalogProperties() {
    Map<String, String> catalogProperties = Maps.newHashMap();
    catalogProperties.put("key1", "val1");
    catalogProperties.put("key2", "val2");

    TYPE = "filesystem";
    WAREHOUSE =
            String.format(
                    "hdfs://%s:%d/user/hive/paimon_catalog_warehouse/",
                    kerberosHiveContainer.getContainerIpAddress(), HiveContainer.HDFS_DEFAULTFS_PORT);
    catalogProperties.put(PaimonCatalogPropertiesMetadata.GRAVITINO_CATALOG_BACKEND, TYPE);
    catalogProperties.put(PaimonCatalogPropertiesMetadata.WAREHOUSE, WAREHOUSE);
    catalogProperties.put(AUTH_TYPE_KEY, "kerberos");
    catalogProperties.put(PRINCIPAL_KEY, FILESYSTEM_CLIENT_PRINCIPAL);
    catalogProperties.put(KEY_TAB_URI_KEY, TMP_DIR + FILESYSTEM_CLIENT_KEYTAB);
    catalogProperties.put(CATALOG_BYPASS_PREFIX + "fs.defaultFS", defaultBaseLocation());

    // to prepare related kerberos configs
    try {
      File baseDir = new File(System.getProperty("java.io.tmpdir"));
      File file = Files.createTempDirectory(baseDir.toPath(), "test").toFile();
      file.deleteOnExit();
      TMP_DIR = file.getAbsolutePath();

      // Prepare kerberos related-config;
      prepareKerberosConfig();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    return catalogProperties;
  }

  @Override
  protected void cleanUp() {
    clearTableAndSchema();
    metalake.dropCatalog(catalogName);
    client.dropMetalake(metalakeName);

    // Reset the UGI
    UserGroupInformation.reset();

    LOG.info("krb5 path: {}", System.getProperty("java.security.krb5.conf"));
    // Clean up the kerberos configuration
    System.clearProperty("java.security.krb5.conf");
    System.clearProperty("sun.security.krb5.debug");
  }

  @Override
  protected void initSparkEnv() {
    spark =
        SparkSession.builder()
            .master("local[1]")
            .appName("Paimon Catalog integration test")
            .config("spark.sql.warehouse.dir", WAREHOUSE)
            .config("spark.sql.catalog.paimon", "org.apache.paimon.spark.SparkCatalog")
            .config("spark.sql.catalog.paimon.warehouse", WAREHOUSE)
            .config(
                "spark.sql.extensions",
                "org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions")
            .config("spark.kerberos.principal", FILESYSTEM_CLIENT_PRINCIPAL)
            .config("spark.kerberos.keytab", TMP_DIR + FILESYSTEM_CLIENT_KEYTAB)
            .getOrCreate();
  }

  private static void prepareKerberosConfig() throws Exception {
    // Keytab of Gravitino server to connect to HDFS
    kerberosHiveContainer
        .getContainer()
        .copyFileFromContainer("/etc/admin.keytab", TMP_DIR + FILESYSTEM_CLIENT_KEYTAB);

    String tmpKrb5Path = TMP_DIR + "/krb5.conf_tmp";
    String krb5Path = TMP_DIR + "/krb5.conf";
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

    refreshKerberosConfig();
    KerberosName.resetDefaultRealm();

    LOG.info("Kerberos default realm: {}", KerberosUtil.getDefaultRealm());
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

  private static String defaultBaseLocation() {
    return String.format(
        "hdfs://%s:%d",
        kerberosHiveContainer.getContainerIpAddress(), HiveContainer.HDFS_DEFAULTFS_PORT);
  }
}
