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
package org.apache.gravitino.spark.connector.integration.test.glue;

import java.io.IOException;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.spark.connector.GravitinoSparkConfig;
import org.apache.gravitino.spark.connector.integration.test.SparkCommonIT;
import org.apache.gravitino.spark.connector.plugin.GravitinoSparkPlugin;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base environment setup for Spark Glue connector integration tests.
 *
 * <p>Replaces the Hive/HDFS initialization chain from SparkEnvIT with Glue-appropriate setup. Glue
 * does not require Hive metastore or HDFS — it uses AWS Glue API (mocked by LocalStack) and S3
 * storage.
 *
 * <p>Subclasses must:
 *
 * <ul>
 *   <li>Start LocalStack container in their {@link #startUp()} and call {@link
 *       SparkGlueCatalogIT#setGlueEndpoint(String)}
 *   <li>Set AWS credentials via {@link SparkGlueCatalogIT#setAwsCredentials(String, String)}
 *   <li>Set S3 credentials via {@link #setS3Credentials(String, String, String)}
 *   <li>Call {@code super.startUp()} after configuring Glue
 * </ul>
 */
public abstract class SparkGlueEnvIT extends SparkCommonIT {

  private static final Logger LOG = LoggerFactory.getLogger(SparkGlueEnvIT.class);

  private SparkSession sparkSession;

  private String s3AccessKey;
  private String s3SecretKey;
  private String s3Endpoint;
  private String s3BucketName = S3_BUCKET_NAME;

  protected static final String S3_BUCKET_NAME = "ice-glue-test-01";
  protected static final int DEFAULT_GRAVITINO_PORT = 8090;

  @Override
  protected SparkSession getSparkSession() {
    return sparkSession;
  }

  /**
   * Starts the test environment. Subclasses must call super.startUp() after configuring the Glue
   * endpoint and credentials.
   */
  @BeforeAll
  protected void startUp() throws Exception {
    warehouse = "s3a://" + s3BucketName + "/warehouse";
    hiveMetastoreUri = null;
    hdfs = null;

    int gravitinoPort;
    boolean serverWasStartedByThisClass = false;
    if (serverConfig != null) {
      // Gravitino server already started by an external process (e.g., real AWS integration
      // test environment). Use existing config.
      gravitinoPort = getGravitinoServerPort();
    } else {
      // Try to start Gravitino via reflection, bypassing the empty @BeforeAll overrides in
      // SparkEnvIT/SparkUtilIT. If this fails (e.g., no Docker for MiniGravitino), fall back to
      // assuming Gravitino is externally provided on the default port.
      try {
        java.lang.reflect.Method baseStartIntegrationTest =
            org.apache.gravitino.integration.test.util.BaseIT.class.getDeclaredMethod(
                "startIntegrationTest");
        baseStartIntegrationTest.setAccessible(true);
        baseStartIntegrationTest.invoke(this);
        // serverConfig may still be null even after a successful call (e.g., Gravitino
        // started externally while reflection succeeded). Guard against NPE here.
        if (serverConfig != null) {
          gravitinoPort = getGravitinoServerPort();
          serverWasStartedByThisClass = true;
        } else {
          LOG.warn(
              "BaseIT.startIntegrationTest() completed but serverConfig is still null. "
                  + "Assuming externally-provided Gravitino on port {}.",
              DEFAULT_GRAVITINO_PORT);
          gravitinoPort = DEFAULT_GRAVITINO_PORT;
        }
      } catch (java.lang.reflect.InvocationTargetException e) {
        LOG.warn(
            "Failed to start Gravitino via BaseIT.startIntegrationTest(), "
                + "assuming Gravitino is externally provided on port {}. Reason: {}",
            DEFAULT_GRAVITINO_PORT,
            e.getCause() != null ? e.getCause().getMessage() : e.getMessage());
        gravitinoPort = DEFAULT_GRAVITINO_PORT;
      } catch (ReflectiveOperationException e) {
        LOG.warn(
            "Reflection failed to invoke BaseIT.startIntegrationTest(), "
                + "assuming Gravitino is externally provided on port {}. Reason: {}",
            DEFAULT_GRAVITINO_PORT,
            e.getMessage());
        gravitinoPort = DEFAULT_GRAVITINO_PORT;
      }
    }
    String gravitinoUri = String.format("http://127.0.0.1:%d", gravitinoPort);
    // Only initialize metalake and catalog if this class started Gravitino itself.
    // For externally-provided Gravitino (e.g., real AWS tests), metalake and catalog
    // must already exist.
    if (serverWasStartedByThisClass) {
      initMetalakeAndCatalogs();
    }
    initSparkEnv(gravitinoUri);
    // Create the default database in the Glue catalog so that tests can USE it.
    // Note: SparkCommonIT.initDefaultDatabase() is package-private and cannot be @Override'd
    // from this package, so we call this class's own implementation directly.
    initDefaultDatabase();

    LOG.info(
        "Startup Spark env for Glue successfully, Gravitino uri: {}, Warehouse: {}",
        gravitinoUri,
        warehouse);
  }

  @BeforeEach
  protected void init() {
    // Spark's Hive-style catalog integration treats USE <name> as switching databases,
    // not catalogs. We must use SET to set the default catalog instead.
    sql("SET spark.sql.defaultCatalog=" + getCatalogName());
    sql("USE " + getDefaultDatabase());
  }

  protected String getDefaultAwsRegion() {
    return "us-east-1";
  }

  /**
   * Skips the Hive container initialization. Glue uses AWS Glue API (catalog) and S3 (storage), not
   * a Hive metastore or HDFS. This method shadows the private {@link SparkEnvIT#initHiveEnv()} from
   * the grandparent class — no @Override since the parent method is private (not inheritable).
   */
  protected void initHiveEnv() {
    // Skip Hive container startup. Glue uses AWS Glue API + S3, not HMS.
    // warehouse and hiveMetastoreUri were already set in startUp():
    // - warehouse = "s3a://bucket/warehouse" (already set)
    // - hiveMetastoreUri = null (already set)
  }

  /**
   * Overrides HDFS filesystem initialization. Glue uses S3, not HDFS, so this is a no-op. Note:
   * no @Override because the parent method is private (not inheritable).
   */
  protected void initHdfsFileSystem() {
    // Glue uses S3, not HDFS. hdfs is already set to null in startUp().
  }

  /**
   * Shadows parent's HDFS-based database initialization. The parent {@link
   * org.apache.gravitino.spark.connector.integration.test.SparkCommonIT} creates a database with an
   * HDFS location ('/user/hive/{db}') which is wrong for Glue. This implementation creates the
   * database with the correct S3 location.
   *
   * <p>Called explicitly from {@link #startUp()}, not via JUnit lifecycle inheritance.
   */
  protected void initDefaultDatabase() {
    String defaultDbName = getDefaultDatabase();
    String dbLocation = warehouse + "/" + defaultDbName;
    // Create in spark_catalog (Derby) first so HiveTableCatalog.loadTable() can validate the
    // schema.
    // GravitinoGlueCatalog delegates non-Iceberg table operations to HiveTableCatalog (Kyuubi),
    // which internally calls Derby's SessionCatalog.requireDbExists() during loadTable().
    sql("SET spark.sql.defaultCatalog=spark_catalog");
    sql(String.format("CREATE DATABASE IF NOT EXISTS %s LOCATION '%s'", defaultDbName, dbLocation));
    // Also create in Glue catalog for Gravitino metadata operations.
    sql("SET spark.sql.defaultCatalog=" + getCatalogName());
    sql(String.format("CREATE DATABASE IF NOT EXISTS %s LOCATION '%s'", defaultDbName, dbLocation));
  }

  /**
   * Overrides HDFS-based directory check. Glue uses S3 storage, not HDFS. Uses S3A filesystem to
   * verify directory existence on S3.
   */
  @Override
  protected void checkDirExists(Path dir) {
    try {
      Configuration conf = newS3Config();
      FileSystem fs = FileSystem.get(dir.toUri(), conf);
      boolean exists = fs.exists(dir);
      fs.close();
      org.junit.jupiter.api.Assertions.assertTrue(exists, "S3 directory not exists: " + dir);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Overrides HDFS-based file existence check. Glue uses S3 storage, not HDFS. Uses S3A filesystem
   * to verify that at least one data file exists in the partition directory.
   */
  @Override
  protected void checkDataFileExists(Path dir) {
    try {
      Configuration conf = newS3Config();
      FileSystem fs = FileSystem.get(dir.toUri(), conf);
      FileStatus[] files = fs.listStatus(dir);
      boolean hasFile = false;
      for (FileStatus file : files) {
        if (file.isFile()) {
          hasFile = true;
          break;
        }
      }
      fs.close();
      org.junit.jupiter.api.Assertions.assertTrue(hasFile, "No data file found in: " + dir);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @AfterAll
  protected void stop() throws IOException, InterruptedException {
    if (sparkSession != null) {
      sparkSession.close();
    }
    super.stopIntegrationTest();
  }

  /**
   * Deletes a directory (file or directory) from S3 if it exists. Overrides the HDFS-based
   * implementation in SparkCommonIT since Glue uses S3 storage instead of HDFS.
   */
  @Override
  protected void deleteDirIfExists(String path) {
    try {
      Configuration conf = newS3Config();
      Path dir = new Path(path);
      FileSystem fs = FileSystem.get(dir.toUri(), conf);
      if (fs.exists(dir)) {
        fs.delete(dir, true);
      }
      fs.close();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /** Creates a Hadoop Configuration configured for S3A access with the test credentials. */
  private Configuration newS3Config() {
    Configuration conf = new Configuration();
    conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
    conf.set("fs.s3a.access.key", s3AccessKey);
    conf.set("fs.s3a.secret.key", s3SecretKey);
    conf.set(
        "fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider");
    if (s3Endpoint != null) {
      conf.set("fs.s3a.endpoint", s3Endpoint);
      conf.set("fs.s3a.path.style.access", "true");
      conf.set("fs.s3a.connection.ssl.enabled", "false");
    }
    return conf;
  }

  private void initMetalakeAndCatalogs() {
    client.createMetalake(metalakeName, "", java.util.Collections.emptyMap());
    GravitinoMetalake metalake = client.loadMetalake(metalakeName);
    metalake.createCatalog(
        getCatalogName(),
        org.apache.gravitino.Catalog.Type.RELATIONAL,
        getProvider(),
        "",
        getCatalogConfigs());
  }

  private void initSparkEnv(String gravitinoUri) {
    String awsRegion = getDefaultAwsRegion();
    // Set AWS region as system properties so the AWS SDK's DefaultAwsRegionProviderChain
    // can find it. Without this, S3A operations fail with "Unable to find a region".
    // aws.region is checked first by DefaultAwsRegionProviderChain; AWS_DEFAULT_REGION is
    // fallback for env-var-based provider.
    if (awsRegion != null && !awsRegion.isEmpty()) {
      System.setProperty("AWS_DEFAULT_REGION", awsRegion);
      System.setProperty("aws.region", awsRegion);
    }

    SparkConf sparkConf =
        new SparkConf()
            .set("spark.plugins", GravitinoSparkPlugin.class.getName())
            .set(GravitinoSparkConfig.GRAVITINO_URI, gravitinoUri)
            .set(GravitinoSparkConfig.GRAVITINO_METALAKE, metalakeName)
            .set(GravitinoSparkConfig.GRAVITINO_ENABLE_ICEBERG_SUPPORT, "true")
            .set(GravitinoSparkConfig.GRAVITINO_ENABLE_PAIMON_SUPPORT, "true")
            .set("hive.exec.dynamic.partition.mode", "nonstrict")
            .set("spark.sql.warehouse.dir", warehouse)
            .set("spark.sql.session.timeZone", TIME_ZONE_UTC)
            // S3A filesystem configuration
            .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .set("spark.hadoop.fs.s3a.access.key", s3AccessKey)
            .set("spark.hadoop.fs.s3a.secret.key", s3SecretKey)
            .set(
                "spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider");
    // Only set endpoint for custom S3 services (e.g., LocalStack).
    // For real AWS, omit this to use the default AWS endpoint.
    if (s3Endpoint != null) {
      sparkConf.set("spark.hadoop.fs.s3a.endpoint", s3Endpoint);
      // LocalStack uses HTTP with path-style access
      sparkConf.set("spark.hadoop.fs.s3a.path.style.access", "true");
      sparkConf.set("spark.hadoop.fs.s3a.connection.ssl.enabled", "false");
    } else if (awsRegion != null && !awsRegion.isEmpty()) {
      // For real AWS S3 (no custom endpoint), configure the region so S3A can
      // determine the correct AWS partition and endpoint without an explicit URL.
      sparkConf.set("spark.hadoop.fs.s3a.endpoint.region", awsRegion);
    }
    sparkSession =
        SparkSession.builder()
            .master("local[1]")
            .appName("Spark connector Glue integration test")
            .config(sparkConf)
            .enableHiveSupport()
            .getOrCreate();
  }

  /**
   * Sets S3 credentials for Spark S3A filesystem access.
   *
   * @param endpoint S3 endpoint URL (e.g., http://localhost:4566)
   * @param accessKey S3 access key
   * @param secretKey S3 secret key
   */
  public void setS3Credentials(String endpoint, String accessKey, String secretKey) {
    this.s3Endpoint = endpoint;
    this.s3AccessKey = accessKey;
    this.s3SecretKey = secretKey;
  }

  public void setS3BucketName(String bucketName) {
    this.s3BucketName = bucketName;
  }

  private final String metalakeName = "test";
}
