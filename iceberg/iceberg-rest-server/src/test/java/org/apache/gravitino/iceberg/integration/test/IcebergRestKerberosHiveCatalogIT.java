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

import java.util.HashMap;
import java.util.Map;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergCatalogBackend;
import org.apache.gravitino.iceberg.common.IcebergConfig;
import org.apache.gravitino.integration.test.container.FixtureContainer;
import org.apache.gravitino.integration.test.container.HiveAuth;
import org.apache.gravitino.integration.test.container.HiveContainer;
import org.apache.gravitino.integration.test.container.HiveFixture;
import org.apache.gravitino.integration.test.util.GravitinoITUtils;
import org.apache.spark.SparkConf;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.api.condition.DisabledIf;

@Tag("gravitino-docker-test")
@TestInstance(Lifecycle.PER_CLASS)
@HiveFixture(auth = HiveAuth.KERBEROS)
public class IcebergRestKerberosHiveCatalogIT extends IcebergRESTHiveCatalogIT {

  protected static final String HIVE_METASTORE_CLIENT_PRINCIPAL = "cli@HADOOPKRB";

  protected static String tempDir;

  // Replaces the superclass's plain Hive fixture, so the inherited hive field holds this same
  // container; the name says which Hive the readers below expect.
  @FixtureContainer protected HiveContainer kerberosHive;

  public IcebergRestKerberosHiveCatalogIT() {
    super();
  }

  void initEnv() {
    tempDir = IcebergRestKerberosTestEnv.init(kerberosHive);
  }

  @Override
  protected void afterIcebergRESTServerStopped() {
    IcebergRestKerberosTestEnv.reset();
  }

  @Override
  Map<String, String> getCatalogConfig() {
    Map<String, String> configMap = new HashMap<>();

    configMap.put("gravitino.iceberg-rest.authentication.type", "kerberos");
    configMap.put(
        "gravitino.iceberg-rest.authentication.kerberos.principal",
        HIVE_METASTORE_CLIENT_PRINCIPAL);
    configMap.put(
        "gravitino.iceberg-rest.authentication.kerberos.keytab-uri",
        tempDir + IcebergRestKerberosTestEnv.CLIENT_KEYTAB);
    configMap.put("gravitino.iceberg-rest.hive.metastore.sasl.enabled", "true");
    configMap.put(
        "gravitino.iceberg-rest.hive.metastore.kerberos.principal",
        "hive/_HOST@HADOOPKRB".replace("_HOST", kerberosHive.getHostName()));

    configMap.put("gravitino.iceberg-rest.hadoop.security.authentication", "kerberos");

    configMap.put(
        "gravitino.iceberg-rest.dfs.namenode.kerberos.principal",
        "hdfs/_HOST@HADOOPKRB".replace("_HOST", kerberosHive.getHostName()));

    configMap.put(
        IcebergConfig.ICEBERG_CONFIG_PREFIX + IcebergConfig.CATALOG_BACKEND.getKey(),
        IcebergCatalogBackend.HIVE.toString().toLowerCase());

    configMap.put(
        IcebergConfig.ICEBERG_CONFIG_PREFIX + IcebergConfig.CATALOG_URI.getKey(),
        String.format(
            "thrift://%s:%d",
            kerberosHive.getContainerIpAddress(), HiveContainer.HIVE_METASTORE_PORT));

    configMap.put(
        IcebergConfig.ICEBERG_CONFIG_PREFIX + IcebergConfig.CATALOG_WAREHOUSE.getKey(),
        GravitinoITUtils.genRandomName(
            String.format(
                "hdfs://%s:%d/user/hive/warehouse-hive",
                kerberosHive.getContainerIpAddress(), HiveContainer.HDFS_DEFAULTFS_PORT)));
    return configMap;
  }

  @Override
  protected void customizeSparkConf(SparkConf sparkConf) {
    IcebergRestKerberosTestEnv.configureSparkKerberos(
        sparkConf, HIVE_METASTORE_CLIENT_PRINCIPAL, kerberosHive.getHostName());
  }

  // Spark writes data files to HDFS directly in these tests; keep them in embedded mode only.
  @Test
  @DisabledIf(
      "org.apache.gravitino.iceberg.integration.test.IcebergRestKerberosTestEnv#isDeployMode")
  @Override
  void testDML() {
    super.testDML();
  }

  @Test
  @DisabledIf(
      "org.apache.gravitino.iceberg.integration.test.IcebergRestKerberosTestEnv#isDeployMode")
  @Override
  void testRegisterTable() {
    super.testRegisterTable();
  }

  @Test
  @DisabledIf(
      "org.apache.gravitino.iceberg.integration.test.IcebergRestKerberosTestEnv#isDeployMode")
  @Override
  void testSnapshot() {
    super.testSnapshot();
  }

  @Test
  @DisabledIf(
      "org.apache.gravitino.iceberg.integration.test.IcebergRestKerberosTestEnv#isDeployMode")
  @Override
  void testRegisterTableOverwrite() throws Exception {
    super.testRegisterTableOverwrite();
  }
}
