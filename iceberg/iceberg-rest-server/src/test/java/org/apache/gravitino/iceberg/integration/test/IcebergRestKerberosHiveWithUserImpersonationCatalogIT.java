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
import javax.annotation.Nullable;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.api.condition.EnabledIf;

@Tag("gravitino-docker-test")
@TestInstance(Lifecycle.PER_CLASS)
@EnabledIf("isEmbedded")
public class IcebergRestKerberosHiveWithUserImpersonationCatalogIT
    extends IcebergRestKerberosHiveCatalogIT {

  private static final String NORMAL_USER = "normal";

  public IcebergRestKerberosHiveWithUserImpersonationCatalogIT() {
    super();
  }

  @BeforeAll
  void prepareSQLContext() {
    containerSuite
        .getKerberosHiveContainer()
        .executeInContainer("hadoop", "fs", "-chown", "-R", NORMAL_USER, "/user/hive/");

    super.prepareSQLContext();
  }

  @Override
  Map<String, String> getCatalogConfig() {
    Map<String, String> superConfig = super.getCatalogConfig();
    Map<String, String> configMap = new HashMap<>(superConfig);

    configMap.put("gravitino.iceberg-rest.authentication.impersonation-enable", "true");
    return configMap;
  }

  @Override
  protected void initSparkEnv() {
    int port = getServerPort();
    LOG.info("Iceberg REST server port:{}", port);
    String icebergRESTUri = String.format("http://127.0.0.1:%d/iceberg/", port);
    SparkConf sparkConf =
        new SparkConf()
            .set(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .set("spark.sql.catalog.rest", "org.apache.iceberg.spark.SparkCatalog")
            .set("spark.sql.catalog.rest.type", "rest")
            .set("spark.sql.catalog.rest.uri", icebergRESTUri)
            .set("spark.sql.catalog.rest.rest.auth.type", "basic")
            .set("spark.sql.catalog.rest.rest.auth.basic.username", NORMAL_USER)
            .set("spark.sql.catalog.rest.rest.auth.basic.password", "mock")
            // drop Iceberg table purge may hang in spark local mode
            .set("spark.locality.wait.node", "0");

    if (supportsCredentialVending()) {
      sparkConf.set(
          "spark.sql.catalog.rest.header.X-Iceberg-Access-Delegation", "vended-credentials");
    }

    sparkSession = SparkSession.builder().master("local[1]").config(sparkConf).getOrCreate();
  }

  protected String getTestNamespace(@Nullable String childNamespace) {
    String separator;
    String parentNamespace;

    if (supportsNestedNamespaces()) {
      parentNamespace = "iceberg_rest.nested.table_test";
      separator = ".";
    } else {
      parentNamespace = "iceberg_rest_with_kerberos_impersonation_table_test";
      separator = "_";
    }

    if (childNamespace != null) {
      return parentNamespace + separator + childNamespace;
    } else {
      return parentNamespace;
    }
  }

  // Disable the following three tests as they contain data insert operations and which are not
  // controlled by the Gravitino Iceberg REST server currently.
  @Test
  @Disabled
  void testDML() {}

  @Test
  @Disabled
  void testRegisterTable() {}

  @Test
  @Disabled
  void testSnapshot() {}
}
