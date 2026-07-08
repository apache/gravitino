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

package org.apache.gravitino.iceberg.common;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergConstants;
import org.apache.gravitino.iceberg.common.authentication.AuthenticationConfig;
import org.apache.gravitino.iceberg.common.authentication.kerberos.KerberosConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.iceberg.CatalogProperties;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestClosableJdbcCatalog {

  @TempDir private Path warehouse;

  @Test
  void testSimpleAuthInitializeWithoutKerberos() {
    ClosableJdbcCatalog catalog = new ClosableJdbcCatalog();
    Configuration conf = new HdfsConfiguration();
    catalog.setConf(conf);
    catalog.initialize("test", newJdbcCatalogProperties());

    Assertions.assertDoesNotThrow(catalog::close);
  }

  @Test
  void testKerberosInitializeRequiresHadoopConf() {
    ClosableJdbcCatalog catalog = new ClosableJdbcCatalog();
    Map<String, String> properties = newJdbcCatalogProperties();
    properties.put(AuthenticationConfig.AUTH_TYPE_KEY, "kerberos");
    properties.put(KerberosConfig.PRINCIPAL_KEY, "cli@HADOOPKRB");
    properties.put(KerberosConfig.KET_TAB_URI_KEY, "/tmp/missing.keytab");

    Assertions.assertThrows(RuntimeException.class, () -> catalog.initialize("test", properties));
  }

  @Test
  void testKerberosInitializeFailsWithInvalidKeytab() {
    ClosableJdbcCatalog catalog = new ClosableJdbcCatalog();
    Configuration conf = new HdfsConfiguration();
    catalog.setConf(conf);

    Map<String, String> properties = newJdbcCatalogProperties();
    properties.put(AuthenticationConfig.AUTH_TYPE_KEY, "kerberos");
    properties.put(KerberosConfig.PRINCIPAL_KEY, "cli@HADOOPKRB");
    properties.put(KerberosConfig.KET_TAB_URI_KEY, "/tmp/missing.keytab");

    RuntimeException exception =
        Assertions.assertThrows(
            RuntimeException.class, () -> catalog.initialize("test", properties));
    Assertions.assertTrue(exception.getMessage().contains("Failed to login with kerberos"));
  }

  @Test
  void testSimpleKerberosOps() throws Throwable {
    ClosableJdbcCatalog catalog = new ClosableJdbcCatalog();
    catalog.setConf(new HdfsConfiguration());
    catalog.initialize("test", newJdbcCatalogProperties());

    Assertions.assertEquals("ok", catalog.doKerberosOperations(() -> "ok"));
  }

  @Test
  void testImpersonationEnableConfigValues() {
    Map<String, String> properties = newJdbcCatalogProperties();
    properties.put(AuthenticationConfig.IMPERSONATION_ENABLE_KEY, "true");
    Assertions.assertTrue(new AuthenticationConfig(properties).isImpersonationEnabled());

    properties.put(AuthenticationConfig.IMPERSONATION_ENABLE_KEY, "false");
    Assertions.assertFalse(new AuthenticationConfig(properties).isImpersonationEnabled());
  }

  @Test
  void testKerberosOpsWithImpersonationDisabled() throws Throwable {
    ClosableJdbcCatalog catalog = new ClosableJdbcCatalog();
    catalog.setConf(new HdfsConfiguration());

    Map<String, String> properties = newJdbcCatalogProperties();
    properties.put(AuthenticationConfig.IMPERSONATION_ENABLE_KEY, "false");
    catalog.initialize("test", properties);

    Assertions.assertEquals("ok", catalog.doKerberosOperations(() -> "ok"));
  }

  @Test
  void testKerberosOpsNoClient() {
    ClosableJdbcCatalog catalog = new ClosableJdbcCatalog();
    Configuration conf = new HdfsConfiguration();
    catalog.setConf(conf);

    Map<String, String> properties = newJdbcCatalogProperties();
    properties.put(AuthenticationConfig.AUTH_TYPE_KEY, "kerberos");
    properties.put(KerberosConfig.PRINCIPAL_KEY, "cli@HADOOPKRB");
    properties.put(KerberosConfig.KET_TAB_URI_KEY, "/tmp/missing.keytab");

    Assertions.assertThrows(RuntimeException.class, () -> catalog.initialize("test", properties));
    Assertions.assertThrows(
        IllegalStateException.class, () -> catalog.doKerberosOperations(() -> "ok"));
  }

  private Map<String, String> newJdbcCatalogProperties() {
    Map<String, String> properties = new HashMap<>();
    properties.put(CatalogProperties.URI, "jdbc:sqlite::memory:");
    properties.put(CatalogProperties.WAREHOUSE_LOCATION, warehouse.toString());
    properties.put(IcebergConstants.GRAVITINO_JDBC_DRIVER, "org.sqlite.JDBC");
    properties.put(IcebergConstants.ICEBERG_JDBC_USER, "test");
    properties.put(IcebergConstants.ICEBERG_JDBC_PASSWORD, "test");
    properties.put(IcebergConstants.ICEBERG_JDBC_INITIALIZE, "true");
    return properties;
  }
}
