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
package com.datastrato.gravitino.iceberg.common.utils;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHORIZATION;

import com.datastrato.gravitino.catalog.lakehouse.iceberg.IcebergConstants;
import com.datastrato.gravitino.iceberg.common.IcebergCatalogBackend;
import com.datastrato.gravitino.iceberg.common.IcebergConfig;
import com.datastrato.gravitino.iceberg.common.authentication.AuthenticationConfig;
import com.datastrato.gravitino.iceberg.common.authentication.kerberos.HiveBackendProxy;
import com.datastrato.gravitino.iceberg.common.authentication.kerberos.KerberosClient;
import com.google.common.base.Preconditions;
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.apache.iceberg.rest.RESTCatalog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IcebergCatalogUtil {

  private static final Logger LOG = LoggerFactory.getLogger(IcebergCatalogUtil.class);

  private static InMemoryCatalog loadMemoryCatalog(Map<String, String> properties) {
    IcebergConfig icebergConfig = new IcebergConfig(properties);
    String icebergCatalogName = icebergConfig.getCatalogBackendName("memory");
    InMemoryCatalog memoryCatalog = new InMemoryCatalog();
    Map<String, String> resultProperties = new HashMap<>(properties);
    resultProperties.put(CatalogProperties.WAREHOUSE_LOCATION, "/tmp");
    memoryCatalog.initialize(icebergCatalogName, resultProperties);
    return memoryCatalog;
  }

  private static HiveCatalog loadHiveCatalog(Map<String, String> properties) {
    HiveCatalog hiveCatalog = new HiveCatalog();
    HdfsConfiguration hdfsConfiguration = new HdfsConfiguration();
    properties.forEach(hdfsConfiguration::set);
    IcebergConfig icebergConfig = new IcebergConfig(properties);
    String icebergCatalogName = icebergConfig.getCatalogBackendName("hive");

    AuthenticationConfig authenticationConfig = new AuthenticationConfig(properties);
    if (authenticationConfig.isSimpleAuth()) {
      hiveCatalog.setConf(hdfsConfiguration);
      hiveCatalog.initialize(icebergCatalogName, properties);
      return hiveCatalog;
    } else if (authenticationConfig.isKerberosAuth()) {
      Map<String, String> resultProperties = new HashMap<>(properties);
      resultProperties.put(CatalogProperties.CLIENT_POOL_CACHE_KEYS, "USER_NAME");
      hdfsConfiguration.set(HADOOP_SECURITY_AUTHORIZATION, "true");
      hdfsConfiguration.set(HADOOP_SECURITY_AUTHENTICATION, "kerberos");
      hiveCatalog.setConf(hdfsConfiguration);
      hiveCatalog.initialize(icebergCatalogName, properties);

      String realm = initKerberosAndReturnRealm(properties, hdfsConfiguration);
      if (authenticationConfig.isImpersonationEnabled()) {
        HiveBackendProxy proxyHiveCatalog =
            new HiveBackendProxy(resultProperties, hiveCatalog, realm);
        return proxyHiveCatalog.getProxy();
      }

      return hiveCatalog;
    } else {
      throw new UnsupportedOperationException(
          "Unsupported authentication method: " + authenticationConfig.getAuthType());
    }
  }

  private static String initKerberosAndReturnRealm(
      Map<String, String> properties, Configuration conf) {
    try {
      KerberosClient kerberosClient = new KerberosClient(properties, conf);
      File keytabFile =
          kerberosClient.saveKeyTabFileFromUri(Long.valueOf(properties.get("catalog_uuid")));
      return kerberosClient.login(keytabFile.getAbsolutePath());
    } catch (IOException e) {
      throw new RuntimeException("Failed to login with kerberos", e);
    }
  }

  private static JdbcCatalog loadJdbcCatalog(Map<String, String> properties) {
    IcebergConfig icebergConfig = new IcebergConfig(properties);
    String driverClassName = icebergConfig.getJdbcDriver();
    String icebergCatalogName = icebergConfig.getCatalogBackendName("jdbc");

    Preconditions.checkNotNull(
        properties.get(IcebergConstants.ICEBERG_JDBC_USER),
        IcebergConstants.ICEBERG_JDBC_USER + " is null");
    Preconditions.checkNotNull(
        properties.get(IcebergConstants.ICEBERG_JDBC_PASSWORD),
        IcebergConstants.ICEBERG_JDBC_PASSWORD + " is null");

    try {
      // Load the jdbc driver
      Class.forName(driverClassName);
    } catch (ClassNotFoundException e) {
      throw new IllegalArgumentException("Couldn't load jdbc driver " + driverClassName);
    }
    JdbcCatalog jdbcCatalog =
        new JdbcCatalog(
            null,
            null,
            Boolean.parseBoolean(
                properties.getOrDefault(IcebergConstants.ICEBERG_JDBC_INITIALIZE, "true")));
    HdfsConfiguration hdfsConfiguration = new HdfsConfiguration();
    properties.forEach(hdfsConfiguration::set);
    jdbcCatalog.setConf(hdfsConfiguration);
    jdbcCatalog.initialize(icebergCatalogName, properties);
    return jdbcCatalog;
  }

  private static Catalog loadRestCatalog(Map<String, String> properties) {
    IcebergConfig icebergConfig = new IcebergConfig(properties);
    String icebergCatalogName = icebergConfig.getCatalogBackendName("rest");
    RESTCatalog restCatalog = new RESTCatalog();
    HdfsConfiguration hdfsConfiguration = new HdfsConfiguration();
    properties.forEach(hdfsConfiguration::set);
    restCatalog.setConf(hdfsConfiguration);
    restCatalog.initialize(icebergCatalogName, properties);
    return restCatalog;
  }

  public static Catalog loadCatalogBackend(String catalogType) {
    return loadCatalogBackend(catalogType, Collections.emptyMap());
  }

  public static Catalog loadCatalogBackend(String catalogType, Map<String, String> properties) {
    LOG.info("Load catalog backend of {}", catalogType);
    switch (IcebergCatalogBackend.valueOf(catalogType.toUpperCase())) {
      case MEMORY:
        return loadMemoryCatalog(properties);
      case HIVE:
        return loadHiveCatalog(properties);
      case JDBC:
        return loadJdbcCatalog(properties);
      case REST:
        return loadRestCatalog(properties);
      default:
        throw new RuntimeException(
            catalogType
                + " catalog is not supported yet, supported catalogs: [memory]"
                + catalogType);
    }
  }

  private IcebergCatalogUtil() {}
}
