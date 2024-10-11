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
package org.apache.gravitino.iceberg.common.utils;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHORIZATION;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergConstants;
import org.apache.gravitino.iceberg.common.ClosableHiveCatalog;
import org.apache.gravitino.iceberg.common.IcebergCatalogBackend;
import org.apache.gravitino.iceberg.common.IcebergConfig;
import org.apache.gravitino.iceberg.common.authentication.AuthenticationConfig;
import org.apache.gravitino.iceberg.common.authentication.kerberos.HiveBackendProxy;
import org.apache.gravitino.iceberg.common.authentication.kerberos.KerberosClient;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.apache.iceberg.rest.RESTCatalog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IcebergCatalogUtil {

  private static final Logger LOG = LoggerFactory.getLogger(IcebergCatalogUtil.class);

  private static InMemoryCatalog loadMemoryCatalog(IcebergConfig icebergConfig) {
    String icebergCatalogName = icebergConfig.getCatalogBackendName();
    InMemoryCatalog memoryCatalog = new InMemoryCatalog();
    Map<String, String> resultProperties = icebergConfig.getIcebergCatalogProperties();
    resultProperties.put(CatalogProperties.WAREHOUSE_LOCATION, "/tmp");
    memoryCatalog.initialize(icebergCatalogName, resultProperties);
    return memoryCatalog;
  }

  private static HiveCatalog loadHiveCatalog(IcebergConfig icebergConfig) {
    ClosableHiveCatalog hiveCatalog = new ClosableHiveCatalog();
    HdfsConfiguration hdfsConfiguration = new HdfsConfiguration();
    String icebergCatalogName = icebergConfig.getCatalogBackendName();

    Map<String, String> properties = icebergConfig.getIcebergCatalogProperties();
    properties.forEach(hdfsConfiguration::set);
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

      KerberosClient kerberosClient = initKerberosAndReturnClient(properties, hdfsConfiguration);
      hiveCatalog.addResource(kerberosClient);
      if (authenticationConfig.isImpersonationEnabled()) {
        HiveBackendProxy proxyHiveCatalog =
            new HiveBackendProxy(resultProperties, hiveCatalog, kerberosClient.getRealm());
        return proxyHiveCatalog.getProxy();
      }

      return hiveCatalog;
    } else {
      throw new UnsupportedOperationException(
          "Unsupported authentication method: " + authenticationConfig.getAuthType());
    }
  }

  private static KerberosClient initKerberosAndReturnClient(
      Map<String, String> properties, Configuration conf) {
    try {
      KerberosClient kerberosClient = new KerberosClient(properties, conf);

      // For Iceberg rest server, we haven't set the catalog_uuid, so we set it to 0 as there is
      // only one catalog in the rest server, so it's okay to set it to 0.
      String catalogUUID = properties.getOrDefault("catalog_uuid", "0");
      File keytabFile = kerberosClient.saveKeyTabFileFromUri(Long.valueOf(catalogUUID));
      kerberosClient.login(keytabFile.getAbsolutePath());
      return kerberosClient;
    } catch (IOException e) {
      throw new RuntimeException("Failed to login with kerberos", e);
    }
  }

  private static JdbcCatalog loadJdbcCatalog(IcebergConfig icebergConfig) {
    String driverClassName = icebergConfig.getJdbcDriver();
    String icebergCatalogName = icebergConfig.getCatalogBackendName();

    Map<String, String> properties = icebergConfig.getIcebergCatalogProperties();
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
        new JdbcCatalog(null, null, icebergConfig.get(IcebergConfig.JDBC_INIT_TABLES));

    HdfsConfiguration hdfsConfiguration = new HdfsConfiguration();
    properties.forEach(hdfsConfiguration::set);
    jdbcCatalog.setConf(hdfsConfiguration);
    jdbcCatalog.initialize(icebergCatalogName, properties);
    return jdbcCatalog;
  }

  private static Catalog loadRestCatalog(IcebergConfig icebergConfig) {
    String icebergCatalogName = icebergConfig.getCatalogBackendName();
    RESTCatalog restCatalog = new RESTCatalog();
    HdfsConfiguration hdfsConfiguration = new HdfsConfiguration();
    Map<String, String> properties = icebergConfig.getIcebergCatalogProperties();
    properties.forEach(hdfsConfiguration::set);
    restCatalog.setConf(hdfsConfiguration);
    restCatalog.initialize(icebergCatalogName, properties);
    return restCatalog;
  }

  private static Catalog loadCustomCatalog(IcebergConfig icebergConfig) {
    String customCatalogName = icebergConfig.getCatalogBackendName();
    String className = icebergConfig.get(IcebergConfig.CATALOG_BACKEND_IMPL);
    return CatalogUtil.loadCatalog(
        className,
        customCatalogName,
        icebergConfig.getIcebergCatalogProperties(),
        new HdfsConfiguration());
  }

  @VisibleForTesting
  static Catalog loadCatalogBackend(String catalogType) {
    return loadCatalogBackend(
        IcebergCatalogBackend.valueOf(catalogType.toUpperCase(Locale.ROOT)),
        new IcebergConfig(Collections.emptyMap()));
  }

  public static Catalog loadCatalogBackend(
      IcebergCatalogBackend catalogBackend, IcebergConfig icebergConfig) {
    LOG.info("Load catalog backend of {}", catalogBackend);
    switch (catalogBackend) {
      case MEMORY:
        return loadMemoryCatalog(icebergConfig);
      case HIVE:
        return loadHiveCatalog(icebergConfig);
      case JDBC:
        return loadJdbcCatalog(icebergConfig);
      case REST:
        return loadRestCatalog(icebergConfig);
      case CUSTOM:
        return loadCustomCatalog(icebergConfig);
      default:
        throw new RuntimeException(
            catalogBackend
                + " catalog is not supported yet, supported catalogs: [memory]"
                + catalogBackend);
    }
  }

  private IcebergCatalogUtil() {}
}
