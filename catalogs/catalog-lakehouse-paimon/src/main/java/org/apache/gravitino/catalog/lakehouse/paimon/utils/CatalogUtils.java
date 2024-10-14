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
package org.apache.gravitino.catalog.lakehouse.paimon.utils;

import static org.apache.gravitino.catalog.lakehouse.paimon.PaimonCatalogPropertiesMetadata.GRAVITINO_CONFIG_TO_PAIMON;
import static org.apache.gravitino.catalog.lakehouse.paimon.PaimonCatalogPropertiesMetadata.KERBEROS_CONFIGURATION;
import static org.apache.gravitino.catalog.lakehouse.paimon.PaimonCatalogPropertiesMetadata.OSS_CONFIGURATION;
import static org.apache.gravitino.catalog.lakehouse.paimon.PaimonCatalogPropertiesMetadata.S3_CONFIGURATION;
import static org.apache.gravitino.catalog.lakehouse.paimon.PaimonConfig.CATALOG_BACKEND;
import static org.apache.gravitino.catalog.lakehouse.paimon.PaimonConfig.CATALOG_URI;
import static org.apache.gravitino.catalog.lakehouse.paimon.PaimonConfig.CATALOG_WAREHOUSE;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHORIZATION;

import com.google.common.base.Preconditions;
import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.catalog.lakehouse.paimon.PaimonCatalogBackend;
import org.apache.gravitino.catalog.lakehouse.paimon.PaimonConfig;
import org.apache.gravitino.catalog.lakehouse.paimon.authentication.AuthenticationConfig;
import org.apache.gravitino.catalog.lakehouse.paimon.authentication.kerberos.KerberosClient;
import org.apache.gravitino.catalog.lakehouse.paimon.ops.PaimonBackendCatalogWrapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.options.Options;

/** Utilities of {@link Catalog} to support catalog management. */
public class CatalogUtils {

  private CatalogUtils() {}

  public static PaimonBackendCatalogWrapper loadCatalogBackend(PaimonConfig paimonConfig) {
    Map<String, String> allConfig = paimonConfig.getAllConfig();
    AuthenticationConfig authenticationConfig = new AuthenticationConfig(allConfig);
    if (authenticationConfig.isSimpleAuth()) {
      return new PaimonBackendCatalogWrapper(loadCatalogBackendWithSimpleAuth(paimonConfig), null);
    } else if (authenticationConfig.isKerberosAuth()) {
      Configuration configuration = new Configuration();
      allConfig.forEach(configuration::set);
      configuration.set(HADOOP_SECURITY_AUTHORIZATION, "true");
      configuration.set(HADOOP_SECURITY_AUTHENTICATION, "kerberos");

      try {
        KerberosClient kerberosClient = new KerberosClient(allConfig, configuration);
        File keytabFile =
            kerberosClient.saveKeyTabFileFromUri(UUID.randomUUID().toString().replace("-", ""));
        kerberosClient.login(keytabFile.getAbsolutePath());
        Catalog catalog = loadCatalogBackendWithKerberosAuth(paimonConfig, configuration);
        return new PaimonBackendCatalogWrapper(catalog, kerberosClient);
      } catch (Exception e) {
        throw new RuntimeException("Failed to login with kerberos", e);
      }
    } else {
      throw new UnsupportedOperationException(
          "Unsupported authentication method: " + authenticationConfig.getAuthType());
    }
  }

  /**
   * Loads {@link Catalog} instance with given {@link PaimonConfig} with kerberos auth.
   *
   * @param paimonConfig The Paimon configuration.
   * @return The {@link Catalog} instance of catalog backend.
   */
  public static Catalog loadCatalogBackendWithKerberosAuth(
      PaimonConfig paimonConfig, Configuration configuration) {
    checkPaimonConfig(paimonConfig);

    // TODO: Now we only support kerberos auth for Filesystem backend, and will support it for Hive
    // backend later.
    Preconditions.checkArgument(
        PaimonCatalogBackend.FILESYSTEM.name().equalsIgnoreCase(paimonConfig.get(CATALOG_BACKEND)));

    CatalogContext catalogContext =
        CatalogContext.create(Options.fromMap(paimonConfig.getAllConfig()), configuration);
    return CatalogFactory.createCatalog(catalogContext);
  }

  /**
   * Loads {@link Catalog} instance with given {@link PaimonConfig} with simple auth.
   *
   * @param paimonConfig The Paimon configuration.
   * @return The {@link Catalog} instance of catalog backend.
   */
  public static Catalog loadCatalogBackendWithSimpleAuth(PaimonConfig paimonConfig) {
    checkPaimonConfig(paimonConfig);
    CatalogContext catalogContext =
        CatalogContext.create(Options.fromMap(paimonConfig.getAllConfig()));
    return CatalogFactory.createCatalog(catalogContext);
  }

  private static void checkPaimonConfig(PaimonConfig paimonConfig) {
    String metastore = paimonConfig.get(CATALOG_BACKEND);
    Preconditions.checkArgument(
        StringUtils.isNotBlank(metastore), "Paimon Catalog metastore can not be null or empty.");

    String warehouse = paimonConfig.get(CATALOG_WAREHOUSE);
    Preconditions.checkArgument(
        StringUtils.isNotBlank(warehouse), "Paimon Catalog warehouse can not be null or empty.");

    if (!PaimonCatalogBackend.FILESYSTEM.name().equalsIgnoreCase(metastore)) {
      String uri = paimonConfig.get(CATALOG_URI);
      Preconditions.checkArgument(
          StringUtils.isNotBlank(uri), "Paimon Catalog uri can not be null or empty.");
    }
  }

  public static Map<String, String> toInnerProperty(
      Map<String, String> properties, boolean keepUnknown) {
    Map<String, String> gravitinoConfig = new HashMap<>();
    properties.forEach(
        (key, value) -> {
          if (GRAVITINO_CONFIG_TO_PAIMON.containsKey(key)) {
            gravitinoConfig.put(GRAVITINO_CONFIG_TO_PAIMON.get(key), value);
          } else if (KERBEROS_CONFIGURATION.containsKey(key)) {
            gravitinoConfig.put(KERBEROS_CONFIGURATION.get(key), value);
          } else if (S3_CONFIGURATION.containsKey(key)) {
            gravitinoConfig.put(S3_CONFIGURATION.get(key), value);
          } else if (OSS_CONFIGURATION.containsKey(key)) {
            gravitinoConfig.put(OSS_CONFIGURATION.get(key), value);
          } else if (keepUnknown) {
            gravitinoConfig.put(key, value);
          }
        });
    return gravitinoConfig;
  }
}
