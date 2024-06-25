/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.lakehouse.paimon.utils;

import static com.datastrato.gravitino.catalog.lakehouse.paimon.PaimonConfig.CATALOG_BACKEND;
import static com.datastrato.gravitino.catalog.lakehouse.paimon.PaimonConfig.CATALOG_URI;
import static com.datastrato.gravitino.catalog.lakehouse.paimon.PaimonConfig.CATALOG_WAREHOUSE;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHORIZATION;

import com.datastrato.gravitino.catalog.lakehouse.paimon.PaimonCatalogBackend;
import com.datastrato.gravitino.catalog.lakehouse.paimon.PaimonConfig;
import com.datastrato.gravitino.catalog.lakehouse.paimon.authentication.AuthenticationConfig;
import com.datastrato.gravitino.catalog.lakehouse.paimon.authentication.kerberos.FilesystemBackendProxy;
import com.datastrato.gravitino.catalog.lakehouse.paimon.authentication.kerberos.KerberosClient;
import com.google.common.base.Preconditions;
import java.io.File;
import java.io.IOException;
import java.util.Locale;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.FileSystemCatalog;
import org.apache.paimon.options.Options;

/** Utilities of {@link Catalog} to support catalog management. */
public class CatalogUtils {

  private CatalogUtils() {}

  /**
   * Loads {@link Catalog} instance with given {@link PaimonConfig}.
   *
   * @param paimonConfig The Paimon configuration.
   * @return The {@link Catalog} instance of catalog backend.
   */
  public static Catalog loadCatalogBackend(PaimonConfig paimonConfig) {
    String metastore = paimonConfig.get(CATALOG_BACKEND);
    Preconditions.checkArgument(
        StringUtils.isNotBlank(metastore), "Paimon Catalog metastore can not be null or empty.");

    String warehouse = paimonConfig.get(CATALOG_WAREHOUSE);
    Preconditions.checkArgument(
        StringUtils.isNotBlank(warehouse), "Paimon Catalog warehouse can not be null or empty.");

    if (!PaimonCatalogBackend.FILESYSTEM
        .name()
        .toLowerCase(Locale.ROOT)
        .equals(metastore.toLowerCase(Locale.ROOT))) {
      String uri = paimonConfig.get(CATALOG_URI);
      Preconditions.checkArgument(
          StringUtils.isNotBlank(uri), "Paimon Catalog uri can not be null or empty.");
    }

    Map<String, String> allConfig = paimonConfig.getAllConfig();
    Configuration configuration = new Configuration();
    allConfig.forEach(configuration::set);

    CatalogContext catalogContext =
        CatalogContext.create(Options.fromMap(paimonConfig.getAllConfig()));

    AuthenticationConfig authenticationConfig = new AuthenticationConfig(allConfig);
    if (authenticationConfig.isSimpleAuth()) {
      return CatalogFactory.createCatalog(catalogContext);
    } else if (authenticationConfig.isKerberosAuth()) {
      configuration.set(HADOOP_SECURITY_AUTHORIZATION, "true");
      configuration.set(HADOOP_SECURITY_AUTHENTICATION, "kerberos");

      switch (PaimonCatalogBackend.valueOf(metastore.toUpperCase(Locale.ROOT))) {
        case FILESYSTEM:
          String realm = initKerberosAndReturnRealm(allConfig, configuration);
          Catalog catalog = CatalogFactory.createCatalog(catalogContext);
          if (authenticationConfig.isImpersonationEnabled()) {
            FilesystemBackendProxy proxyFilesystemCatalog =
                new FilesystemBackendProxy((FileSystemCatalog) catalog, realm);
            return proxyFilesystemCatalog.getProxy();
          }
          return catalog;

          // TODO: support hive backend

        default:
          throw new IllegalArgumentException(
              String.format("Unsupported Kerberos authentication for %s backend.", metastore));
      }
    } else {
      throw new UnsupportedOperationException(
          "Unsupported authentication method: " + authenticationConfig.getAuthType());
    }
  }

  private static String initKerberosAndReturnRealm(
      Map<String, String> properties, Configuration conf) {
    try {
      KerberosClient kerberosClient = new KerberosClient(properties, conf);
      File keytabFile = kerberosClient.saveKeyTabFileFromUri(properties.get("catalog_uuid"));
      return kerberosClient.login(keytabFile.getAbsolutePath());
    } catch (IOException e) {
      throw new RuntimeException("Failed to login with kerberos", e);
    }
  }
}
