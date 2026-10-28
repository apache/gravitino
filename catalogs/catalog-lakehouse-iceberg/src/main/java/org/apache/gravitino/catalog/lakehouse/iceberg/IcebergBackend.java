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
package org.apache.gravitino.catalog.lakehouse.iceberg;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.catalog.lakehouse.iceberg.ops.IcebergCatalogWrapperHelper;
import org.apache.gravitino.iceberg.common.IcebergConfig;
import org.apache.gravitino.iceberg.common.authentication.AuthenticationConfig;
import org.apache.gravitino.iceberg.common.authentication.SupportsKerberos;
import org.apache.gravitino.iceberg.common.ops.IcebergCatalogWrapper;
import org.apache.gravitino.iceberg.common.ops.KerberosAwareIcebergCatalogProxy;
import org.apache.gravitino.iceberg.common.utils.IcebergCatalogUtil;
import org.apache.iceberg.exceptions.NoSuchWarehouseException;
import org.apache.iceberg.exceptions.RESTException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Owns the Iceberg client for one lakehouse catalog and remints minted GCS OAuth2 tokens before
 * use. {@link IcebergCatalogOperations} stays focused on Gravitino API mapping.
 */
final class IcebergBackend implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(IcebergBackend.class);

  private static final long GCS_TOKEN_REFRESH_BUFFER_MS = TimeUnit.MINUTES.toMillis(5);

  private final Map<String, String> catalogConf;
  private IcebergCatalogWrapper wrapper;
  private IcebergCatalogWrapperHelper helper;
  private IcebergViewCatalogOperations views;

  private IcebergBackend(Map<String, String> catalogConf) {
    this.catalogConf = catalogConf;
  }

  static IcebergBackend create(Map<String, String> catalogConf) {
    IcebergBackend backend = new IcebergBackend(catalogConf);
    backend.reload();
    return backend;
  }

  /** Fixed client for tests; skips GCS reminting. */
  @VisibleForTesting
  static IcebergBackend fixed(IcebergCatalogWrapper wrapper) {
    Preconditions.checkNotNull(wrapper, "wrapper");
    IcebergBackend backend = new IcebergBackend(null);
    backend.wrapper = wrapper;
    return backend;
  }

  synchronized IcebergCatalogWrapper wrapper() {
    refreshGcsTokenIfNeeded();
    return wrapper;
  }

  synchronized IcebergCatalogWrapperHelper helper() {
    refreshGcsTokenIfNeeded();
    if (helper == null) {
      helper = new IcebergCatalogWrapperHelper(wrapper.getCatalog());
    }
    return helper;
  }

  synchronized IcebergViewCatalogOperations views() {
    refreshGcsTokenIfNeeded();
    if (views == null) {
      views = new IcebergViewCatalogOperations(wrapper);
    }
    return views;
  }

  private void refreshGcsTokenIfNeeded() {
    if (catalogConf == null || wrapper == null) {
      return;
    }
    String expiresAt =
        wrapper
            .getIcebergConfig()
            .getAllConfig()
            .get(IcebergConstants.ICEBERG_GCS_OAUTH2_TOKEN_EXPIRES_AT);
    if (StringUtils.isBlank(expiresAt)) {
      return;
    }
    try {
      if (Long.parseLong(expiresAt) - GCS_TOKEN_REFRESH_BUFFER_MS > System.currentTimeMillis()) {
        return;
      }
    } catch (NumberFormatException e) {
      LOG.warn("Invalid {}: {}", IcebergConstants.ICEBERG_GCS_OAUTH2_TOKEN_EXPIRES_AT, expiresAt);
      return;
    }
    IcebergCatalogWrapper previous = wrapper;
    reload();
    closeQuietly(previous);
  }

  private void reload() {
    IcebergConfig icebergConfig =
        IcebergCatalogUtil.withGcsServiceAccountCredentials(new IcebergConfig(catalogConf));
    try {
      IcebergCatalogWrapper rawWrapper = new IcebergCatalogWrapper(icebergConfig);
      AuthenticationConfig authenticationConfig = new AuthenticationConfig(catalogConf);
      this.wrapper =
          authenticationConfig.isKerberosAuth()
                  && rawWrapper.getCatalog() instanceof SupportsKerberos
              ? new KerberosAwareIcebergCatalogProxy(rawWrapper).getProxy(icebergConfig)
              : rawWrapper;
      this.helper = new IcebergCatalogWrapperHelper(wrapper.getCatalog());
      this.views = new IcebergViewCatalogOperations(wrapper);
    } catch (NoSuchWarehouseException e) {
      throw new IllegalArgumentException(
          String.format(
              "The 'warehouse' value '%s' could not be resolved by the Iceberg REST server. On "
                  + "the REST backend 'warehouse' selects a catalog by name on the remote server "
                  + "and is not a storage location; remove 'warehouse' to use the server's default "
                  + "catalog, or set it to a catalog name/identifier that the server recognizes.",
              icebergConfig.get(IcebergConfig.CATALOG_WAREHOUSE)),
          e);
    } catch (RESTException e) {
      throw IcebergCatalogOperations.handleRestException(e);
    }
  }

  @Override
  public synchronized void close() {
    closeQuietly(wrapper);
    wrapper = null;
    helper = null;
    views = null;
  }

  private static void closeQuietly(IcebergCatalogWrapper catalogWrapper) {
    if (catalogWrapper == null) {
      return;
    }
    try {
      catalogWrapper.close();
    } catch (Exception e) {
      LOG.warn("Failed to close Iceberg catalog wrapper", e);
    }
  }
}
