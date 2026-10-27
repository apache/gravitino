/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.gravitino.iceberg.service.authorization;

import com.google.common.base.Preconditions;
import org.apache.gravitino.iceberg.common.ops.CatalogWrapperManager;
import org.apache.gravitino.iceberg.service.provider.IcebergConfigProvider;

/**
 * Process-wide Iceberg REST server context. Lives in iceberg-common so lakehouse can resolve IRC
 * catalog wrappers via {@link #catalogWrapperManager()} the same way {@code InternalCatalogFetcher}
 * uses {@code GravitinoEnv#catalogManager()}.
 */
public class IcebergRESTServerContext {
  private boolean isAuthorizationEnabled;
  private boolean auxMode;
  private boolean skipAuthorizationForRestBackend;
  private boolean catalogConfigsFromMetalake;
  private String metalakeName;
  private String defaultCatalogName;
  private CatalogWrapperManager catalogWrapperManager;

  private IcebergRESTServerContext(
      Boolean isAuthorizationEnabled,
      Boolean auxMode,
      Boolean skipAuthorizationForRestBackend,
      Boolean catalogConfigsFromMetalake,
      String metalakeName,
      String defaultCatalogName,
      CatalogWrapperManager catalogWrapperManager) {
    this.isAuthorizationEnabled = isAuthorizationEnabled;
    this.auxMode = auxMode;
    this.skipAuthorizationForRestBackend = skipAuthorizationForRestBackend;
    this.catalogConfigsFromMetalake = Boolean.TRUE.equals(catalogConfigsFromMetalake);
    this.metalakeName = metalakeName;
    this.defaultCatalogName = defaultCatalogName;
    this.catalogWrapperManager = catalogWrapperManager;
  }

  private static class InstanceHolder {
    private static IcebergRESTServerContext INSTANCE;
  }

  public static IcebergRESTServerContext create(
      IcebergConfigProvider configProvider,
      Boolean enableAuth,
      Boolean auxMode,
      Boolean skipAuthorizationForRestBackend,
      CatalogWrapperManager catalogWrapperManager) {
    InstanceHolder.INSTANCE =
        new IcebergRESTServerContext(
            enableAuth,
            auxMode,
            skipAuthorizationForRestBackend,
            configProvider.catalogConfigsFromMetalake(),
            configProvider.getMetalakeName(),
            configProvider.getDefaultCatalogName(),
            catalogWrapperManager);
    return InstanceHolder.INSTANCE;
  }

  public static IcebergRESTServerContext getInstance() {
    Preconditions.checkState(InstanceHolder.INSTANCE != null, "Not initialized");
    return InstanceHolder.INSTANCE;
  }

  public boolean isAuthorizationEnabled() {
    return isAuthorizationEnabled;
  }

  public boolean isAuxMode() {
    return auxMode;
  }

  public boolean skipAuthorizationForRestBackend() {
    return skipAuthorizationForRestBackend;
  }

  /**
   * Whether IRC catalog configs are loaded from metalake (dynamic-config-provider).
   *
   * @return true when lakehouse catalog names align with IRC managed catalogs
   */
  public boolean catalogConfigsFromMetalake() {
    return catalogConfigsFromMetalake;
  }

  public String metalakeName() {
    return metalakeName;
  }

  public String defaultCatalogName() {
    return defaultCatalogName;
  }

  public CatalogWrapperManager catalogWrapperManager() {
    return catalogWrapperManager;
  }
}
