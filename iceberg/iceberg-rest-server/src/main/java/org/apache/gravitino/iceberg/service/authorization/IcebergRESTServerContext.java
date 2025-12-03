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
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergConstants;
import org.apache.gravitino.iceberg.service.provider.DynamicIcebergConfigProvider;
import org.apache.gravitino.iceberg.service.provider.IcebergConfigProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IcebergRESTServerContext {
  private static final Logger LOG = LoggerFactory.getLogger(IcebergRESTServerContext.class);

  private boolean isAuthorizationEnabled;
  private String metalakeName;
  private String defaultCatalogName;

  private IcebergRESTServerContext(
      Boolean isAuthorizationEnabled, String metalakeName, String defaultCatalogName) {
    this.isAuthorizationEnabled = isAuthorizationEnabled;
    this.metalakeName = metalakeName;
    this.defaultCatalogName = defaultCatalogName;
  }

  private static class InstanceHolder {
    private static IcebergRESTServerContext INSTANCE;
  }

  public static IcebergRESTServerContext create(
      IcebergConfigProvider configProvider, Boolean enableAuth) {
    if (enableAuth && !(configProvider instanceof DynamicIcebergConfigProvider)) {
      LOG.warn(
          "Authorization is enabled but the Iceberg REST catalog-config-provider is not '{}'. "
              + "Requests to Iceberg REST will fail until "
              + "`gravitino.iceberg-rest.catalog-config-provider` is set to '{}'.",
          IcebergConstants.DYNAMIC_ICEBERG_CATALOG_CONFIG_PROVIDER_NAME,
          IcebergConstants.DYNAMIC_ICEBERG_CATALOG_CONFIG_PROVIDER_NAME);
    }
    InstanceHolder.INSTANCE =
        new IcebergRESTServerContext(
            enableAuth, configProvider.getMetalakeName(), configProvider.getDefaultCatalogName());
    return InstanceHolder.INSTANCE;
  }

  public static IcebergRESTServerContext getInstance() {
    Preconditions.checkState(InstanceHolder.INSTANCE != null, "Not initialized");
    return InstanceHolder.INSTANCE;
  }

  public boolean isAuthorizationEnabled() {
    return isAuthorizationEnabled;
  }

  public String metalakeName() {
    return metalakeName;
  }

  public String defaultCatalogName() {
    return defaultCatalogName;
  }
}
