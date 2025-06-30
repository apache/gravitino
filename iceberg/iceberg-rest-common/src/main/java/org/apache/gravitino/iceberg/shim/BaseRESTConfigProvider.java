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

package org.apache.gravitino.iceberg.shim;

import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergConstants;
import org.apache.gravitino.iceberg.service.CatalogWrapperForREST;
import org.apache.gravitino.iceberg.service.IcebergCatalogWrapperManager;
import org.apache.iceberg.rest.responses.ConfigResponse;

public class BaseRESTConfigProvider implements IcebergRESTConfigProvider {

  private IcebergCatalogWrapperManager catalogManager;

  public void initialize(IcebergCatalogWrapperManager catalogManager) {
    this.catalogManager = catalogManager;
  }

  protected Map<String, String> getDefaultConfig(String warehouse) {
    return getCatalogConfig(warehouse);
  }

  protected ConfigResponse.Builder getConfigResponseBuilder(boolean supportsViewOperations) {
    return ConfigResponse.builder();
  }

  @Override
  public ConfigResponse getConfig(String warehouse) {
    String catalogName = getCatalogName(warehouse);
    ConfigResponse.Builder builder =
        getConfigResponseBuilder(supportsViewOperations(catalogName))
            .withDefaults(getDefaultConfig(catalogName));
    if (StringUtils.isNotBlank(warehouse)) {
      builder.withDefault("prefix", warehouse);
    }
    return builder.build();
  }

  private Map<String, String> getCatalogConfig(String catalogName) {
    Map<String, String> configs = new HashMap<>();
    CatalogWrapperForREST catalogWrapper = getCatalogWrapper(catalogName);
    configs.putAll(catalogWrapper.getCatalogConfigToClient());
    return configs;
  }

  private String getCatalogName(String warehouse) {
    if (StringUtils.isBlank(warehouse)) {
      return IcebergConstants.ICEBERG_REST_DEFAULT_CATALOG;
    } else {
      return warehouse;
    }
  }

  private boolean supportsViewOperations(String catalogName) {
    CatalogWrapperForREST catalogWrapperForREST = getCatalogWrapper(catalogName);
    return catalogWrapperForREST.supportsViewOperations();
  }

  private CatalogWrapperForREST getCatalogWrapper(String catalogName) {
    return catalogManager.getCatalogWrapper(catalogName);
  }
}
