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

package org.apache.gravitino.iceberg.service.provider;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergConstants;
import org.apache.gravitino.iceberg.common.IcebergConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IcebergConfigProviderFactory {
  public static final Logger LOG = LoggerFactory.getLogger(IcebergConfigProviderFactory.class);

  private static final ImmutableMap<String, String> ICEBERG_CATALOG_CONFIG_PROVIDER_NAMES =
      ImmutableMap.of(
          IcebergConstants.STATIC_ICEBERG_CATALOG_CONFIG_PROVIDER_NAME,
          StaticIcebergConfigProvider.class.getCanonicalName(),
          IcebergConstants.DYNAMIC_ICEBERG_CATALOG_CONFIG_PROVIDER_NAME,
          DynamicIcebergConfigProvider.class.getCanonicalName());

  public static IcebergConfigProvider create(Map<String, String> properties) {
    String providerName =
        (new IcebergConfig(properties)).get(IcebergConfig.ICEBERG_REST_CATALOG_CONFIG_PROVIDER);
    String className =
        ICEBERG_CATALOG_CONFIG_PROVIDER_NAMES.getOrDefault(providerName, providerName);
    LOG.info("Load Iceberg catalog provider: {}.", className);
    try {
      Class<?> providerClz = Class.forName(className);
      return (IcebergConfigProvider) providerClz.getDeclaredConstructor().newInstance();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
