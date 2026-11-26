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
package org.apache.gravitino.paimon.service.provider;

import java.util.Map;

/** Creates the static catalog configuration provider. */
public class PaimonConfigProviderFactory {

  static final String CATALOG_CONFIG_PROVIDER = "catalog-config-provider";

  private PaimonConfigProviderFactory() {}

  /**
   * Creates and initializes the provider for the given service configuration.
   *
   * @param properties the service configuration ({@code gravitino.paimon-rest.} prefix stripped)
   * @return the provider
   */
  public static PaimonConfigProvider create(Map<String, String> properties) {
    return create(properties, false);
  }

  /**
   * Creates a provider for the deployment mode.
   *
   * @param properties service configuration
   * @param auxiliary whether the service runs inside Gravitino
   * @return initialized provider
   */
  public static PaimonConfigProvider create(Map<String, String> properties, boolean auxiliary) {
    String providerName =
        properties.getOrDefault(CATALOG_CONFIG_PROVIDER, "static-config-provider");
    if ("dynamic-config-provider".equals(providerName)) {
      PaimonConfigProvider provider = new DynamicPaimonConfigProvider(auxiliary);
      provider.initialize(properties);
      return provider;
    }
    if (!"static-config-provider".equals(providerName)) {
      throw new IllegalArgumentException(
          "Unsupported catalog-config-provider: "
              + providerName
              + ". Use static-config-provider or dynamic-config-provider.");
    }
    PaimonConfigProvider provider = new StaticPaimonConfigProvider();
    provider.initialize(properties);
    return provider;
  }
}
