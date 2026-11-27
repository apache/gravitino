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
package org.apache.gravitino.authorization.chain;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.authorization.common.ChainedAuthorizationProperties;
import org.apache.gravitino.connector.authorization.AuthorizationPlugin;
import org.apache.gravitino.connector.authorization.BaseAuthorization;
import org.apache.gravitino.utils.IsolatedClassLoader;

/** Implementation of a Chained authorization in Gravitino. */
public class ChainedAuthorization extends BaseAuthorization<ChainedAuthorization> {
  @Override
  public String shortName() {
    return "chain";
  }

  @Override
  public void validateProperties(
      String metalake, String catalogProvider, Map<String, String> config) {
    if (!"hive".equals(catalogProvider) && !"kafka".equals(catalogProvider)) {
      throw new IllegalArgumentException("Unknown catalog provider: " + catalogProvider);
    }
    ChainedAuthorizationProperties chainedAuthzProperties =
        new ChainedAuthorizationProperties(config);
    chainedAuthzProperties.validate();
    for (String pluginName : chainedAuthzProperties.plugins()) {
      String authzProvider = chainedAuthzProperties.getPluginProvider(pluginName);
      Map<String, String> pluginConfig =
          chainedAuthzProperties.fetchAuthPluginProperties(pluginName);
      if (config.containsKey(BaseAuthorization.METALAKE_ID)) {
        pluginConfig.put(BaseAuthorization.METALAKE_ID, config.get(BaseAuthorization.METALAKE_ID));
      }
      if (config.containsKey(BaseAuthorization.CATALOG_ID)) {
        pluginConfig.put(BaseAuthorization.CATALOG_ID, config.get(BaseAuthorization.CATALOG_ID));
      }
      ArrayList<String> libAndResourcesPaths = Lists.newArrayList();
      if (System.getenv("GRAVITINO_HOME") != null) {
        BaseAuthorization.buildAuthorizationPkgPath(
                ImmutableMap.of(Catalog.AUTHORIZATION_PROVIDER, authzProvider))
            .ifPresent(libAndResourcesPaths::add);
      }
      try (IsolatedClassLoader classLoader =
              libAndResourcesPaths.isEmpty()
                  ? new IsolatedClassLoader(
                      Collections.emptyList(), Collections.emptyList(), Collections.emptyList())
                  : IsolatedClassLoader.buildClassLoader(libAndResourcesPaths);
          BaseAuthorization<?> authorization =
              BaseAuthorization.createAuthorization(classLoader, authzProvider)) {
        classLoader.withClassLoader(
            cl -> {
              authorization.validateProperties(metalake, catalogProvider, pluginConfig);
              return null;
            },
            IllegalArgumentException.class);
      } catch (IllegalArgumentException e) {
        throw e;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public AuthorizationPlugin newPlugin(
      String metalake, String catalogProvider, Map<String, String> config) {
    switch (catalogProvider) {
      case "hive":
      case "kafka":
        return new ChainedAuthorizationPlugin(metalake, catalogProvider, config);
      default:
        throw new IllegalArgumentException("Unknown catalog provider: " + catalogProvider);
    }
  }
}
