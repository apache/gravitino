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
package org.apache.gravitino.connector.authorization;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Streams;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.utils.IsolatedClassLoader;

/**
 * The abstract base class for Authorization implementations.<br>
 * A typical authorization always contain {@link AuthorizationPlugin} which is used to trigger the
 * specific operations by the authorization. <br>
 * For example, a Ranger authorization has a RangerAuthorizationPlugin which manipulates Ranger to
 * management Hive and HDFS permission. <br>
 *
 * @param <T> The type of the concrete subclass of BaseAuthorization.
 */
public abstract class BaseAuthorization<T extends BaseAuthorization>
    implements AuthorizationProvider, Closeable {

  /**
   * Creates a new instance of AuthorizationPlugin. <br>
   * The child class should implement this method to provide a specific AuthorizationPlugin instance
   * regarding that authorization. <br>
   *
   * @param metalake the identifier of the metalake environment
   * @param catalogProvider the identifier of the catalog provider within the metadata lake
   * @param config a map of configuration properties required by the plugin
   * @return A new instance of AuthorizationHook.
   */
  public abstract AuthorizationPlugin newPlugin(
      String metalake, String catalogProvider, Map<String, String> config);

  @Override
  public void close() throws IOException {}

  public static BaseAuthorization<?> createAuthorization(
      IsolatedClassLoader classLoader, String authorizationProvider) throws Exception {
    BaseAuthorization<?> authorization =
        classLoader.withClassLoader(
            cl -> {
              try {
                ServiceLoader<AuthorizationProvider> loader =
                    ServiceLoader.load(AuthorizationProvider.class, cl);

                List<Class<? extends AuthorizationProvider>> providers =
                    Streams.stream(loader.iterator())
                        .filter(p -> p.shortName().equalsIgnoreCase(authorizationProvider))
                        .map(AuthorizationProvider::getClass)
                        .collect(Collectors.toList());
                if (providers.isEmpty()) {
                  throw new IllegalArgumentException(
                      "No authorization provider found for: " + authorizationProvider);
                } else if (providers.size() > 1) {
                  throw new IllegalArgumentException(
                      "Multiple authorization providers found for: " + authorizationProvider);
                }
                return (BaseAuthorization<?>)
                    Iterables.getOnlyElement(providers).getDeclaredConstructor().newInstance();
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
            });
    return authorization;
  }

  public static Optional<String> buildAuthorizationPkgPath(Map<String, String> conf) {
    String gravitinoHome = System.getenv("GRAVITINO_HOME");
    Preconditions.checkArgument(gravitinoHome != null, "GRAVITINO_HOME not set");
    boolean testEnv = System.getenv("GRAVITINO_TEST") != null;

    String authorizationProvider = conf.get(Catalog.AUTHORIZATION_PROVIDER);
    if (StringUtils.isBlank(authorizationProvider)) {
      return Optional.empty();
    }

    String pkgPath;
    if (testEnv) {
      // In test, the authorization package is under the build directory.
      pkgPath =
          String.join(
              File.separator,
              gravitinoHome,
              "authorizations",
              "authorization-" + authorizationProvider,
              "build",
              "libs");
    } else {
      // In real environment, the authorization package is under the authorization directory.
      pkgPath =
          String.join(
              File.separator, gravitinoHome, "authorizations", authorizationProvider, "libs");
    }

    return Optional.of(pkgPath);
  }
}
