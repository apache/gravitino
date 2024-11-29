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

import com.google.common.collect.Iterables;
import com.google.common.collect.Streams;
import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.stream.Collectors;

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
  private volatile AuthorizationPlugin plugin = null;

  /**
   * Creates a new instance of AuthorizationPlugin. <br>
   * The child class should implement this method to provide a specific AuthorizationPlugin instance
   * regarding that authorization. <br>
   *
   * @return A new instance of AuthorizationHook.
   */
  protected abstract AuthorizationPlugin newPlugin(
      String catalogProvider, Map<String, String> config);

  public AuthorizationPlugin plugin(String catalogProvider, Map<String, String> config) {
    if (plugin == null) {
      synchronized (this) {
        if (plugin == null) {
          plugin = newPlugin(catalogProvider, config);
        }
      }
    }

    return plugin;
  }

  public static BaseAuthorization<?> createAuthorization(
      ClassLoader classLoader, String authorizationProvider) {
    try {
      ServiceLoader<AuthorizationProvider> loader =
          ServiceLoader.load(AuthorizationProvider.class, classLoader);

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
  }

  @Override
  public void close() throws IOException {
    if (plugin != null) {
      plugin.close();
    }
  }
}
