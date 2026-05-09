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
package org.apache.gravitino;

import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;
import org.apache.gravitino.authorization.IdpManager;
import org.apache.gravitino.dto.IdpGroupDTO;
import org.apache.gravitino.dto.IdpUserDTO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is responsible for creating instances of IdpManager implementations. IdpManager
 * implementations are used to manage built-in IdP users and groups within the Apache Gravitino
 * framework.
 */
public class IdpManagerFactory {

  private static final Logger LOG = LoggerFactory.getLogger(IdpManagerFactory.class);

  private static final String IDP_MANAGER_UNAVAILABLE_MESSAGE =
      "Built-in IdP management is unavailable because no IdpManager plugin implementation was found"
          + " on the runtime classpath.";

  // Private constructor to prevent instantiation of this factory class.
  private IdpManagerFactory() {}

  /**
   * Creates an instance of IdpManager from the runtime classpath.
   *
   * @return An instance of IdpManager.
   */
  public static IdpManager createIdpManager() {
    return loadService(IdpManager.class);
  }

  /**
   * Creates an instance of IdpManager from the runtime classpath or returns an unavailable
   * placeholder when no implementation is present.
   *
   * @return An instance of IdpManager.
   */
  public static IdpManager createIdpManagerOrDefault() {
    try {
      return createIdpManager();
    } catch (IllegalStateException e) {
      if (e.getMessage() != null
          && e.getMessage()
              .contains("No " + IdpManager.class.getSimpleName() + " implementation")) {
        LOG.warn("No IdpManager implementation found on the runtime classpath.");
        return new UnavailableIdpManager();
      }

      throw e;
    }
  }

  private static <T> T loadService(Class<T> serviceClass) {
    List<T> services = new ArrayList<>();
    for (T service : ServiceLoader.load(serviceClass)) {
      services.add(service);
    }

    if (services.isEmpty()) {
      throw new IllegalStateException(
          String.format("No %s implementation found", serviceClass.getSimpleName()));
    }

    if (services.size() > 1) {
      throw new IllegalStateException(
          String.format("Multiple %s implementations found", serviceClass.getSimpleName()));
    }

    return services.get(0);
  }

  private static class UnavailableIdpManager implements IdpManager {

    @Override
    public IdpUserDTO createUser(String userName, String password) {
      throw new UnsupportedOperationException(IDP_MANAGER_UNAVAILABLE_MESSAGE);
    }

    @Override
    public IdpUserDTO getUser(String userName) {
      throw new UnsupportedOperationException(IDP_MANAGER_UNAVAILABLE_MESSAGE);
    }

    @Override
    public boolean deleteUser(String userName) {
      throw new UnsupportedOperationException(IDP_MANAGER_UNAVAILABLE_MESSAGE);
    }

    @Override
    public IdpUserDTO resetPassword(String userName, String password) {
      throw new UnsupportedOperationException(IDP_MANAGER_UNAVAILABLE_MESSAGE);
    }

    @Override
    public IdpGroupDTO createGroup(String groupName) {
      throw new UnsupportedOperationException(IDP_MANAGER_UNAVAILABLE_MESSAGE);
    }

    @Override
    public IdpGroupDTO getGroup(String groupName) {
      throw new UnsupportedOperationException(IDP_MANAGER_UNAVAILABLE_MESSAGE);
    }

    @Override
    public boolean deleteGroup(String groupName, boolean force) {
      throw new UnsupportedOperationException(IDP_MANAGER_UNAVAILABLE_MESSAGE);
    }

    @Override
    public IdpGroupDTO addUsersToGroup(String groupName, List<String> userNames) {
      throw new UnsupportedOperationException(IDP_MANAGER_UNAVAILABLE_MESSAGE);
    }

    @Override
    public IdpGroupDTO removeUsersFromGroup(String groupName, List<String> userNames) {
      throw new UnsupportedOperationException(IDP_MANAGER_UNAVAILABLE_MESSAGE);
    }
  }
}
