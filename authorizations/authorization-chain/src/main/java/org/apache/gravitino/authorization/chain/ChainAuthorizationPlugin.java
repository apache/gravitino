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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.authorization.ChainAuthorizationProperties;
import org.apache.gravitino.authorization.Group;
import org.apache.gravitino.authorization.MetadataObjectChange;
import org.apache.gravitino.authorization.Owner;
import org.apache.gravitino.authorization.RangerAuthorizationProperties;
import org.apache.gravitino.authorization.Role;
import org.apache.gravitino.authorization.RoleChange;
import org.apache.gravitino.authorization.User;
import org.apache.gravitino.connector.authorization.AuthorizationPlugin;
import org.apache.gravitino.connector.authorization.BaseAuthorization;
import org.apache.gravitino.exceptions.AuthorizationPluginException;
import org.apache.gravitino.utils.IsolatedClassLoader;

/** Chain authorization operations plugin class. <br> */
public class ChainAuthorizationPlugin implements AuthorizationPlugin {
  private List<AuthorizationPlugin> plugins = Lists.newArrayList();
  private final String metalake;

  public ChainAuthorizationPlugin(
      String metalake, String catalogProvider, Map<String, String> config) {
    this.metalake = metalake;
    initPlugins(catalogProvider, config);
  }

  private void initPlugins(String catalogProvider, Map<String, String> properties) {
    ChainAuthorizationProperties.validate(properties);
    // Validate the properties for each plugin
    ChainAuthorizationProperties.plugins(properties)
        .forEach(
            pluginName -> {
              Map<String, String> pluginProperties =
                  ChainAuthorizationProperties.fetchAuthPluginProperties(pluginName, properties);
              String authProvider =
                  ChainAuthorizationProperties.getPluginProvider(pluginName, properties);
              if ("ranger".equals(authProvider)) {
                RangerAuthorizationProperties.validate(pluginProperties);
              } else {
                throw new IllegalArgumentException("Unsupported provider: " + authProvider);
              }
            });
    // Create the plugins
    ChainAuthorizationProperties.plugins(properties)
        .forEach(
            pluginName -> {
              String authProvider =
                  ChainAuthorizationProperties.getPluginProvider(pluginName, properties);
              Map<String, String> pluginConfig =
                  ChainAuthorizationProperties.fetchAuthPluginProperties(pluginName, properties);

              ArrayList<String> libAndResourcesPaths = Lists.newArrayList();
              BaseAuthorization.buildAuthorizationPkgPath(
                      ImmutableMap.of(Catalog.AUTHORIZATION_PROVIDER, authProvider))
                  .ifPresent(libAndResourcesPaths::add);
              IsolatedClassLoader classLoader =
                  IsolatedClassLoader.buildClassLoader(libAndResourcesPaths);
              try {
                BaseAuthorization<?> authorization =
                    BaseAuthorization.createAuthorization(classLoader, authProvider);
                AuthorizationPlugin authorizationPlugin =
                    authorization.newPlugin(metalake, catalogProvider, pluginConfig);
                plugins.add(authorizationPlugin);
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
            });
  }

  @VisibleForTesting
  public final List<AuthorizationPlugin> getPlugins() {
    return plugins;
  }

  @Override
  public void close() throws IOException {
    for (AuthorizationPlugin plugin : plugins) {
      plugin.close();
    }
  }

  @Override
  public Boolean onMetadataUpdated(MetadataObjectChange... changes)
      throws AuthorizationPluginException {
    for (AuthorizationPlugin plugin : plugins) {
      Boolean result = plugin.onMetadataUpdated(changes);
      if (Boolean.FALSE.equals(result)) {
        return Boolean.FALSE;
      }
    }
    return Boolean.TRUE;
  }

  @Override
  public Boolean onRoleCreated(Role role) throws AuthorizationPluginException {
    for (AuthorizationPlugin plugin : plugins) {
      Boolean result = plugin.onRoleCreated(role);
      if (Boolean.FALSE.equals(result)) {
        return Boolean.FALSE;
      }
    }
    return Boolean.TRUE;
  }

  @Override
  public Boolean onRoleAcquired(Role role) throws AuthorizationPluginException {
    for (AuthorizationPlugin plugin : plugins) {
      Boolean result = plugin.onRoleAcquired(role);
      if (Boolean.FALSE.equals(result)) {
        return Boolean.FALSE;
      }
    }
    return Boolean.TRUE;
  }

  @Override
  public Boolean onRoleDeleted(Role role) throws AuthorizationPluginException {
    for (AuthorizationPlugin plugin : plugins) {
      Boolean result = plugin.onRoleDeleted(role);
      if (Boolean.FALSE.equals(result)) {
        return Boolean.FALSE;
      }
    }
    return Boolean.TRUE;
  }

  @Override
  public Boolean onRoleUpdated(Role role, RoleChange... changes)
      throws AuthorizationPluginException {
    for (AuthorizationPlugin plugin : plugins) {
      Boolean result = plugin.onRoleUpdated(role, changes);
      if (Boolean.FALSE.equals(result)) {
        return Boolean.FALSE;
      }
    }
    return Boolean.TRUE;
  }

  @Override
  public Boolean onGrantedRolesToUser(List<Role> roles, User user)
      throws AuthorizationPluginException {
    for (AuthorizationPlugin plugin : plugins) {
      Boolean result = plugin.onGrantedRolesToUser(roles, user);
      if (Boolean.FALSE.equals(result)) {
        return Boolean.FALSE;
      }
    }
    return Boolean.TRUE;
  }

  @Override
  public Boolean onRevokedRolesFromUser(List<Role> roles, User user)
      throws AuthorizationPluginException {
    for (AuthorizationPlugin plugin : plugins) {
      Boolean result = plugin.onRevokedRolesFromUser(roles, user);
      if (Boolean.FALSE.equals(result)) {
        return Boolean.FALSE;
      }
    }
    return Boolean.TRUE;
  }

  @Override
  public Boolean onGrantedRolesToGroup(List<Role> roles, Group group)
      throws AuthorizationPluginException {
    for (AuthorizationPlugin plugin : plugins) {
      Boolean result = plugin.onGrantedRolesToGroup(roles, group);
      if (Boolean.FALSE.equals(result)) {
        return Boolean.FALSE;
      }
    }
    return Boolean.TRUE;
  }

  @Override
  public Boolean onRevokedRolesFromGroup(List<Role> roles, Group group)
      throws AuthorizationPluginException {
    for (AuthorizationPlugin plugin : plugins) {
      Boolean result = plugin.onRevokedRolesFromGroup(roles, group);
      if (Boolean.FALSE.equals(result)) {
        return Boolean.FALSE;
      }
    }
    return Boolean.TRUE;
  }

  @Override
  public Boolean onUserAdded(User user) throws AuthorizationPluginException {
    for (AuthorizationPlugin plugin : plugins) {
      Boolean result = plugin.onUserAdded(user);
      if (Boolean.FALSE.equals(result)) {
        return Boolean.FALSE;
      }
    }
    return Boolean.TRUE;
  }

  @Override
  public Boolean onUserRemoved(User user) throws AuthorizationPluginException {
    for (AuthorizationPlugin plugin : plugins) {
      Boolean result = plugin.onUserRemoved(user);
      if (Boolean.FALSE.equals(result)) {
        return Boolean.FALSE;
      }
    }
    return Boolean.TRUE;
  }

  @Override
  public Boolean onUserAcquired(User user) throws AuthorizationPluginException {
    for (AuthorizationPlugin plugin : plugins) {
      Boolean result = plugin.onUserAcquired(user);
      if (Boolean.FALSE.equals(result)) {
        return Boolean.FALSE;
      }
    }
    return Boolean.TRUE;
  }

  @Override
  public Boolean onGroupAdded(Group group) throws AuthorizationPluginException {
    for (AuthorizationPlugin plugin : plugins) {
      Boolean result = plugin.onGroupAdded(group);
      if (Boolean.FALSE.equals(result)) {
        return Boolean.FALSE;
      }
    }
    return Boolean.TRUE;
  }

  @Override
  public Boolean onGroupRemoved(Group group) throws AuthorizationPluginException {
    for (AuthorizationPlugin plugin : plugins) {
      Boolean result = plugin.onGroupRemoved(group);
      if (Boolean.FALSE.equals(result)) {
        return Boolean.FALSE;
      }
    }
    return Boolean.TRUE;
  }

  @Override
  public Boolean onGroupAcquired(Group group) throws AuthorizationPluginException {
    for (AuthorizationPlugin plugin : plugins) {
      Boolean result = plugin.onGroupAcquired(group);
      if (Boolean.FALSE.equals(result)) {
        return Boolean.FALSE;
      }
    }
    return Boolean.TRUE;
  }

  @Override
  public Boolean onOwnerSet(MetadataObject metadataObject, Owner preOwner, Owner newOwner)
      throws AuthorizationPluginException {
    for (AuthorizationPlugin plugin : plugins) {
      Boolean result = plugin.onOwnerSet(metadataObject, preOwner, newOwner);
      if (Boolean.FALSE.equals(result)) {
        return Boolean.FALSE;
      }
    }
    return Boolean.TRUE;
  }
}
