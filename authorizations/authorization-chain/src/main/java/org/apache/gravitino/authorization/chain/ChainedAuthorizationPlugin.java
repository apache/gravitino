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
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.authorization.Group;
import org.apache.gravitino.authorization.MetadataObjectChange;
import org.apache.gravitino.authorization.Owner;
import org.apache.gravitino.authorization.Role;
import org.apache.gravitino.authorization.RoleChange;
import org.apache.gravitino.authorization.User;
import org.apache.gravitino.authorization.common.ChainedAuthorizationProperties;
import org.apache.gravitino.connector.authorization.AuthorizationPlugin;
import org.apache.gravitino.connector.authorization.BaseAuthorization;
import org.apache.gravitino.exceptions.AuthorizationPluginException;
import org.apache.gravitino.utils.IsolatedClassLoader;

/** Chained authorization operations plugin class. <br> */
public class ChainedAuthorizationPlugin implements AuthorizationPlugin {
  private List<AuthorizationPlugin> plugins = Lists.newArrayList();
  private final String metalake;

  public ChainedAuthorizationPlugin(
      String metalake, String catalogProvider, Map<String, String> config) {
    this.metalake = metalake;
    initPlugins(catalogProvider, config);
  }

  private void initPlugins(String catalogProvider, Map<String, String> properties) {
    ChainedAuthorizationProperties chainedAuthzProperties =
        new ChainedAuthorizationProperties(properties);
    chainedAuthzProperties.validate();
    // Create the plugins
    chainedAuthzProperties
        .plugins()
        .forEach(
            pluginName -> {
              String authzProvider = chainedAuthzProperties.getPluginProvider(pluginName);
              Map<String, String> pluginConfig =
                  chainedAuthzProperties.fetchAuthPluginProperties(pluginName);

              ArrayList<String> libAndResourcesPaths = Lists.newArrayList();
              BaseAuthorization.buildAuthorizationPkgPath(
                      ImmutableMap.of(Catalog.AUTHORIZATION_PROVIDER, authzProvider))
                  .ifPresent(libAndResourcesPaths::add);
              IsolatedClassLoader classLoader =
                  IsolatedClassLoader.buildClassLoader(libAndResourcesPaths);
              try {
                BaseAuthorization<?> authorization =
                    BaseAuthorization.createAuthorization(classLoader, authzProvider);

                // Load the authorization plugin with the class loader of the catalog.
                // Because the JDBC authorization plugin may load JDBC driver using the class
                // loader.
                AuthorizationPlugin authorizationPlugin =
                    classLoader.withClassLoader(
                        cl -> authorization.newPlugin(metalake, catalogProvider, pluginConfig));
                plugins.add(authorizationPlugin);
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
            });
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
    return chainedAction(plugin -> plugin.onMetadataUpdated(changes));
  }

  @Override
  public Boolean onRoleCreated(Role role) throws AuthorizationPluginException {
    return chainedAction(plugin -> plugin.onRoleCreated(role));
  }

  @Override
  public Boolean onRoleAcquired(Role role) throws AuthorizationPluginException {
    return chainedAction(plugin -> plugin.onRoleAcquired(role));
  }

  @Override
  public Boolean onRoleDeleted(Role role) throws AuthorizationPluginException {
    onRoleUpdated(
        role,
        role.securableObjects().stream()
            .map(securableObject -> RoleChange.removeSecurableObject(role.name(), securableObject))
            .toArray(RoleChange[]::new));
    return chainedAction(plugin -> plugin.onRoleDeleted(role));
  }

  @Override
  public Boolean onRoleUpdated(Role role, RoleChange... changes)
      throws AuthorizationPluginException {
    return chainedAction(plugin -> plugin.onRoleUpdated(role, changes));
  }

  @Override
  public Boolean onGrantedRolesToUser(List<Role> roles, User user)
      throws AuthorizationPluginException {
    return chainedAction(plugin -> plugin.onGrantedRolesToUser(roles, user));
  }

  @Override
  public Boolean onRevokedRolesFromUser(List<Role> roles, User user)
      throws AuthorizationPluginException {
    return chainedAction(plugin -> plugin.onRevokedRolesFromUser(roles, user));
  }

  @Override
  public Boolean onGrantedRolesToGroup(List<Role> roles, Group group)
      throws AuthorizationPluginException {
    return chainedAction(plugin -> plugin.onGrantedRolesToGroup(roles, group));
  }

  @Override
  public Boolean onRevokedRolesFromGroup(List<Role> roles, Group group)
      throws AuthorizationPluginException {
    return chainedAction(plugin -> plugin.onRevokedRolesFromGroup(roles, group));
  }

  @Override
  public Boolean onUserAdded(User user) throws AuthorizationPluginException {
    return chainedAction(plugin -> plugin.onUserAdded(user));
  }

  @Override
  public Boolean onUserRemoved(User user) throws AuthorizationPluginException {
    return chainedAction(plugin -> plugin.onUserRemoved(user));
  }

  @Override
  public Boolean onUserAcquired(User user) throws AuthorizationPluginException {
    return chainedAction(plugin -> plugin.onUserAcquired(user));
  }

  @Override
  public Boolean onGroupAdded(Group group) throws AuthorizationPluginException {
    return chainedAction(plugin -> plugin.onGroupAdded(group));
  }

  @Override
  public Boolean onGroupRemoved(Group group) throws AuthorizationPluginException {
    return chainedAction(plugin -> plugin.onGroupRemoved(group));
  }

  @Override
  public Boolean onGroupAcquired(Group group) throws AuthorizationPluginException {
    return chainedAction(plugin -> plugin.onGroupAcquired(group));
  }

  @Override
  public Boolean onOwnerSet(MetadataObject metadataObject, Owner preOwner, Owner newOwner)
      throws AuthorizationPluginException {
    return chainedAction(plugin -> plugin.onOwnerSet(metadataObject, preOwner, newOwner));
  }

  private Boolean chainedAction(Function<AuthorizationPlugin, Boolean> action) {
    for (AuthorizationPlugin plugin : plugins) {
      if (!action.apply(plugin)) {
        return false;
      }
    }
    return true;
  }
}
