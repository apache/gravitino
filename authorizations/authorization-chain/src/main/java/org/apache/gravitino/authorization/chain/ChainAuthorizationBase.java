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
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.authorization.Group;
import org.apache.gravitino.authorization.MetadataObjectChange;
import org.apache.gravitino.authorization.Owner;
import org.apache.gravitino.authorization.Role;
import org.apache.gravitino.authorization.RoleChange;
import org.apache.gravitino.authorization.User;
import org.apache.gravitino.connector.AuthorizationPropertiesMeta;
import org.apache.gravitino.connector.authorization.AuthorizationPlugin;
import org.apache.gravitino.connector.authorization.AuthorizationPluginProvider;
import org.apache.gravitino.connector.authorization.BaseAuthorization;
import org.apache.gravitino.exceptions.AuthorizationPluginException;
import org.apache.gravitino.utils.MapUtils;

/** Chain call authorization plugin. */
public abstract class ChainAuthorizationBase implements AuthorizationPlugin {
  private List<AuthorizationPlugin> plugins = Lists.newArrayList();
  private final String catalogProviderName;

  ChainAuthorizationBase(String catalogProvider, Map<String, String> config) {
    this.catalogProviderName = catalogProvider;
    initPlugins(config);
  }

  private void initPlugins(Map<String, String> config) {
    String chainPlugins = config.get(AuthorizationPropertiesMeta.CHAIN_PLUGINS);
    Map<String, String> chainConfig =
        MapUtils.getFilteredMap(
            config, key -> key.toString().startsWith(AuthorizationPropertiesMeta.getChainPrefix()));
    Arrays.stream(chainPlugins.split(AuthorizationPropertiesMeta.getChainPluginsSplitter()))
        .forEach(
            pluginName -> {
              String providerKey =
                  AuthorizationPropertiesMeta.generateChainPluginsKey(
                      pluginName, AuthorizationPropertiesMeta.getChainProviderKey());
              Preconditions.checkArgument(
                  config.containsKey(providerKey), "Missing provider for plugin: " + pluginName);
              String authProvider = config.get(providerKey);
              Preconditions.checkArgument(
                  !authProvider.isEmpty(), "Provider for plugin: " + pluginName + " is empty");
              // Convert chain config to plugin config
              Map<String, String> pluginConfig =
                  chainConfig.entrySet().stream()
                      .filter(
                          entry ->
                              entry
                                  .getKey()
                                  .startsWith(
                                      String.format(
                                          "%s.%s",
                                          AuthorizationPropertiesMeta.getChainPrefix(),
                                          pluginName)))
                      .collect(
                          Collectors.toMap(
                              entry ->
                                  AuthorizationPropertiesMeta.chainKeyToPluginKey(
                                      entry.getKey(), pluginName),
                              Map.Entry::getValue));
              AuthorizationPlugin authorizationPlugin =
                  BaseAuthorization.createAuthorization(
                          this.getClass().getClassLoader(), authProvider)
                      .plugin(catalogProviderName, pluginConfig);
              plugins.add(authorizationPlugin);
            });
  }

  @VisibleForTesting
  public final List<AuthorizationPlugin> getPlugins() {
    return plugins;
  }

  @Override
  public String catalogProviderName() {
    return catalogProviderName;
  }

  @Override
  public String pluginProviderName() {
    return AuthorizationPluginProvider.Type.Chain.getName();
  }

  abstract Role translateRole(Role role, String toCatalogName, String toPluginName);

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
      Role pluginRole =
          translateRole(role, plugin.catalogProviderName(), plugin.pluginProviderName());
      Boolean result = plugin.onRoleCreated(pluginRole);
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
