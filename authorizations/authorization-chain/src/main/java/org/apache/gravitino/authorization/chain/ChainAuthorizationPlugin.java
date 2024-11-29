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

import java.util.List;
import java.util.Map;
import org.apache.gravitino.authorization.Role;
import org.apache.gravitino.authorization.SecurableObject;
import org.apache.gravitino.authorization.chain.translates.ChainTranslateEntity;
import org.apache.gravitino.authorization.chain.translates.ChainTranslateMappingProvider;
import org.apache.gravitino.meta.RoleEntity;

/** Chain authorization operations plugin class. <br> */
public class ChainAuthorizationPlugin extends ChainAuthorizationBase {
  private static volatile ChainAuthorizationPlugin instance = null;

  public static synchronized ChainAuthorizationPlugin getInstance(
      String catalogProvider, Map<String, String> config) {
    if (instance == null) {
      synchronized (ChainAuthorizationPlugin.class) {
        if (instance == null) {
          instance = new ChainAuthorizationPlugin(catalogProvider, config);
        }
      }
    }
    return instance;
  }

  private ChainAuthorizationPlugin(String catalogProvider, Map<String, String> config) {
    super(catalogProvider, config);
  }

  @Override
  Role translateRole(Role role, String toCatalogName, String toPluginName) {
    ChainTranslateEntity chainMappingProvider =
        new ChainTranslateEntity(catalogProviderName(), toCatalogName);
    ChainTranslateMappingProvider.ChainTranslate translator =
        ChainTranslateMappingProvider.getChainTranslator(chainMappingProvider);
    if (translator == null) {
      throw new UnsupportedOperationException(
          "Translation not supported for " + chainMappingProvider);
    }
    List<SecurableObject> translateSecrableObjects = translator.translate(role.securableObjects());

    RoleEntity roleEntity = (RoleEntity) role;
    return RoleEntity.builder()
        .withId(roleEntity.id())
        .withName(roleEntity.name())
        .withProperties(roleEntity.properties())
        .withAuditInfo(roleEntity.auditInfo())
        .withSecurableObjects(translateSecrableObjects)
        .build();
  }
}
