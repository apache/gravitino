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
import java.util.Set;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.authorization.Privilege;
import org.apache.gravitino.authorization.SecurableObject;
import org.apache.gravitino.connector.authorization.AuthorizationMetadataObject;
import org.apache.gravitino.connector.authorization.AuthorizationPrivilege;
import org.apache.gravitino.connector.authorization.AuthorizationPrivilegesMappingProvider;
import org.apache.gravitino.connector.authorization.AuthorizationSecurableObject;

/** Chain authorization operations plugin class. <br> */
public class ChainAuthorizationPlugin extends ChainAuthorizationBase
    implements AuthorizationPrivilegesMappingProvider {
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
  public Map<Privilege.Name, Set<AuthorizationPrivilege>> privilegesMappingRule() {
    return null;
  }

  @Override
  public Set<AuthorizationPrivilege> ownerMappingRule() {
    return null;
  }

  @Override
  public Set<Privilege.Name> allowPrivilegesRule() {
    return null;
  }

  @Override
  public Set<MetadataObject.Type> allowMetadataObjectTypesRule() {
    return null;
  }

  @Override
  public List<AuthorizationSecurableObject> translatePrivilege(SecurableObject securableObject) {
    return null;
  }

  @Override
  public List<AuthorizationSecurableObject> translateOwner(MetadataObject metadataObject) {
    return null;
  }

  @Override
  public AuthorizationMetadataObject translateMetadataObject(MetadataObject metadataObject) {
    return null;
  }
}
