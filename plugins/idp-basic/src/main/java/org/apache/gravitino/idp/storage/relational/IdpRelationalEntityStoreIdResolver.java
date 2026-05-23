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
package org.apache.gravitino.idp.storage.relational;

import org.apache.gravitino.Entity;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.idp.authorization.IdpAuthorizationUtils;
import org.apache.gravitino.idp.storage.service.IdpGroupMetaService;
import org.apache.gravitino.idp.storage.service.IdpUserMetaService;
import org.apache.gravitino.meta.EntityIdResolver;
import org.apache.gravitino.meta.NamespacedEntityId;

/** Resolves built-in IdP entity ids for the relational entity store. */
public class IdpRelationalEntityStoreIdResolver implements EntityIdResolver {

  @Override
  public NamespacedEntityId getEntityIds(NameIdentifier nameIdentifier, Entity.EntityType type) {
    switch (type) {
      case IDP_USER:
        IdpAuthorizationUtils.checkIdpUser(nameIdentifier);
        return new NamespacedEntityId(
            IdpUserMetaService.getInstance()
                .getIdpUserByUsername(nameIdentifier.name())
                .getUserId());
      case IDP_GROUP:
        IdpAuthorizationUtils.checkIdpGroup(nameIdentifier);
        return new NamespacedEntityId(
            IdpGroupMetaService.getInstance()
                .getIdpGroupByName(nameIdentifier.name())
                .getGroupId());
      default:
        throw new IllegalArgumentException("Unsupported built-in IdP entity type: " + type);
    }
  }

  @Override
  public Long getEntityId(NameIdentifier nameIdentifier, Entity.EntityType type) {
    return getEntityIds(nameIdentifier, type).entityId();
  }
}
