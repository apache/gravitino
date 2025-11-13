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
package org.apache.gravitino.storage.relational;

import com.google.common.collect.ImmutableSet;
import org.apache.gravitino.Entity;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.meta.EntityIdResolver;
import org.apache.gravitino.meta.EntityIds;
import org.apache.gravitino.storage.relational.helper.CatalogIds;
import org.apache.gravitino.storage.relational.helper.SchemaIds;
import org.apache.gravitino.storage.relational.service.*;
import org.apache.gravitino.utils.NameIdentifierUtil;

import java.util.Set;

public class RelationalEntityStoreIdResolver implements EntityIdResolver {
    @Override
    public EntityIds getEntityIds(NameIdentifier nameIdentifier, Entity.EntityType type) {
        return null;
    }

    @Override
    public long getEntityId(NameIdentifier nameIdentifier, Entity.EntityType type) {
        Set<Entity.EntityType> needMetalakeId = ImmutableSet.of(Entity.EntityType.METALAKE, Entity.EntityType.ROLE, Entity.EntityType.USER, Entity.EntityType.GROUP);
        Set<Entity.EntityType> needCatalogIds = ImmutableSet.of(Entity.EntityType.CATALOG);
        Set<Entity.EntityType> needSchemaIds = ImmutableSet.of(Entity.EntityType.SCHEMA, Entity.EntityType.TABLE, Entity.EntityType.FILESET, Entity.EntityType.TOPIC, Entity.EntityType.MODEL);

        if (needMetalakeId.contains(type)) {
            metalakeId =
        } else if (needCatalogIds.contains(type)) {
            CatalogIds catalogIds = CatalogMetaService.getInstance().getCatalogIdByMetalakeAndCatalogName(NameIdentifierUtil.getMetalake(nameIdentifier), NameIdentifierUtil.getCatalogIdentifier(nameIdentifier).name());
            metalakeId = catalogIds.getMetalakeId();
            catalogId = catalogIds.getCatalogId();
        } else if (needSchemaIds.contains(type)) {
            SchemaIds schemaIds = SchemaMetaService.getInstance().getSchemaIdByMetalakeNameAndCatalogNameAndSchemaName(
                    NameIdentifierUtil.getMetalake(nameIdentifier),
                    NameIdentifierUtil.getCatalogIdentifier(nameIdentifier).name(),
                    NameIdentifierUtil.getSchemaIdentifier(nameIdentifier).name()
            );
            metalakeId = schemaIds.getMetalakeId();
            catalogId = schemaIds.getCatalogId();

        } else {

        }



        return 0;
    }

    private EntityIds getEntityIdsV1(NameIdentifier nameIdentifier, Entity.EntityType type) {
        long metalakeId = MetalakeMetaService.getInstance().getMetalakeIdByName(NameIdentifierUtil.getMetalake(nameIdentifier));
        switch (type) {
            case METALAKE:
                return new EntityIds(metalakeId);
            case ROLE:
                long roleId = RoleMetaService.getInstance().getRoleIdByMetalakeIdAndName(metalakeId, nameIdentifier.name());
                return new EntityIds(roleId, metalakeId);
            case USER:
                long userId = UserMetaService.getInstance().getUserIdByMetalakeIdAndName(metalakeId, nameIdentifier.name());
                return new EntityIds(userId, metalakeId);
            case GROUP:
                long groupId = GroupMetaService.getInstance().getGroupIdByMetalakeIdAndName(metalakeId, nameIdentifier.name());
                return new EntityIds(groupId, metalakeId);
            default:
                throw new IllegalArgumentException("Unsupported entity type: " + type);
        }
    }
}
