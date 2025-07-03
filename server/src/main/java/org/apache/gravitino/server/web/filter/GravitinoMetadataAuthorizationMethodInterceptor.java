/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.gravitino.server.web.filter;

import java.lang.reflect.Parameter;
import java.util.HashMap;
import java.util.Map;
import org.apache.gravitino.Entity.EntityType;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.server.authorization.annotations.AuthorizationMetadata;
import org.apache.gravitino.utils.NameIdentifierUtil;

/**
 * Through dynamic proxy, obtain the annotations on the method and parameter list to perform
 * metadata authorization.
 */
public class GravitinoMetadataAuthorizationMethodInterceptor
    extends BaseMetadataAuthorizationMethodInterceptor {

  @Override
  Map<EntityType, NameIdentifier> extractNameIdentifierFromParameters(
      Parameter[] parameters, Object[] args) {
    Map<EntityType, String> metadatas = new HashMap<>();
    Map<EntityType, NameIdentifier> nameIdentifierMap = new HashMap<>();
    for (int i = 0; i < parameters.length; i++) {
      Parameter parameter = parameters[i];
      AuthorizationMetadata authorizeResource =
          parameter.getAnnotation(AuthorizationMetadata.class);
      if (authorizeResource == null) {
        continue;
      }
      MetadataObject.Type type = authorizeResource.type();
      metadatas.put(EntityType.valueOf(type.name()), String.valueOf(args[i]));
    }
    String metalake = metadatas.get(EntityType.METALAKE);
    String catalog = metadatas.get(EntityType.CATALOG);
    String schema = metadatas.get(EntityType.SCHEMA);
    String table = metadatas.get(EntityType.TABLE);
    String topic = metadatas.get(EntityType.TOPIC);
    String fileset = metadatas.get(EntityType.FILESET);
    metadatas.forEach(
        (type, metadata) -> {
          switch (type) {
            case CATALOG:
              nameIdentifierMap.put(
                  EntityType.CATALOG, NameIdentifierUtil.ofCatalog(metalake, catalog));
              break;
            case SCHEMA:
              nameIdentifierMap.put(
                  EntityType.SCHEMA, NameIdentifierUtil.ofSchema(metalake, catalog, schema));
              break;
            case TABLE:
              nameIdentifierMap.put(
                  EntityType.TABLE, NameIdentifierUtil.ofTable(metalake, catalog, schema, table));
              break;
            case TOPIC:
              nameIdentifierMap.put(
                  EntityType.TOPIC, NameIdentifierUtil.ofTopic(metalake, catalog, schema, topic));
              break;
            case FILESET:
              nameIdentifierMap.put(
                  EntityType.FILESET,
                  NameIdentifierUtil.ofFileset(metalake, catalog, schema, fileset));
              break;
            case MODEL:
              String model = metadatas.get(EntityType.MODEL);
              nameIdentifierMap.put(
                  EntityType.MODEL, NameIdentifierUtil.ofModel(metadata, catalog, schema, model));
              break;
            case METALAKE:
              nameIdentifierMap.put(EntityType.METALAKE, NameIdentifierUtil.ofMetalake(metalake));
              break;
            default:
              break;
          }
        });
    return nameIdentifierMap;
  }
}
