/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *  http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.gravitino.server.web.filter;

import java.lang.reflect.Parameter;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import org.apache.gravitino.Entity;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.server.authorization.MetadataAuthzHelper;
import org.apache.gravitino.server.authorization.annotations.AuthorizationFullName;
import org.apache.gravitino.server.authorization.annotations.AuthorizationMetadata;
import org.apache.gravitino.server.authorization.annotations.AuthorizationObjectType;
import org.apache.gravitino.server.authorization.annotations.AuthorizationRequest;
import org.apache.gravitino.utils.MetadataObjectUtil;
import org.apache.gravitino.utils.NameIdentifierUtil;

public class ParameterUtil {

  private ParameterUtil() {}

  public static AuthorizationRequest.RequestType extractAuthorizationRequestTypeFromParameters(
      Parameter[] parameters) {
    for (Parameter parameter : parameters) {
      AuthorizationRequest authorizationRequest =
          parameter.getAnnotation(AuthorizationRequest.class);
      if (authorizationRequest != null) {
        return authorizationRequest.type();
      }
    }
    return AuthorizationRequest.RequestType.COMMON;
  }

  public static Object extractFromParameters(Parameter[] parameters, Object[] args) {
    for (int i = 0; i < parameters.length; i++) {
      Parameter parameter = parameters[i];
      AuthorizationRequest authorizationBatchTarget =
          parameter.getAnnotation(AuthorizationRequest.class);
      if (authorizationBatchTarget == null) {
        continue;
      }
      return args[i];
    }
    return null;
  }

  public static Map<Entity.EntityType, NameIdentifier> extractNameIdentifierFromParameters(
      Parameter[] parameters, Object[] args) {
    Map<Entity.EntityType, String> entities = new HashMap<>();
    Map<Entity.EntityType, NameIdentifier> nameIdentifierMap = new HashMap<>();
    // Extract AuthorizationMetadata
    for (int i = 0; i < parameters.length; i++) {
      Parameter parameter = parameters[i];
      AuthorizationMetadata authorizeResource =
          parameter.getAnnotation(AuthorizationMetadata.class);
      if (authorizeResource == null) {
        continue;
      }
      Entity.EntityType type = authorizeResource.type();
      entities.put(type, String.valueOf(args[i]));
    }

    String metalake = entities.get(Entity.EntityType.METALAKE);
    String catalog = entities.get(Entity.EntityType.CATALOG);
    String schema = entities.get(Entity.EntityType.SCHEMA);
    String table = entities.get(Entity.EntityType.TABLE);
    String topic = entities.get(Entity.EntityType.TOPIC);
    String fileset = entities.get(Entity.EntityType.FILESET);

    // Extract full name and types
    String fullName = null;
    String metadataObjectType = null;
    for (int i = 0; i < parameters.length; i++) {
      Parameter parameter = parameters[i];
      AuthorizationFullName authorizeFullName =
          parameter.getAnnotation(AuthorizationFullName.class);
      if (authorizeFullName != null) {
        fullName = String.valueOf(args[i]);
      }

      AuthorizationObjectType objectType = parameter.getAnnotation(AuthorizationObjectType.class);
      if (objectType != null) {
        metadataObjectType = String.valueOf(args[i]);
      }
    }

    entities.forEach(
        (type, metadata) -> {
          switch (type) {
            case CATALOG:
              nameIdentifierMap.put(
                  Entity.EntityType.CATALOG, NameIdentifierUtil.ofCatalog(metalake, catalog));
              break;
            case SCHEMA:
              nameIdentifierMap.put(
                  Entity.EntityType.SCHEMA, NameIdentifierUtil.ofSchema(metalake, catalog, schema));
              break;
            case TABLE:
              nameIdentifierMap.put(
                  Entity.EntityType.TABLE,
                  NameIdentifierUtil.ofTable(metalake, catalog, schema, table));
              break;
            case TOPIC:
              nameIdentifierMap.put(
                  Entity.EntityType.TOPIC,
                  NameIdentifierUtil.ofTopic(metalake, catalog, schema, topic));
              break;
            case FILESET:
              nameIdentifierMap.put(
                  Entity.EntityType.FILESET,
                  NameIdentifierUtil.ofFileset(metalake, catalog, schema, fileset));
              break;
            case MODEL:
              String model = entities.get(Entity.EntityType.MODEL);
              nameIdentifierMap.put(
                  Entity.EntityType.MODEL,
                  NameIdentifierUtil.ofModel(metalake, catalog, schema, model));
              break;
            case METALAKE:
              nameIdentifierMap.put(
                  Entity.EntityType.METALAKE, NameIdentifierUtil.ofMetalake(metalake));
              break;
            case USER:
              nameIdentifierMap.put(
                  Entity.EntityType.USER,
                  NameIdentifierUtil.ofUser(metadata, entities.get(Entity.EntityType.USER)));
              break;
            case GROUP:
              nameIdentifierMap.put(
                  Entity.EntityType.GROUP,
                  NameIdentifierUtil.ofGroup(metalake, entities.get(Entity.EntityType.GROUP)));
              break;
            case ROLE:
              nameIdentifierMap.put(
                  Entity.EntityType.ROLE,
                  NameIdentifierUtil.ofRole(metalake, entities.get(Entity.EntityType.ROLE)));
              break;
            case TAG:
              nameIdentifierMap.put(
                  Entity.EntityType.TAG,
                  NameIdentifierUtil.ofTag(metalake, entities.get(Entity.EntityType.TAG)));
              break;
            default:
              break;
          }
        });

    // Extract fullName and metadataObjectType
    if (fullName != null && metadataObjectType != null && metalake != null) {
      MetadataObject.Type type =
          MetadataObject.Type.valueOf(metadataObjectType.toUpperCase(Locale.ROOT));
      NameIdentifier nameIdentifier =
          MetadataObjectUtil.toEntityIdent(metalake, MetadataObjects.parse(fullName, type));
      nameIdentifierMap.putAll(
          MetadataAuthzHelper.spiltMetadataNames(
              metalake, MetadataObjectUtil.toEntityType(type), nameIdentifier));
    }

    return nameIdentifierMap;
  }

  public static String extractMetadataObjectTypeFromParameters(
      Parameter[] parameters, Object[] args) {
    for (int i = 0; i < parameters.length; i++) {
      Parameter parameter = parameters[i];
      AuthorizationObjectType objectType = parameter.getAnnotation(AuthorizationObjectType.class);
      if (objectType != null) {
        return String.valueOf(args[i]).toUpperCase();
      }
    }
    return null;
  }

  public static void buildNameIdentifierForBatchAuthorization(
      Map<Entity.EntityType, NameIdentifier> metadataNames, String name, Entity.EntityType type) {
    NameIdentifier metalake = metadataNames.get(Entity.EntityType.METALAKE);
    if (Objects.requireNonNull(type) == Entity.EntityType.TAG) {
      metadataNames.put(
          Entity.EntityType.TAG,
          NameIdentifierUtil.ofTag(NameIdentifierUtil.getMetalake(metalake), name));
      return;
    }
    throw new UnsupportedOperationException(
        "Unsupported to build NameIdentifier for batch authorization target");
  }
}
