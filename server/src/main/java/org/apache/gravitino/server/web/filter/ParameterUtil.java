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

import java.lang.annotation.Annotation;
import java.lang.reflect.Parameter;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import org.apache.gravitino.Entity;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.NameIdentifier;
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
    Map<Entity.EntityType, String> entities = extractEntitiesFromAnnotations(parameters, args);

    // Build name identifiers from extracted entities
    Map<Entity.EntityType, NameIdentifier> nameIdentifierMap =
        NameIdentifierUtil.buildNameIdentifierMap(entities);

    // Extract full name and object type for metadata objects
    Optional<String> fullName =
        extractAnnotatedValue(parameters, args, AuthorizationFullName.class);
    Optional<String> metadataObjectType =
        extractAnnotatedValue(parameters, args, AuthorizationObjectType.class);

    // Handle fullName and metadataObjectType if present
    if (fullName.isPresent() && metadataObjectType.isPresent()) {
      String metalake = entities.get(Entity.EntityType.METALAKE);
      if (metalake != null) {
        MetadataObject.Type type =
            MetadataObject.Type.valueOf(metadataObjectType.get().toUpperCase(Locale.ROOT));
        NameIdentifier nameIdentifier =
            MetadataObjectUtil.toEntityIdent(metalake, MetadataObjects.parse(fullName.get(), type));
        nameIdentifierMap.putAll(
            NameIdentifierUtil.splitNameIdentifier(
                metalake, MetadataObjectUtil.toEntityType(type), nameIdentifier));
      }
    }

    return nameIdentifierMap;
  }

  private static Map<Entity.EntityType, String> extractEntitiesFromAnnotations(
      Parameter[] parameters, Object[] args) {
    Map<Entity.EntityType, String> entities = new HashMap<>();
    for (int i = 0; i < parameters.length; i++) {
      AuthorizationMetadata annotation = parameters[i].getAnnotation(AuthorizationMetadata.class);
      if (annotation != null) {
        entities.put(annotation.type(), String.valueOf(args[i]));
      }
    }
    return entities;
  }

  private static Optional<String> extractAnnotatedValue(
      Parameter[] parameters, Object[] args, Class<? extends Annotation> annotationClass) {
    for (int i = 0; i < parameters.length; i++) {
      if (parameters[i].getAnnotation(annotationClass) != null) {
        return Optional.of(String.valueOf(args[i]));
      }
    }
    return Optional.empty();
  }

  public static Optional<String> extractMetadataObjectTypeFromParameters(
      Parameter[] parameters, Object[] args) {
    for (int i = 0; i < parameters.length; i++) {
      Parameter parameter = parameters[i];
      AuthorizationObjectType objectType = parameter.getAnnotation(AuthorizationObjectType.class);
      if (objectType != null) {
        return Optional.of(String.valueOf(args[i]).toUpperCase());
      }
    }
    return Optional.empty();
  }

  public static void buildNameIdentifierForBatchAuthorization(
      Map<Entity.EntityType, NameIdentifier> metadataNames, String name, Entity.EntityType type) {
    NameIdentifier metalake = metadataNames.get(Entity.EntityType.METALAKE);
    if (Objects.requireNonNull(type) == Entity.EntityType.TAG) {
      metadataNames.put(
          Entity.EntityType.TAG,
          NameIdentifierUtil.ofTag(NameIdentifierUtil.getMetalake(metalake), name));
      return;
    } else if (type == Entity.EntityType.POLICY) {
      metadataNames.put(
          Entity.EntityType.POLICY,
          NameIdentifierUtil.ofPolicy(NameIdentifierUtil.getMetalake(metalake), name));
      return;
    }
    throw new UnsupportedOperationException(
        "Unsupported to build NameIdentifier for batch authorization target");
  }
}
