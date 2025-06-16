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

package org.apache.gravitino.server.authorization;

import java.security.Principal;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.gravitino.Entity;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.authorization.Privilege;
import org.apache.gravitino.server.authorization.expression.AuthorizationExpressionEvaluator;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.PrincipalUtils;

/**
 * MetadataFilterHelper performs permission checks on the list data returned by the REST API based
 * on expressions or metadata types, and calls {@link GravitinoAuthorizer} for authorization,
 * returning only the metadata that the user has permission to access.
 */
public class MetadataFilterHelper {

  private MetadataFilterHelper() {}

  /**
   * Call {@link GravitinoAuthorizer} to filter the metadata list
   *
   * @param entityType for example, CATALOG, SCHEMA,TABLE, etc.
   * @param privilege for example, CREATE_CATALOG, CREATE_TABLE, etc.
   * @param metadataList metadata list.
   * @return metadata List that the user has permission to access.
   */
  public static NameIdentifier[] filterByPrivilege(
      String metalake,
      Entity.EntityType entityType,
      String privilege,
      NameIdentifier[] metadataList) {
    GravitinoAuthorizer gravitinoAuthorizer =
        GravitinoAuthorizerProvider.getInstance().getGravitinoAuthorizer();
    Principal currentPrincipal = PrincipalUtils.getCurrentPrincipal();
    return Arrays.stream(metadataList)
        .filter(
            metaDataName ->
                gravitinoAuthorizer.authorize(
                    currentPrincipal,
                    metalake,
                    NameIdentifierUtil.toMetadataObject(metaDataName, entityType),
                    Privilege.Name.valueOf(privilege)))
        .toArray(NameIdentifier[]::new);
  }

  /**
   * Call {@link AuthorizationExpressionEvaluator} to filter the metadata list
   *
   * @param metalake metalake
   * @param expression authorization expression
   * @param entityType for example, CATALOG, SCHEMA,TABLE, etc.
   * @param nameIdentifiers metaData list.
   * @return metadata List that the user has permission to access.
   */
  public static NameIdentifier[] filterByExpression(
      String metalake,
      String expression,
      Entity.EntityType entityType,
      NameIdentifier[] nameIdentifiers) {
    AuthorizationExpressionEvaluator authorizationExpressionEvaluator =
        new AuthorizationExpressionEvaluator(expression);
    return Arrays.stream(nameIdentifiers)
        .filter(
            metaDataName -> {
              Map<MetadataObject.Type, NameIdentifier> nameIdentifierMap =
                  spiltMetadataNames(metalake, entityType, metaDataName);
              return authorizationExpressionEvaluator.evaluate(nameIdentifierMap);
            })
        .toArray(NameIdentifier[]::new);
  }

  /**
   * Extract the parent metadata from NameIdentifier. For example, when given a Table
   * NameIdentifier, it returns a map containing the Table itself along with its parent Schema and
   * Catalog.
   *
   * @param metalake metalake
   * @param entityType metadata type
   * @param nameIdentifier metadata name
   * @return A map containing the metadata object and all its parent objects, keyed by their types
   */
  private static Map<MetadataObject.Type, NameIdentifier> spiltMetadataNames(
      String metalake, Entity.EntityType entityType, NameIdentifier nameIdentifier) {
    Map<MetadataObject.Type, NameIdentifier> nameIdentifierMap = new HashMap<>();
    nameIdentifierMap.put(MetadataObject.Type.METALAKE, NameIdentifierUtil.ofMetalake(metalake));
    switch (entityType) {
      case CATALOG:
        nameIdentifierMap.put(MetadataObject.Type.CATALOG, nameIdentifier);
        break;
      case SCHEMA:
        nameIdentifierMap.put(MetadataObject.Type.SCHEMA, nameIdentifier);
        nameIdentifierMap.put(
            MetadataObject.Type.CATALOG, NameIdentifierUtil.getCatalogIdentifier(nameIdentifier));
        break;
      case TABLE:
        nameIdentifierMap.put(MetadataObject.Type.TABLE, nameIdentifier);
        nameIdentifierMap.put(
            MetadataObject.Type.SCHEMA, NameIdentifierUtil.getSchemaIdentifier(nameIdentifier));
        nameIdentifierMap.put(
            MetadataObject.Type.CATALOG, NameIdentifierUtil.getCatalogIdentifier(nameIdentifier));
        break;
      default:
        break;
    }
    return nameIdentifierMap;
  }
}
