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
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
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
   * @param metadataType for example, CATALOG, SCHEMA,TABLE, etc.
   * @param privilege for example, CREATE_CATALOG, CREATE_TABLE, etc.
   * @param metadataList metadata list.
   * @return metadata List that the user has permission to access.
   */
  public static NameIdentifier[] filter(
      String metalake,
      MetadataObject.Type metadataType,
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
                    MetadataObjects.of(metadataType, metaDataName),
                    Privilege.Name.valueOf(privilege)))
        .toArray(NameIdentifier[]::new);
  }

  /**
   * Call {@link AuthorizationExpressionEvaluator} to filter the metadata list
   *
   * @param metalake metalake
   * @param expression authorization expression
   * @param metadataType for example, CATALOG, SCHEMA,TABLE, etc.
   * @param nameIdentifiers metaData list.
   * @return metadata List that the user has permission to access.
   */
  public static NameIdentifier[] filterByExpression(
      String metalake,
      String expression,
      MetadataObject.Type metadataType,
      NameIdentifier[] nameIdentifiers) {
    AuthorizationExpressionEvaluator authorizationExpressionEvaluator =
        new AuthorizationExpressionEvaluator(expression);
    return Arrays.stream(nameIdentifiers)
        .filter(
            metaDataName -> {
              Map<MetadataObject.Type, NameIdentifier> nameIdentifierMap =
                  spiltMetadataNames(metalake, metadataType, metaDataName);
              return authorizationExpressionEvaluator.evaluate(nameIdentifierMap);
            })
        .toArray(NameIdentifier[]::new);
  }

  /**
   * Extract the parent metadata from NameIdentify, for example, extract Schema and Catalog from
   * Table.
   *
   * @param metalake metalake
   * @param metadataType metadata type
   * @param nameIdentifier metadata name
   * @return metadata name
   */
  private static Map<MetadataObject.Type, NameIdentifier> spiltMetadataNames(
      String metalake, MetadataObject.Type metadataType, NameIdentifier nameIdentifier) {
    Map<MetadataObject.Type, NameIdentifier> nameIdentifierMap = new HashMap<>();
    nameIdentifierMap.put(MetadataObject.Type.METALAKE, NameIdentifierUtil.ofMetalake(metalake));
    switch (metadataType) {
      case CATALOG:
        nameIdentifierMap.put(MetadataObject.Type.CATALOG, nameIdentifier);
        break;
      case SCHEMA:
        String[] schemaParents = nameIdentifier.namespace().levels();
        nameIdentifierMap.put(MetadataObject.Type.SCHEMA, nameIdentifier);
        nameIdentifierMap.put(
            MetadataObject.Type.CATALOG, NameIdentifierUtil.ofCatalog(metalake, schemaParents[1]));
        break;
      case TABLE:
        String[] tableParents = nameIdentifier.namespace().levels();
        nameIdentifierMap.put(MetadataObject.Type.TABLE, nameIdentifier);
        nameIdentifierMap.put(
            MetadataObject.Type.SCHEMA,
            NameIdentifierUtil.ofSchema(metalake, tableParents[0], tableParents[2]));
        nameIdentifierMap.put(
            MetadataObject.Type.CATALOG, NameIdentifierUtil.ofCatalog(metalake, tableParents[1]));
        break;
      default:
        break;
    }
    return nameIdentifierMap;
  }
}
