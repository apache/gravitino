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
import java.util.Map;
import org.apache.gravitino.Entity.EntityType;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.server.authorization.annotations.ExpressionCondition;
import org.apache.gravitino.server.authorization.annotations.IcebergAuthorizationMetadata;
import org.apache.gravitino.server.web.filter.BaseMetadataAuthorizationMethodInterceptor.AuthorizationHandler;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.iceberg.rest.requests.RenameTableRequest;

/**
 * Handler for RENAME_TABLE operations. Performs authorization checks for cross-namespace renames
 * which require stricter checks than same-namespace renames, following MySQL privilege model.
 */
@SuppressWarnings("FormatStringAnnotation")
public class RenameTableAuthzHandler implements AuthorizationHandler {
  private final Parameter[] parameters;
  private final Object[] args;
  private boolean crossNamespaceRename = false;
  private String destinationSchema = null;

  public RenameTableAuthzHandler(Parameter[] parameters, Object[] args) {
    this.parameters = parameters;
    this.args = args;
  }

  @Override
  public void process(Map<EntityType, NameIdentifier> nameIdentifierMap) {
    RenameTableRequest renameTableRequest = null;
    for (int i = 0; i < parameters.length; i++) {
      IcebergAuthorizationMetadata metadata =
          parameters[i].getAnnotation(IcebergAuthorizationMetadata.class);
      if (metadata != null
          && metadata.type() == IcebergAuthorizationMetadata.RequestType.RENAME_TABLE) {
        renameTableRequest = (RenameTableRequest) args[i];
        break;
      }
    }

    if (renameTableRequest == null) {
      throw new ForbiddenException("RenameTableRequest not found in parameters");
    }

    // Extract metalake and catalog from nameIdentifierMap
    NameIdentifier metalakeIdent = nameIdentifierMap.get(EntityType.METALAKE);
    NameIdentifier catalogIdent = nameIdentifierMap.get(EntityType.CATALOG);

    if (metalakeIdent == null || catalogIdent == null) {
      throw new ForbiddenException("Missing metalake or catalog context for authorization");
    }

    String metalakeName = metalakeIdent.name();
    String catalog = catalogIdent.name();

    // Extract source table information from the request and add to map
    // The source table is NOT extracted via standard @AuthorizationMetadata annotations
    // because it's embedded in the RenameTableRequest body
    String sourceSchema = renameTableRequest.source().namespace().level(0);
    String sourceTable = renameTableRequest.source().name();

    nameIdentifierMap.put(
        EntityType.SCHEMA, NameIdentifierUtil.ofSchema(metalakeName, catalog, sourceSchema));
    nameIdentifierMap.put(
        EntityType.TABLE,
        NameIdentifierUtil.ofTable(metalakeName, catalog, sourceSchema, sourceTable));

    destinationSchema = renameTableRequest.destination().namespace().level(0);
    crossNamespaceRename = !sourceSchema.equals(destinationSchema);
  }

  @Override
  public boolean authorizationCompleted() {
    return false;
  }

  @Override
  public Map<ExpressionCondition, Boolean> expressionConditionContext() {
    return Map.of(ExpressionCondition.RENAMING_CROSSING_NAMESPACE, crossNamespaceRename);
  }

  @Override
  public Map<String, Object> additionalPathParams() {
    return destinationSchema == null
        ? Map.of()
        : Map.of("destinationSchema", destinationSchema, "crossNamespaceRename", crossNamespaceRename);
  }
}
