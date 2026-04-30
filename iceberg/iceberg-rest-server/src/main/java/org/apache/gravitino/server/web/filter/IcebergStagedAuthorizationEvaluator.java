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

import java.util.Map;
import java.util.Optional;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Entity;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.authorization.AuthorizationRequestContext;
import org.apache.gravitino.iceberg.common.ops.IcebergCatalogWrapper;
import org.apache.gravitino.iceberg.service.IcebergCatalogWrapperManager;
import org.apache.gravitino.iceberg.service.authorization.IcebergRESTServerContext;
import org.apache.gravitino.server.authorization.annotations.AuthorizationExpression;
import org.apache.gravitino.server.authorization.annotations.ExpressionAction;
import org.apache.gravitino.server.authorization.annotations.ExpressionCondition;
import org.apache.gravitino.server.authorization.expression.AuthorizationExpressionEvaluator;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;

public class IcebergStagedAuthorizationEvaluator {

  public boolean evaluate(
      AuthorizationExpression authorizationExpression,
      Map<Entity.EntityType, NameIdentifier> nameIdentifierMap,
      Map<String, Object> pathParams,
      Map<ExpressionCondition, Boolean> conditionContext) {
    boolean previousAllowed =
        evaluateStage(
            authorizationExpression.expression(),
            ExpressionAction.EVALUATE,
            nameIdentifierMap,
            pathParams);
    if (previousAllowed) {
      return true;
    }

    boolean secondaryAllowed =
        evaluateConditionalStage(
            authorizationExpression.secondaryExpression(),
            authorizationExpression.secondaryExpressionCondition(),
            authorizationExpression.secondaryExpressionAction(),
            previousAllowed,
            nameIdentifierMap,
            pathParams,
            conditionContext);
    if (secondaryAllowed) {
      return true;
    }

    return evaluateConditionalStage(
        authorizationExpression.thirdExpression(),
        authorizationExpression.thirdExpressionCondition(),
        authorizationExpression.thirdExpressionAction(),
        secondaryAllowed,
        nameIdentifierMap,
        pathParams,
        conditionContext);
  }

  private boolean evaluateConditionalStage(
      String expression,
      ExpressionCondition condition,
      ExpressionAction action,
      boolean previousAllowed,
      Map<Entity.EntityType, NameIdentifier> nameIdentifierMap,
      Map<String, Object> pathParams,
      Map<ExpressionCondition, Boolean> conditionContext) {
    if (!shouldEvaluate(condition, previousAllowed, conditionContext)) {
      return false;
    }
    Map<Entity.EntityType, NameIdentifier> stageContext = nameIdentifierMap;
    if (condition == ExpressionCondition.RENAMING_CROSSING_NAMESPACE
        && pathParams.get("destinationSchema") instanceof String
        && expression.contains("ANY_CREATE_TABLE")) {
      stageContext = ContextUtil.withDestinationSchema(nameIdentifierMap, (String) pathParams.get("destinationSchema"));
    }
    return evaluateStage(expression, action, stageContext, pathParams);
  }

  private boolean shouldEvaluate(
      ExpressionCondition condition,
      boolean previousAllowed,
      Map<ExpressionCondition, Boolean> conditionContext) {
    return switch (condition) {
      case ALWAYS -> true;
      case PREVIOUS_EXPRESSION_FORBIDDEN -> !previousAllowed;
      case CONTAIN_REQUIRED_PRIVILEGES -> conditionContext.getOrDefault(condition, false);
      default -> conditionContext.getOrDefault(condition, false);
    };
  }

  private boolean evaluateStage(
      String expression,
      ExpressionAction action,
      Map<Entity.EntityType, NameIdentifier> nameIdentifierMap,
      Map<String, Object> pathParams) {
    if (StringUtils.isBlank(expression)) {
      return false;
    }

    boolean expressionAllowed =
        new AuthorizationExpressionEvaluator(expression)
            .evaluate(nameIdentifierMap, pathParams, new AuthorizationRequestContext(), Optional.empty());
    if (!expressionAllowed) {
      return false;
    }

    if (action == ExpressionAction.CHECK_METADATA_OBJECT_EXISTS) {
      return metadataObjectExists(nameIdentifierMap);
    }
    return true;
  }

  private boolean metadataObjectExists(Map<Entity.EntityType, NameIdentifier> nameIdentifierMap) {
    NameIdentifier catalogId = nameIdentifierMap.get(Entity.EntityType.CATALOG);
    NameIdentifier tableId = nameIdentifierMap.get(Entity.EntityType.TABLE);
    NameIdentifier viewId = nameIdentifierMap.get(Entity.EntityType.VIEW);
    if (catalogId == null || (tableId == null && viewId == null)) {
      return false;
    }

    NameIdentifier objectId = tableId != null ? tableId : viewId;
    IcebergCatalogWrapperManager wrapperManager =
        IcebergRESTServerContext.getInstance().catalogWrapperManager();
    if (wrapperManager == null) {
      return false;
    }

    IcebergCatalogWrapper catalogWrapper = wrapperManager.getCatalogWrapper(catalogId.name());
    TableIdentifier tableIdentifier = toTableIdentifier(objectId);
    if (tableId != null) {
      return catalogWrapper.tableExists(tableIdentifier);
    }

    return catalogWrapper.supportsViewOperations() && catalogWrapper.viewExists(tableIdentifier);
  }

  private TableIdentifier toTableIdentifier(NameIdentifier identifier) {
    String schemaName = identifier.namespace().level(identifier.namespace().length() - 1);
    Namespace namespace = Namespace.of(schemaName.split("\\."));
    return TableIdentifier.of(namespace, identifier.name());
  }

  private static class ContextUtil {
    private static Map<Entity.EntityType, NameIdentifier> withDestinationSchema(
        Map<Entity.EntityType, NameIdentifier> source, String destinationSchema) {
      NameIdentifier metalakeId = source.get(Entity.EntityType.METALAKE);
      NameIdentifier catalogId = source.get(Entity.EntityType.CATALOG);
      if (metalakeId == null || catalogId == null) {
        return source;
      }

      Map<Entity.EntityType, NameIdentifier> target = new java.util.HashMap<>(source);
      target.put(
          Entity.EntityType.SCHEMA,
          org.apache.gravitino.utils.NameIdentifierUtil.ofSchema(
              metalakeId.name(), catalogId.name(), destinationSchema));
      return target;
    }
  }
}
