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

package org.apache.gravitino.server.web.filter.authorization;

import static org.apache.gravitino.server.web.filter.ParameterUtil.buildNameIdentifierForBatchAuthorization;
import static org.apache.gravitino.server.web.filter.ParameterUtil.extractFromParameters;

import com.google.common.base.Preconditions;
import java.lang.reflect.Parameter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Entity;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.authorization.AuthorizationRequestContext;
import org.apache.gravitino.dto.requests.TagValuesAssociateRequest;
import org.apache.gravitino.dto.requests.TagsAssociateRequest;
import org.apache.gravitino.server.web.rest.MetadataObjectTagOperations;
import org.apache.gravitino.tag.TagValue;

/**
 * Metadata object authorization for {@link
 * MetadataObjectTagOperations#associateTagsForObject(String, String, String, TagsAssociateRequest)}
 */
public class AssociateTagAuthorizationExecutor extends CommonAuthorizerExecutor
    implements AuthorizationExecutor {

  private final Parameter[] parameters;
  private final Object[] args;

  public AssociateTagAuthorizationExecutor(
      String expression,
      Parameter[] parameters,
      Object[] args,
      Map<Entity.EntityType, NameIdentifier> metadataContext,
      Map<String, Object> pathParams,
      Optional<String> entityType) {
    super(expression, metadataContext, pathParams, entityType);
    this.parameters = parameters;
    this.args = args;
  }

  @Override
  public boolean execute(AuthorizationRequestContext context) throws Exception {
    Object request = extractFromParameters(parameters, args);
    if (request == null) {
      return false;
    }

    context.setOriginalAuthorizationExpression(expression);
    Entity.EntityType targetType =
        Entity.EntityType.TAG; // Tags are the only supported batch target here

    Preconditions.checkArgument(
        request instanceof TagsAssociateRequest || request instanceof TagValuesAssociateRequest,
        "Only tag can use AssociateTagAuthorizationExecutor, please contact the administrator.");

    TagValue[] tagsToAdd;
    TagValue[] tagsToRemove;
    if (request instanceof TagsAssociateRequest) {
      TagsAssociateRequest tagsAssociateRequest = (TagsAssociateRequest) request;
      tagsToAdd = toNoValue(tagsAssociateRequest.getTagsToAdd());
      tagsToRemove = toNoValue(tagsAssociateRequest.getTagsToRemove());
    } else {
      TagValuesAssociateRequest tagValuesAssociateRequest = (TagValuesAssociateRequest) request;
      tagsToAdd = toNoValue(tagValuesAssociateRequest.tagNamesToAdd());
      tagsToRemove = toNoValue(tagValuesAssociateRequest.tagNamesToRemove());
    }

    // Authorize both 'tagsToAdd' and 'tagsToRemove' fields.
    return authorizeTag(tagsToAdd, context, targetType)
        && authorizeTag(tagsToRemove, context, targetType);
  }

  /**
   * Performs batch authorization for a given field (e.g., "tagsToAdd" or "tagsToRemove") containing
   * an array of tag values.
   *
   * @param tagValues tag values
   * @param context The shared authorization request context.
   * @param targetType The entity type being authorized (expected to be TAG).
   * @return {@code true} if all tags in the field pass authorization; {@code false} otherwise.
   */
  private boolean authorizeTag(
      TagValue[] tagValues, AuthorizationRequestContext context, Entity.EntityType targetType) {

    // Treat null or empty arrays as no-op (implicitly authorized)
    if (tagValues == null) {
      return true;
    }

    for (TagValue tagValue : tagValues) {
      // Use a fresh context copy for each tag to avoid cross-contamination
      Map<Entity.EntityType, NameIdentifier> currentContext = new HashMap<>(this.metadataContext);
      buildNameIdentifierForBatchAuthorization(currentContext, tagValue.name(), targetType);

      boolean authorized =
          authorizationExpressionEvaluator.evaluate(
              currentContext, pathParams, context, entityType);

      if (!authorized) {
        return false; // Fail fast on first unauthorized tag
      }
    }
    return true;
  }

  private static TagValue[] toNoValue(String[] tags) {
    if (tags == null) {
      return new TagValue[0];
    }
    return Arrays.stream(tags)
        .filter(StringUtils::isNotBlank)
        .map(TagValue::noValue)
        .toArray(TagValue[]::new);
  }
}
