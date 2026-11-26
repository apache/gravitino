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

import static org.apache.gravitino.server.authorization.expression.AuthorizationExpressionConstants.APPLY_ACCESS_TAG_AUTHORIZATION_EXPRESSION;
import static org.apache.gravitino.server.web.filter.ParameterUtil.buildNameIdentifierForBatchAuthorization;
import static org.apache.gravitino.server.web.filter.ParameterUtil.extractFromParameters;

import com.google.common.base.Preconditions;
import java.lang.reflect.Parameter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiPredicate;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Entity;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.HasIdentifier;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.RelationalEntity;
import org.apache.gravitino.authorization.AuthorizationRequestContext;
import org.apache.gravitino.dto.requests.TagValuesAssociateRequest;
import org.apache.gravitino.dto.requests.TagsAssociateRequest;
import org.apache.gravitino.exceptions.NoSuchTagException;
import org.apache.gravitino.exceptions.NotFoundException;
import org.apache.gravitino.meta.PolicyEntity;
import org.apache.gravitino.policy.HiddenPolicyTag;
import org.apache.gravitino.policy.Policy;
import org.apache.gravitino.server.authorization.GravitinoAuthorizerProvider;
import org.apache.gravitino.server.authorization.MetadataIdConverter;
import org.apache.gravitino.server.authorization.PassThroughAuthorizer;
import org.apache.gravitino.server.authorization.expression.AuthorizationExpressionEvaluator;
import org.apache.gravitino.server.web.rest.MetadataObjectTagOperations;
import org.apache.gravitino.storage.relational.service.EntityIdService;
import org.apache.gravitino.tag.Tag;
import org.apache.gravitino.tag.TagAssignmentAuthorization;
import org.apache.gravitino.tag.TagManager;
import org.apache.gravitino.tag.TagValue;
import org.apache.gravitino.utils.MetadataObjectUtil;
import org.apache.gravitino.utils.NameIdentifierUtil;

/**
 * Metadata object authorization for {@link
 * MetadataObjectTagOperations#associateTagsForObject(String, String, String, TagsAssociateRequest)}
 */
public class AssociateTagAuthorizationExecutor extends CommonAuthorizerExecutor
    implements AuthorizationExecutor {

  private final Parameter[] parameters;
  private final Object[] args;
  AuthorizationExpressionEvaluator accessTagEvaluator;
  boolean authorizationDisabled;
  private boolean targetCaptured;
  BiPredicate<String, String> accessTagClassifier =
      (metalake, tag) ->
          isAccessTag(
              GravitinoEnv.getInstance()
                  .tagDispatcher()
                  .listPolicyAssociationsForTag(metalake, tag));

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
    this.accessTagEvaluator =
        new AuthorizationExpressionEvaluator(APPLY_ACCESS_TAG_AUTHORIZATION_EXPRESSION);
    this.authorizationDisabled =
        GravitinoAuthorizerProvider.getInstance().getGravitinoAuthorizer()
            instanceof PassThroughAuthorizer;
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

    boolean validRequest = true;
    try {
      if (request instanceof TagsAssociateRequest) {
        ((TagsAssociateRequest) request).validate();
      } else {
        ((TagValuesAssociateRequest) request).validate();
      }
    } catch (IllegalArgumentException e) {
      // The resource rejects invalid requests before mutation; preserve its normal error response.
      validRequest = false;
    }

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
    return authorizeTag(tagsToAdd, context, targetType, validRequest)
        && authorizeTag(tagsToRemove, context, targetType, validRequest);
  }

  static boolean isAccessTag(RelationalEntity<?>[] associations) {
    return Arrays.stream(associations)
        .anyMatch(
            association ->
                ((PolicyEntity) association.targetEntity()).policyType()
                    == Policy.BuiltInType.ACCESS_CONTROL);
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
      TagValue[] tagValues,
      AuthorizationRequestContext context,
      Entity.EntityType targetType,
      boolean validRequest) {

    // Treat null or empty arrays as no-op (implicitly authorized)
    if (tagValues == null) {
      return true;
    }

    for (TagValue tagValue : tagValues) {
      // Use a fresh context copy for each tag to avoid cross-contamination
      Map<Entity.EntityType, NameIdentifier> currentContext = new HashMap<>(this.metadataContext);
      buildNameIdentifierForBatchAuthorization(currentContext, tagValue.name(), targetType);

      String metalake = currentContext.get(Entity.EntityType.METALAKE).name();
      if (validRequest
          && !authorizationDisabled
          && HiddenPolicyTag.isReservedName(tagValue.name())) {
        throw new NoSuchTagException(
            "Tag with name %s under metalake %s does not exist", tagValue.name(), metalake);
      }
      boolean accessTag = false;
      if (validRequest && !authorizationDisabled) {
        try {
          if (TagAssignmentAuthorization.isActive()) {
            captureTargetIdentity();
            Tag tag =
                GravitinoEnv.getInstance()
                    .internalTagDispatcher()
                    .getTag(metalake, tagValue.name());
            Preconditions.checkState(tag instanceof HasIdentifier, "Tag identity is unavailable");
            TagAssignmentAuthorization.recordIdentity(
                NameIdentifierUtil.ofTag(metalake, tagValue.name()),
                ((HasIdentifier) tag).id(),
                tag.name());
          }
          accessTag = accessTagClassifier.test(metalake, tagValue.name());
        } catch (NoSuchTagException missing) {
          // Missing tags use the original authorization and are rechecked during the write.
        }
      }
      if (validRequest && !authorizationDisabled && !accessTag) {
        TagAssignmentAuthorization.requireDescriptive(
            NameIdentifierUtil.ofTag(metalake, tagValue.name()));
      }
      context.setOriginalAuthorizationExpression(
          accessTag ? APPLY_ACCESS_TAG_AUTHORIZATION_EXPRESSION : expression);
      AuthorizationExpressionEvaluator evaluator =
          accessTag ? accessTagEvaluator : authorizationExpressionEvaluator;
      Map<String, Object> evaluationPath = pathParams;
      if (accessTag && entityType.isPresent() && entityType.get().equalsIgnoreCase("column")) {
        MetadataObject column =
            MetadataObjects.parse(
                (String) pathParams.get("p_fullName"), MetadataObject.Type.COLUMN);
        MetadataObject table = MetadataObjects.parent(column);
        evaluationPath = new HashMap<>(pathParams);
        evaluationPath.put("p_type", table.type().name());
        evaluationPath.put("p_fullName", table.fullName());
      }
      boolean authorized = evaluator.evaluate(currentContext, evaluationPath, context, entityType);

      if (!authorized) {
        return false; // Fail fast on first unauthorized tag
      }
    }
    return true;
  }

  private void captureTargetIdentity() {
    if (targetCaptured || entityType.isEmpty()) {
      return;
    }
    targetCaptured = true;
    MetadataObject.Type objectType = MetadataObject.Type.valueOf(entityType.get());
    if (!TagManager.SUPPORTED_METADATA_OBJECT_TYPES_FOR_TAGS.contains(objectType)) {
      return;
    }
    Entity.EntityType type = MetadataObjectUtil.toEntityType(objectType);
    NameIdentifier identifier = metadataContext.get(type);
    String metalake = metadataContext.get(Entity.EntityType.METALAKE).name();
    MetadataObject original = NameIdentifierUtil.toMetadataObject(identifier, type);
    MetadataObject normalized = original;
    try {
      normalized = MetadataIdConverter.normalizeMetadataObject(normalized, metalake);
      NameIdentifier normalizedIdentifier = MetadataObjectUtil.toEntityIdent(metalake, normalized);
      TagAssignmentAuthorization.recordTarget(
          normalizedIdentifier, type, EntityIdService.getEntityIds(normalizedIdentifier, type));
    } catch (NotFoundException missing) {
      TagAssignmentAuthorization.recordTarget(
          MetadataObjectUtil.toEntityIdent(metalake, normalized), type, null);
      captureClosestExistingAncestor(normalized, metalake);
    }
    captureSchemaAncestors(original, metalake);
  }

  private void captureSchemaAncestors(MetadataObject target, String metalake) {
    List<MetadataObject> parents =
        new ArrayList<>(MetadataObjectUtil.getParentMetadataObjects(target));
    Collections.reverse(parents);
    for (MetadataObject parent : parents) {
      if (parent.type() != MetadataObject.Type.SCHEMA) {
        continue;
      }
      MetadataObject normalized = parent;
      try {
        normalized = MetadataIdConverter.normalizeMetadataObject(parent, metalake);
        NameIdentifier identifier = MetadataObjectUtil.toEntityIdent(metalake, normalized);
        TagAssignmentAuthorization.recordSchemaAncestor(
            identifier, EntityIdService.getEntityIds(identifier, Entity.EntityType.SCHEMA));
      } catch (NotFoundException missing) {
        TagAssignmentAuthorization.recordSchemaAncestor(
            MetadataObjectUtil.toEntityIdent(metalake, normalized), null);
      }
    }
  }

  private void captureClosestExistingAncestor(MetadataObject target, String metalake) {
    List<MetadataObject> parents =
        new ArrayList<>(MetadataObjectUtil.getParentMetadataObjects(target));
    parents.add(MetadataObjects.of(null, metalake, MetadataObject.Type.METALAKE));
    EnumSet<MetadataObject.Type> seen = EnumSet.of(target.type());
    for (MetadataObject parent : parents) {
      if (!seen.add(parent.type())) {
        continue;
      }
      try {
        MetadataObject normalized = MetadataIdConverter.normalizeMetadataObject(parent, metalake);
        Entity.EntityType type = MetadataObjectUtil.toEntityType(normalized.type());
        TagAssignmentAuthorization.recordMissingTargetAncestor(
            EntityIdService.getEntityIds(
                MetadataObjectUtil.toEntityIdent(metalake, normalized), type));
        return;
      } catch (NotFoundException missing) {
        // Try the next physical ancestor. A missing target must not reuse an old target grant.
      }
    }
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
