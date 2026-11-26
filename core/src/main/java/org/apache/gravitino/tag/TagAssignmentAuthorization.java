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
package org.apache.gravitino.tag;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.gravitino.Entity;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.exceptions.OptimisticLockException;
import org.apache.gravitino.meta.NamespacedEntityId;
import org.apache.gravitino.utils.NameIdentifierUtil;

/** Carries identities and tag classifications checked by a synchronous assignment request. */
public final class TagAssignmentAuthorization implements AutoCloseable {
  private static final ThreadLocal<TagAssignmentAuthorization> CURRENT = new ThreadLocal<>();

  @Nullable private final TagAssignmentAuthorization previous;
  private final Set<NameIdentifier> descriptiveTags = new HashSet<>();
  private final Map<NameIdentifier, Long> checkedTags = new HashMap<>();
  private final Map<NameIdentifier, String> checkedTagNames = new HashMap<>();
  private final Map<NameIdentifier, Optional<NamespacedEntityId>> schemaAncestors =
      new LinkedHashMap<>();
  private boolean targetChecked;
  private boolean targetAbsent;
  @Nullable private NameIdentifier targetIdentifier;
  @Nullable private Entity.EntityType targetType;
  @Nullable private long[] targetIds;

  private TagAssignmentAuthorization(@Nullable TagAssignmentAuthorization previous) {
    this.previous = previous;
  }

  /**
   * Opens a request scope.
   *
   * @return the new scope
   */
  public static TagAssignmentAuthorization open() {
    TagAssignmentAuthorization scope = new TagAssignmentAuthorization(CURRENT.get());
    CURRENT.set(scope);
    return scope;
  }

  /**
   * Returns whether a request scope is active on the current thread.
   *
   * @return whether a scope is active
   */
  public static boolean isActive() {
    return CURRENT.get() != null;
  }

  /**
   * Records the tag identity observed by authorization.
   *
   * @param tag the tag identifier
   * @param id the observed stable tag ID
   */
  public static void recordIdentity(NameIdentifier tag, long id) {
    recordIdentity(tag, id, tag.name());
  }

  /**
   * Records the stored spelling of a tag along with its observed identity.
   *
   * @param tag the requested tag identifier
   * @param id the observed stable tag ID
   * @param storedName the name returned by the store
   */
  public static void recordIdentity(NameIdentifier tag, long id, String storedName) {
    TagAssignmentAuthorization scope = CURRENT.get();
    if (scope != null) {
      scope.checkedTags.putIfAbsent(tag, id);
      scope.checkedTagNames.putIfAbsent(tag, storedName);
    }
  }

  /**
   * Returns the stored spelling observed for a requested tag.
   *
   * @param tag the requested tag identifier
   * @return the stored spelling, or null if none was recorded
   */
  @Nullable
  public static String checkedTagName(NameIdentifier tag) {
    TagAssignmentAuthorization scope = CURRENT.get();
    return scope == null ? null : scope.checkedTagNames.get(tag);
  }

  /**
   * Returns the tag ID observed by authorization, or null when none was recorded.
   *
   * @param tag the tag identifier
   * @return the observed ID, or null
   */
  @Nullable
  public static Long checkedIdentity(NameIdentifier tag) {
    TagAssignmentAuthorization scope = CURRENT.get();
    return scope == null ? null : scope.checkedTags.get(tag);
  }

  /**
   * Records the target identity observed before authorization.
   *
   * @param identifier the normalized target identifier
   * @param type the target type
   * @param target the observed identity, or null if the target was absent
   */
  public static void recordTarget(
      NameIdentifier identifier, Entity.EntityType type, @Nullable NamespacedEntityId target) {
    TagAssignmentAuthorization scope = CURRENT.get();
    if (scope != null) {
      scope.targetChecked = true;
      scope.targetAbsent = target == null;
      scope.targetIdentifier = identifier;
      scope.targetType = type;
      scope.targetIds = target == null ? null : target.fullIds();
    }
  }

  /**
   * Records the closest existing ancestor when the target was not yet materialized.
   *
   * @param ancestor the observed ancestor identity
   */
  public static void recordMissingTargetAncestor(NamespacedEntityId ancestor) {
    TagAssignmentAuthorization scope = CURRENT.get();
    if (scope != null && scope.targetChecked && scope.targetAbsent && scope.targetIds == null) {
      scope.targetIds = ancestor.fullIds();
    }
  }

  /**
   * Records a schema ancestor in outermost-first order.
   *
   * @param identifier the normalized schema identifier
   * @param ancestor the observed identity, or null when absent
   */
  public static void recordSchemaAncestor(
      NameIdentifier identifier, @Nullable NamespacedEntityId ancestor) {
    TagAssignmentAuthorization scope = CURRENT.get();
    if (scope != null) {
      scope.schemaAncestors.putIfAbsent(identifier, Optional.ofNullable(ancestor));
    }
  }

  /**
   * Returns the schema ancestors recorded for the current request in lock order.
   *
   * @return the recorded ancestors
   */
  public static Map<NameIdentifier, Optional<NamespacedEntityId>> checkedSchemaAncestors() {
    TagAssignmentAuthorization scope = CURRENT.get();
    return scope == null
        ? Collections.emptyMap()
        : Collections.unmodifiableMap(new LinkedHashMap<>(scope.schemaAncestors));
  }

  /**
   * Returns the observed ID for the request's target or an authorization ancestor, when it exists.
   *
   * @param identifier the metadata identifier being authorized
   * @param type the metadata type being authorized
   * @return the observed ID, or null when the scope has no matching identity
   */
  @Nullable
  public static Long checkedTargetIdentity(NameIdentifier identifier, MetadataObject.Type type) {
    TagAssignmentAuthorization scope = CURRENT.get();
    if (scope == null
        || !scope.targetChecked
        || scope.targetType == null
        || scope.targetIdentifier == null) {
      return null;
    }
    if (type == MetadataObject.Type.SCHEMA && scope.schemaAncestors.containsKey(identifier)) {
      return scope.schemaAncestors.get(identifier).map(NamespacedEntityId::entityId).orElse(null);
    }
    if (scope.targetIds == null) {
      return null;
    }
    int index = checkedPathIndex(scope, identifier, type);
    return index >= 0 && index < scope.targetIds.length ? scope.targetIds[index] : null;
  }

  /**
   * Returns whether a relevant target or ancestor was absent at authorization.
   *
   * @param identifier the metadata identifier being authorized
   * @param type the metadata type being authorized
   * @return whether the matching target or ancestor was absent
   */
  public static boolean wasCheckedAbsent(NameIdentifier identifier, MetadataObject.Type type) {
    TagAssignmentAuthorization scope = CURRENT.get();
    if (scope != null
        && type == MetadataObject.Type.SCHEMA
        && scope.schemaAncestors.containsKey(identifier)) {
      return scope.schemaAncestors.get(identifier).isEmpty();
    }
    if (scope == null
        || !scope.targetChecked
        || !scope.targetAbsent
        || scope.targetIdentifier == null) {
      return false;
    }
    int index = checkedPathIndex(scope, identifier, type);
    return index >= 0 && (scope.targetIds == null || index >= scope.targetIds.length);
  }

  /**
   * Returns whether this metadata type can represent the checked target or an ancestor.
   *
   * @param type the metadata type being authorized
   * @return whether the type can be relevant to the checked target
   */
  public static boolean hasTargetType(MetadataObject.Type type) {
    TagAssignmentAuthorization scope = CURRENT.get();
    return scope != null
        && scope.targetChecked
        && scope.targetType != null
        && (scope.targetType.name().equals(type.name())
            || type == MetadataObject.Type.METALAKE
            || type == MetadataObject.Type.CATALOG
            || type == MetadataObject.Type.SCHEMA
            || type == MetadataObject.Type.TABLE);
  }

  /**
   * Rejects a write if the target or an ancestor has changed identity.
   *
   * @param identifier the target identifier
   * @param type the target type
   * @param current the target identity used for the write
   */
  public static void checkTarget(
      NameIdentifier identifier, Entity.EntityType type, NamespacedEntityId current) {
    TagAssignmentAuthorization scope = CURRENT.get();
    if (scope != null
        && scope.targetChecked
        && (scope.targetType != type
            || !identifier.equals(scope.targetIdentifier)
            || (scope.targetAbsent
                ? scope.targetIds == null
                    || current.fullIds().length <= scope.targetIds.length
                    || !Arrays.equals(
                        scope.targetIds, Arrays.copyOf(current.fullIds(), scope.targetIds.length))
                : !Arrays.equals(scope.targetIds, current.fullIds())))) {
      throw new OptimisticLockException(
          "The %s %s changed after authorization; retry the operation", type, identifier);
    }
  }

  /**
   * Records a tag whose assignment used descriptive-tag authorization.
   *
   * @param tag the tag identifier
   */
  public static void requireDescriptive(NameIdentifier tag) {
    TagAssignmentAuthorization scope = CURRENT.get();
    if (scope != null) {
      scope.descriptiveTags.add(tag);
    }
  }

  /**
   * Returns whether the named tag must remain descriptive at mutation.
   *
   * @param tag the tag identifier
   * @return whether the condition applies
   */
  public static boolean requiresDescriptive(NameIdentifier tag) {
    TagAssignmentAuthorization scope = CURRENT.get();
    return scope != null && scope.descriptiveTags.contains(tag);
  }

  /** {@inheritDoc} */
  @Override
  public void close() {
    if (previous == null) {
      CURRENT.remove();
    } else {
      CURRENT.set(previous);
    }
  }

  private static int checkedPathIndex(
      TagAssignmentAuthorization scope, NameIdentifier identifier, MetadataObject.Type type) {
    if (scope.targetType.name().equals(type.name()) && identifier.equals(scope.targetIdentifier)) {
      if (!scope.targetAbsent && scope.targetIds != null) {
        return scope.targetIds.length - 1;
      }
      switch (type) {
        case CATALOG:
          return 1;
        case SCHEMA:
          return 2;
        case COLUMN:
          return 4;
        default:
          return 3;
      }
    }
    if (type == MetadataObject.Type.METALAKE
        && identifier.equals(NameIdentifier.of(scope.targetIdentifier.namespace().level(0)))) {
      return 0;
    }
    if (type == MetadataObject.Type.CATALOG
        && identifier.equals(NameIdentifierUtil.getCatalogIdentifier(scope.targetIdentifier))) {
      return 1;
    }
    if (type == MetadataObject.Type.SCHEMA
        && scope.targetIdentifier.namespace().length() > 1
        && identifier.equals(NameIdentifierUtil.getSchemaIdentifier(scope.targetIdentifier))) {
      return 2;
    }
    if (type == MetadataObject.Type.TABLE
        && scope.targetType == Entity.EntityType.COLUMN
        && identifier.equals(NameIdentifierUtil.getTableIdentifier(scope.targetIdentifier))) {
      return 3;
    }
    return -1;
  }
}
