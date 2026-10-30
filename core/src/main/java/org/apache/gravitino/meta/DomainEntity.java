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
package org.apache.gravitino.meta;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nullable;
import lombok.AccessLevel;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import lombok.experimental.Accessors;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Audit;
import org.apache.gravitino.Auditable;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityFieldLimits;
import org.apache.gravitino.Field;
import org.apache.gravitino.HasIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.domain.Domain;
import org.apache.gravitino.tag.TagValue;

/**
 * The stored form of a {@link Domain}. It holds the domain's data only; the operations on {@link
 * Domain} are served by the domain manager.
 *
 * <p>The backing tag is stored with the domain as the ID of the tag and an optional value; the tag
 * name is read from the tag when the domain is read. If the tag is deleted, the ID stays on the
 * domain but has no name, so the domain has no backing tag. The parent is not stored with the
 * domain: it comes from the sub-domain relation and is set on the entity when it is read.
 */
@Getter
@Accessors(fluent = true)
@EqualsAndHashCode
@ToString
public class DomainEntity implements Entity, Auditable, HasIdentifier {

  /** The unique id of the domain entity. */
  public static final Field ID =
      Field.required("id", Long.class, "The unique id of the domain entity.");

  /** The name of the domain entity. */
  public static final Field NAME =
      Field.required("name", "The name of the domain entity.", EntityFieldLimits.MAX_NAME_LENGTH);

  /** The comment of the domain entity. */
  public static final Field COMMENT =
      Field.optional(
          "comment", "The comment of the domain entity.", EntityFieldLimits.MAX_COMMENT_LENGTH);

  /** The properties of the domain entity. */
  public static final Field PROPERTIES =
      Field.optional("properties", Map.class, "The properties of the domain entity.");

  /** The status of the domain entity. */
  public static final Field STATUS =
      Field.required("status", Domain.Status.class, "The status of the domain entity.");

  /** The ID of the backing tag of the domain entity. */
  public static final Field BACKING_TAG_ID =
      Field.optional(
          "backing_tag_id", Long.class, "The ID of the backing tag of the domain entity.");

  /** The value of the backing tag of the domain entity. */
  public static final Field BACKING_TAG_VALUE =
      Field.optional(
          "backing_tag_value",
          "The value of the backing tag of the domain entity.",
          EntityFieldLimits.MAX_TAG_VALUE_LENGTH);

  /** The audit details of the domain entity. */
  public static final Field AUDIT_INFO =
      Field.required("audit_info", Audit.class, "The audit details of the domain entity.");

  private final Long id;
  private final String name;
  private final Namespace namespace;
  @Nullable private final String comment;
  private final Map<String, String> properties;
  private final Domain.Status status;

  @Getter(AccessLevel.NONE)
  @Nullable
  private final String parent;

  /** The ID of the backing tag, or null if the domain has none. */
  @Nullable private final Long backingTagId;

  /** The name of the backing tag, or null if the domain has none or the tag is deleted. */
  @Getter(AccessLevel.NONE)
  @Nullable
  private final String backingTagName;

  /** The value of the backing tag, or null if the tag is bound without a value. */
  @Nullable private final String backingTagValue;

  private final Audit auditInfo;

  @lombok.Builder(setterPrefix = "with", toBuilder = true)
  private DomainEntity(
      Long id,
      String name,
      Namespace namespace,
      @Nullable String comment,
      Map<String, String> properties,
      Domain.Status status,
      @Nullable String parent,
      @Nullable Long backingTagId,
      @Nullable String backingTagName,
      @Nullable String backingTagValue,
      Audit auditInfo) {
    this.id = id;
    this.name = name;
    this.namespace = namespace;
    this.comment = comment;
    this.properties = properties;
    this.status = status;
    this.parent = parent;
    this.backingTagId = backingTagId;
    // The name is read from the bound tag, so it follows the ID: clearing the ID unbinds the tag.
    this.backingTagName = backingTagId == null ? null : backingTagName;
    this.backingTagValue = backingTagValue;
    this.auditInfo = auditInfo;
    validate();
    Preconditions.checkArgument(
        backingTagId != null || backingTagValue == null,
        "The backing tag value of domain %s needs a backing tag ID",
        name);
    // A tag bound without a value is stored as NULL, so the value is never blank.
    Preconditions.checkArgument(
        backingTagValue == null || StringUtils.isNotBlank(backingTagValue),
        "The backing tag value of domain %s must not be blank",
        name);
  }

  @Override
  public Map<Field, Object> fields() {
    Map<Field, Object> fields = Maps.newHashMap();
    fields.put(ID, id);
    fields.put(NAME, name);
    fields.put(COMMENT, comment);
    fields.put(PROPERTIES, properties);
    fields.put(STATUS, status);
    fields.put(BACKING_TAG_ID, backingTagId);
    fields.put(BACKING_TAG_VALUE, backingTagValue);
    fields.put(AUDIT_INFO, auditInfo);

    return Collections.unmodifiableMap(fields);
  }

  @Override
  public EntityType type() {
    return EntityType.DOMAIN;
  }

  /**
   * Returns the name of the parent domain.
   *
   * @return The name of the parent domain, or empty if the domain is top-level.
   */
  public Optional<String> parent() {
    return Optional.ofNullable(parent);
  }

  /**
   * Returns the backing tag of the domain. It is empty if the domain has no backing tag or the tag
   * has been deleted.
   *
   * @return The backing tag of the domain.
   */
  public Optional<TagValue> backingTag() {
    if (backingTagName == null) {
      return Optional.empty();
    }
    return Optional.of(
        backingTagValue == null
            ? TagValue.noValue(backingTagName)
            : TagValue.of(backingTagName, backingTagValue));
  }
}
