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
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import org.apache.gravitino.Audit;
import org.apache.gravitino.Auditable;
import org.apache.gravitino.Entity;
import org.apache.gravitino.Field;
import org.apache.gravitino.HasIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.tag.Tag;
import org.apache.gravitino.tag.TagAssignment;
import org.apache.gravitino.tag.TagValueConstraint;

/** A tag entity in a metadata-object relation context, with assignment values from the relation. */
public final class AssignedTagEntity implements Tag, Entity, Auditable, HasIdentifier {

  private final TagEntity tagEntity;
  private final TagAssignment assignment;

  private AssignedTagEntity(TagEntity tagEntity, TagAssignment assignment) {
    this.tagEntity = tagEntity;
    this.assignment = assignment;
  }

  /**
   * Creates an assigned tag entity.
   *
   * @param tagEntity The tag definition entity.
   * @param assignmentValues The assignment values from the relation row. Empty means no value.
   * @return The assigned tag entity.
   */
  public static AssignedTagEntity of(TagEntity tagEntity, String[] assignmentValues) {
    Preconditions.checkArgument(tagEntity != null, "tagEntity must not be null");
    String[] values = assignmentValues == null ? new String[0] : assignmentValues.clone();
    TagAssignment assignment =
        values.length == 0 ? TagAssignment.noValue() : TagAssignment.ofValues(values);
    return new AssignedTagEntity(tagEntity, assignment);
  }

  /**
   * Returns the tag definition fields only. Assignment values are relation-context state and are
   * exposed through {@link #assignment()}.
   *
   * @return The tag definition fields.
   */
  @Override
  public Map<Field, Object> fields() {
    return tagEntity.fields();
  }

  @Override
  public EntityType type() {
    return tagEntity.type();
  }

  @Override
  public Long id() {
    return tagEntity.id();
  }

  @Override
  public String name() {
    return tagEntity.name();
  }

  @Override
  public Namespace namespace() {
    return tagEntity.namespace();
  }

  @Override
  public String comment() {
    return tagEntity.comment();
  }

  @Override
  public Map<String, String> properties() {
    return tagEntity.properties();
  }

  @Override
  public TagValueConstraint valueConstraint() {
    return tagEntity.valueConstraint();
  }

  @Override
  public Optional<TagAssignment> assignment() {
    return Optional.of(assignment);
  }

  @Override
  public Optional<Boolean> inherited() {
    return tagEntity.inherited();
  }

  @Override
  public Audit auditInfo() {
    return tagEntity.auditInfo();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof AssignedTagEntity)) {
      return false;
    }

    AssignedTagEntity that = (AssignedTagEntity) obj;
    return Objects.equals(tagEntity, that.tagEntity) && Objects.equals(assignment, that.assignment);
  }

  @Override
  public int hashCode() {
    return Objects.hash(tagEntity, assignment);
  }
}
