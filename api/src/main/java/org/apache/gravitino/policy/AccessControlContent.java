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
package org.apache.gravitino.policy;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.authorization.Privilege;

/**
 * The content of a {@link Policy.BuiltInType#ACCESS_CONTROL} policy.
 *
 * <p>The policy is bound to a tag, and every object carrying that tag confers {@link #privileges()}
 * on any caller whose active roles include one of {@link #applicableRoles()}. The roles are a
 * condition on the rule, not the principals it is granted to.
 */
public final class AccessControlContent implements PolicyContent {

  /** Rule key for the privileges the rule confers. */
  public static final String PRIVILEGES_KEY = "privileges";

  /** Rule key for the roles that satisfy the rule's condition. */
  public static final String APPLICABLE_ROLES_KEY = "applicableRoles";

  /**
   * The privileges a tag is allowed to confer.
   *
   * <p>An allowlist rather than a denylist, so the boundary fails closed: a privilege added to
   * {@link Privilege.Name} later confers nothing through a tag until it is added here deliberately.
   * It holds only privileges that act on the tagged object itself. Privileges that hand out access,
   * create entities, change what other principals execute, or open a path to untagged objects are
   * excluded.
   */
  public static final Set<Privilege.Name> PERMITTED_PRIVILEGES =
      ImmutableSet.of(
          Privilege.Name.SELECT_TABLE,
          Privilege.Name.MODIFY_TABLE,
          Privilege.Name.PROBE_TABLE_LIKE,
          Privilege.Name.SELECT_VIEW,
          Privilege.Name.READ_FILESET,
          Privilege.Name.WRITE_FILESET,
          Privilege.Name.CONSUME_TOPIC,
          Privilege.Name.PRODUCE_TOPIC,
          Privilege.Name.USE_MODEL,
          Privilege.Name.EXECUTE_FUNCTION);

  // The object types a tag can be applied to, minus COLUMN, which no permitted privilege can be
  // scoped to. METALAKE is absent because a tag cannot be applied to a metalake.
  private static final Set<MetadataObject.Type> SUPPORTED_OBJECT_TYPES =
      ImmutableSet.of(
          MetadataObject.Type.CATALOG,
          MetadataObject.Type.SCHEMA,
          MetadataObject.Type.TABLE,
          MetadataObject.Type.VIEW,
          MetadataObject.Type.FILESET,
          MetadataObject.Type.TOPIC,
          MetadataObject.Type.MODEL,
          MetadataObject.Type.FUNCTION);

  private final List<Privilege.Name> privileges;
  private final List<String> applicableRoles;

  /** Default constructor for Jackson deserialization only. */
  private AccessControlContent() {
    this(null, null);
  }

  AccessControlContent(List<Privilege.Name> privileges, List<String> applicableRoles) {
    // Copied without rejecting nulls so that validate() is the single place that reports a bad
    // entry, with the offending value named.
    this.privileges =
        privileges == null
            ? Collections.emptyList()
            : Collections.unmodifiableList(new ArrayList<>(privileges));
    this.applicableRoles =
        applicableRoles == null
            ? Collections.emptyList()
            : Collections.unmodifiableList(new ArrayList<>(applicableRoles));
  }

  /**
   * Returns the privileges this rule confers on the tagged object.
   *
   * @return the conferred privileges
   */
  public List<Privilege.Name> privileges() {
    return privileges;
  }

  /**
   * Returns the roles that satisfy this rule's condition.
   *
   * @return the applicable role names
   */
  public List<String> applicableRoles() {
    return applicableRoles;
  }

  @Override
  public Set<MetadataObject.Type> supportedObjectTypes() {
    return SUPPORTED_OBJECT_TYPES;
  }

  @Override
  public Map<String, String> properties() {
    return ImmutableMap.of();
  }

  @Override
  public Map<String, Object> rules() {
    Map<String, Object> rules = new LinkedHashMap<>();
    rules.put(
        PRIVILEGES_KEY,
        privileges.stream()
            .map(privilege -> privilege == null ? null : privilege.name())
            .collect(Collectors.toList()));
    rules.put(APPLICABLE_ROLES_KEY, applicableRoles);
    return Collections.unmodifiableMap(rules);
  }

  @Override
  public void validate() throws IllegalArgumentException {
    PolicyContent.super.validate();
    Preconditions.checkArgument(!privileges.isEmpty(), "privileges cannot be empty");
    privileges.forEach(
        privilege ->
            Preconditions.checkArgument(
                PERMITTED_PRIVILEGES.contains(privilege),
                "privilege %s cannot be conferred by a tag, permitted privileges are %s",
                privilege,
                PERMITTED_PRIVILEGES));

    Preconditions.checkArgument(!applicableRoles.isEmpty(), "applicableRoles cannot be empty");
    applicableRoles.forEach(
        role ->
            Preconditions.checkArgument(
                StringUtils.isNotBlank(role), "applicable role name cannot be blank"));
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof AccessControlContent)) {
      return false;
    }
    AccessControlContent that = (AccessControlContent) o;
    return Objects.equals(privileges, that.privileges)
        && Objects.equals(applicableRoles, that.applicableRoles);
  }

  @Override
  public int hashCode() {
    return Objects.hash(privileges, applicableRoles);
  }

  @Override
  public String toString() {
    return "AccessControlContent{"
        + "privileges="
        + privileges
        + ", applicableRoles="
        + applicableRoles
        + '}';
  }
}
