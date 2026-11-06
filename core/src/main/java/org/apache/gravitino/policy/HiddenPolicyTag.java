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
import java.util.Collections;
import java.util.Locale;
import javax.annotation.Nullable;
import org.apache.gravitino.Audit;
import org.apache.gravitino.exceptions.NoSuchTagException;
import org.apache.gravitino.meta.PolicyEntity;
import org.apache.gravitino.meta.TagEntity;
import org.apache.gravitino.utils.NamespaceUtil;

/** Naming and visibility checks for server-managed policy tags. */
public final class HiddenPolicyTag {

  private static final String PREFIX = "gravitino_hidden_";
  private static final String POLICY_ID_PROPERTY = "gravitino.internal.policy-id";

  private HiddenPolicyTag() {}

  /**
   * Builds the server-managed tag for a policy using Gravitino's tag entity model.
   *
   * @param policy The policy that owns the hidden tag.
   * @param tagId The generated tag entity ID.
   * @return An unassigned, value-less tag with the policy's namespace and audit information.
   */
  public static TagEntity create(PolicyEntity policy, long tagId) {
    return create(policy.id(), tagId, policy.namespace().level(0), policy.auditInfo());
  }

  /**
   * Builds a hidden tag from persisted policy identity and audit metadata.
   *
   * @param policyId The owning policy ID.
   * @param tagId The generated tag ID.
   * @param metalake The policy's metalake name.
   * @param audit The creation audit information.
   * @return The hidden tag entity.
   */
  public static TagEntity create(long policyId, long tagId, String metalake, Audit audit) {
    return TagEntity.builder()
        .withId(tagId)
        .withName(name(policyId))
        .withNamespace(NamespaceUtil.ofTag(metalake))
        .withComment("Server-managed policy binding")
        .withProperties(Collections.singletonMap(POLICY_ID_PROPERTY, Long.toString(policyId)))
        .withAllowedValues(new String[0])
        .withAuditInfo(audit)
        .build();
  }

  /**
   * Returns a stable tag name that survives policy renames.
   *
   * @param policyId The policy entity ID.
   * @return The reserved tag name.
   */
  public static String name(long policyId) {
    return PREFIX + policyId;
  }

  /**
   * Returns whether a tag name belongs to the reserved internal namespace.
   *
   * @param name The tag name.
   * @return Whether the name is reserved.
   */
  public static boolean isReservedName(@Nullable String name) {
    return name != null && name.toLowerCase(Locale.ROOT).startsWith(PREFIX);
  }

  /**
   * Parses the policy identity from a canonical hidden tag name.
   *
   * @param tagName The internal tag name.
   * @return The policy entity ID.
   */
  public static long policyId(String tagName) {
    Preconditions.checkArgument(isReservedName(tagName), "Not a reserved policy tag name");
    long id = Long.parseLong(tagName.substring(PREFIX.length()));
    Preconditions.checkArgument(name(id).equals(tagName), "Invalid hidden policy tag name");
    return id;
  }

  /**
   * Returns the persisted ownership marker for a hidden tag.
   *
   * @param policyId The policy entity ID.
   * @return The internal properties as JSON.
   */
  public static String properties(long policyId) {
    return "{\"" + POLICY_ID_PROPERTY + "\":\"" + policyId + "\"}";
  }

  /**
   * Rejects an ordinary operation on a tag in the internal name namespace.
   *
   * @param tag The resolved tag entity.
   * @throws NoSuchTagException If the tag is hidden.
   */
  public static void checkVisible(TagEntity tag) {
    if (isReservedName(tag.name())) {
      throw new NoSuchTagException(
          "Tag with name %s under metalake %s does not exist",
          tag.name(), tag.namespace().level(0));
    }
  }

  /**
   * Rejects the reserved prefix for a new tag name or a rename target.
   *
   * @param name The proposed tag name.
   */
  public static void checkNameNotReserved(String name) {
    Preconditions.checkArgument(
        !isReservedName(name), "Tag name is reserved for internal policy bindings");
  }
}
