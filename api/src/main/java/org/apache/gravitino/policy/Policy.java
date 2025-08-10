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
import com.google.common.collect.ImmutableSet;
import java.util.Optional;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Auditable;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.annotation.Evolving;

/**
 * The interface of the policy. The policy is a set of rules that can be associated with a metadata
 * object. The policy can be used for data governance and so on.
 */
@Evolving
public interface Policy extends Auditable {

  /** The set of metadata object types that the policy can be applied to. */
  Set<MetadataObject.Type> SUPPORTS_ALL_OBJECT_TYPES =
      ImmutableSet.of(
          MetadataObject.Type.CATALOG,
          MetadataObject.Type.SCHEMA,
          MetadataObject.Type.FILESET,
          MetadataObject.Type.TABLE,
          MetadataObject.Type.TOPIC,
          MetadataObject.Type.MODEL);

  /**
   * The prefix for built-in policy types. All built-in policy types should start with this prefix.
   */
  String BUILT_IN_TYPE_PREFIX = "system_";

  /** The built-in policy types. Predefined policy types that are provided by the system. */
  enum BuiltInType {
    // todo: add built-in policies, such as:
    //    DATA_COMPACTION(BUILT_IN_TYPE_PREFIX + "data_compaction", true, true,
    // SUPPORTS_ALL_OBJECT_TYPES
    //      PolicyContent.DataCompactionContent.class)

    /**
     * Custom policy type. "custom" is a dummy type for custom policies, all non-built-in types are
     * custom types.
     */
    CUSTOM("custom", null, null, null, PolicyContents.CustomContent.class);

    private final String policyType;
    private final Boolean exclusive;
    private final Boolean inheritable;
    private final ImmutableSet<MetadataObject.Type> supportedObjectTypes;
    private final Class<? extends PolicyContent> contentClass;

    BuiltInType(
        String policyType,
        Boolean exclusive,
        Boolean inheritable,
        Set<MetadataObject.Type> supportedObjectTypes,
        Class<? extends PolicyContent> contentClass) {
      this.policyType = policyType;
      this.exclusive = exclusive;
      this.inheritable = inheritable;
      this.supportedObjectTypes =
          supportedObjectTypes == null
              ? ImmutableSet.of()
              : ImmutableSet.copyOf(supportedObjectTypes);
      this.contentClass = contentClass;
    }

    /**
     * Get the built-in policy type from the policy type string.
     *
     * @param policyType the policy type string
     * @return the built-in policy type if it matches, otherwise returns CUSTOM type
     */
    public static BuiltInType fromPolicyType(String policyType) {
      Preconditions.checkArgument(StringUtils.isNotBlank(policyType), "policyType cannot be blank");
      for (BuiltInType type : BuiltInType.values()) {
        if (type.policyType.equalsIgnoreCase(policyType)) {
          return type;
        }
      }

      if (policyType.startsWith(BUILT_IN_TYPE_PREFIX)) {
        throw new IllegalArgumentException(
            String.format("Unknown built-in policy type: %s", policyType));
      }

      // If the policy type is not a built-in type, it is a custom type.
      return CUSTOM;
    }

    /**
     * Get the policy type string.
     *
     * @return the policy type string
     */
    public String policyType() {
      return policyType;
    }

    /**
     * Check if the policy is exclusive.
     *
     * @return true if the policy is exclusive, false otherwise
     */
    public Boolean exclusive() {
      return exclusive;
    }

    /**
     * Check if the policy is inheritable.
     *
     * @return true if the policy is inheritable, false otherwise
     */
    public Boolean inheritable() {
      return inheritable;
    }

    /**
     * Get the set of metadata object types that the policy can be associated with.
     *
     * @return the set of metadata object types that the policy can be associated with
     */
    public Set<MetadataObject.Type> supportedObjectTypes() {
      return supportedObjectTypes;
    }

    /**
     * Get the content class of the policy.
     *
     * @return the content class of the policy
     */
    public Class<? extends PolicyContent> contentClass() {
      return contentClass;
    }
  }

  /**
   * Get the name of the policy.
   *
   * @return The name of the policy.
   */
  String name();

  /**
   * Get the type of the policy.
   *
   * @return The type of the policy.
   */
  String policyType();

  /**
   * Get the comment of the policy.
   *
   * @return The comment of the policy.
   */
  String comment();

  /**
   * Whether the policy is enabled or not.
   *
   * @return True if the policy is enabled, false otherwise.
   */
  boolean enabled();

  /**
   * Whether the policy is exclusive or not. If the policy is exclusive, only one of the same type
   * policy can be associated with the same object. If the policy is not exclusive, multiple
   * policies of the same type can be associated with the same object.
   *
   * @return True if the policy is exclusive, false otherwise.
   */
  boolean exclusive();

  /**
   * Whether the policy is inheritable or not. If the policy is inheritable, it can be inherited by
   * child objects. If the policy is not inheritable, it can only be associated with the metadata
   * object itself.
   *
   * @return True if the policy is inheritable, false otherwise.
   */
  boolean inheritable();

  /**
   * Get the set of the metadata object types that the policy can be associated with.
   *
   * @return The set of the metadata object types that the policy can be associated with.
   */
  Set<MetadataObject.Type> supportedObjectTypes();

  /**
   * Get the content of the policy.
   *
   * @return The content of the policy.
   */
  PolicyContent content();

  /**
   * Check if the policy is inherited from a parent object or not.
   *
   * <p>Note: The return value is optional, Only when the policy is associated with a metadata
   * object, and called from the metadata object, the return value will be present. Otherwise, it
   * will be empty.
   *
   * @return True if the policy is inherited, false if it is owned by the object itself. Empty if
   *     the policy is not associated with any object.
   */
  Optional<Boolean> inherited();

  /** @return The associated objects of the policy. */
  default AssociatedObjects associatedObjects() {
    throw new UnsupportedOperationException("The associatedObjects method is not supported.");
  }

  /** The interface of the associated objects of the policy. */
  interface AssociatedObjects {

    /** @return The number of objects that are associated with this policy */
    default int count() {
      MetadataObject[] objects = objects();
      return objects == null ? 0 : objects.length;
    }

    /** @return The list of objects that are associated with this policy. */
    MetadataObject[] objects();
  }
}
