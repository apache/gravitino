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

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.gravitino.Auditable;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.annotation.Evolving;
import org.apache.gravitino.exceptions.IllegalPolicyException;

/**
 * The interface of the policy. The policy is a set of rules that can be associated with a metadata
 * object. The policy can be used for data governance and so on.
 */
@Evolving
public interface Policy extends Auditable {

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
  String type();

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
  Content content();

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

  /**
   * Validate the policy. This method should be called when the policy is created or updated. It
   * will check if the policy is valid or not. If the policy is not valid, it will throw an
   * IllegalPolicyException.
   *
   * @throws IllegalPolicyException if the policy is not valid.
   */
  void validate() throws IllegalPolicyException;

  /** @return The associated objects of the policy. */
  default AssociatedObjects associatedObjects() {
    throw new UnsupportedOperationException("The associatedObjects method is not supported.");
  }

  /** The interface of the content of the policy. */
  interface Content {

    /** @return The additional properties of the policy. */
    Map<String, String> properties();
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
