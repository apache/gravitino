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
package org.apache.gravitino.connector.capability;

import com.google.common.collect.ImmutableSet;
import java.util.Set;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.annotation.Evolving;

/**
 * The Catalog interface to provide the capabilities of the catalog. If the implemented catalog has
 * some special capabilities, it should override the default implementation of the capabilities.
 */
@Evolving
public interface Capability {

  Capability DEFAULT = new DefaultCapability();

  /** The scope of the capability. */
  enum Scope {
    SCHEMA,
    TABLE,
    COLUMN,
    FILESET,
    TOPIC,
    PARTITION
  }

  /**
   * Check if the catalog supports not null constraint on column.
   *
   * @return The check result of the not null constraint.
   */
  default CapabilityResult columnNotNull() {
    return DEFAULT.columnNotNull();
  }

  /**
   * Check if the catalog supports default value on column.
   *
   * @return The check result of the default value.
   */
  default CapabilityResult columnDefaultValue() {
    return DEFAULT.columnDefaultValue();
  }

  /**
   * Check if the name is case-sensitive in the scope.
   *
   * @param scope The scope of the capability.
   * @return The capability of the case-sensitive on name.
   */
  default CapabilityResult caseSensitiveOnName(Scope scope) {
    return DEFAULT.caseSensitiveOnName(scope);
  }

  /**
   * Check if the name is illegal in the scope, such as special characters, reserved words, etc.
   *
   * @param scope The scope of the capability.
   * @param name The name to be checked.
   * @return The capability of the specification on name.
   */
  default CapabilityResult specificationOnName(Scope scope, String name) {
    return DEFAULT.specificationOnName(scope, name);
  }

  /**
   * Check if the entity is fully managed by Gravitino in the scope.
   *
   * @param scope The scope of the capability.
   * @return The capability of the managed storage.
   */
  default CapabilityResult managedStorage(Scope scope) {
    return DEFAULT.managedStorage(scope);
  }

  /** The default implementation of the capability. */
  class DefaultCapability implements Capability {

    private static final Set<String> RESERVED_WORDS =
        ImmutableSet.of(MetadataObjects.METADATA_OBJECT_RESERVED_NAME);

    /**
     * Regular expression explanation:
     *
     * <p>^\w - Starts with a letter, digit, or underscore
     *
     * <p>[\w/=-]{0,63} - Followed by 0 to 63 characters (making the total length at most 64) of
     * letters (both cases), digits, underscores, slashes, hyphens, or equals signs
     *
     * <p>$ - End of the string
     */
    private static final String DEFAULT_NAME_PATTERN = "^\\w[\\w/=-]{0,63}$";

    @Override
    public CapabilityResult columnNotNull() {
      return CapabilityResult.SUPPORTED;
    }

    @Override
    public CapabilityResult columnDefaultValue() {
      return CapabilityResult.SUPPORTED;
    }

    @Override
    public CapabilityResult caseSensitiveOnName(Scope scope) {
      return CapabilityResult.SUPPORTED;
    }

    @Override
    public CapabilityResult specificationOnName(Scope scope, String name) {
      if (RESERVED_WORDS.contains(name.toLowerCase())) {
        return CapabilityResult.unsupported(
            String.format("The %s name '%s' is reserved.", scope, name));
      }

      if (!name.matches(DEFAULT_NAME_PATTERN)) {
        return CapabilityResult.unsupported(
            String.format("The %s name '%s' is illegal.", scope, name));
      }
      return CapabilityResult.SUPPORTED;
    }

    @Override
    public CapabilityResult managedStorage(Scope scope) {
      return CapabilityResult.unsupported(
          String.format("The %s entity is not fully managed by Gravitino.", scope));
    }
  }
}
