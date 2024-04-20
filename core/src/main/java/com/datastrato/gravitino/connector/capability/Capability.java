/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.connector.capability;

import com.datastrato.gravitino.annotation.Evolving;

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
      return CapabilityResult.SUPPORTED;
    }

    @Override
    public CapabilityResult managedStorage(Scope scope) {
      return CapabilityResult.unsupported(
          String.format("The %s entity is not fully managed by Gravitino.", scope));
    }
  }
}
