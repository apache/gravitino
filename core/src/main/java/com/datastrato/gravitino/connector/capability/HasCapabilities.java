/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.connector.capability;

public interface HasCapabilities {
  default Capability columnNotNull() {
    return Capability.SUPPORTED;
  }

  default Capability columnDefaultValue() {
    return Capability.SUPPORTED;
  }

  default Capability caseSensitiveOnName(Capability.Scope scope) {
    return Capability.SUPPORTED;
  }

  /**
   * Check if the catalog supports the specification on name, such as special characters, reserved
   * words, etc.
   *
   * @param scope The scope of the capability.
   * @param name The name to be checked.
   * @return The capability of the specification on name.
   */
  default Capability specificationOnName(Capability.Scope scope, String name) {
    return Capability.SUPPORTED;
  }

  default Capability managedStorage(Capability.Scope scope) {
    return Capability.unsupported(
        String.format("The %s entity is not fully managed by Gravitino.", scope));
  }
}
