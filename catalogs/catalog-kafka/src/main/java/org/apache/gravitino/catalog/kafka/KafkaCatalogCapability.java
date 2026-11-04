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
package org.apache.gravitino.catalog.kafka;

import java.util.Objects;
import org.apache.gravitino.connector.capability.Capability;
import org.apache.gravitino.connector.capability.CapabilityResult;

public class KafkaCatalogCapability implements Capability {
  private static final String TOPIC_NAME_PATTERN = "[a-zA-Z0-9_-]{1,128}";

  @Override
  public CapabilityResult managedStorage(Scope scope) {
    if (Objects.requireNonNull(scope) == Scope.SCHEMA) {
      return CapabilityResult.SUPPORTED;
    }
    return CapabilityResult.unsupported(
        String.format("Kafka catalog does not support managed storage for %s.", scope));
  }

  /**
   * Checks topic names against Kafka's character restrictions and Gravitino's compatibility limits.
   * Other scopes use Gravitino's default name rules.
   *
   * <p>Topic names cannot contain dots because Gravitino authorization does not support them, and
   * cannot exceed 128 characters because of the metadata store's name length limit.
   *
   * @param scope The scope of the capability.
   * @param name The name to check.
   * @return Whether the name is supported in the given scope.
   */
  @Override
  public CapabilityResult specificationOnName(Scope scope, String name) {
    if (scope != Scope.TOPIC) {
      return Capability.super.specificationOnName(scope, name);
    }

    if (!name.matches(TOPIC_NAME_PATTERN)) {
      return CapabilityResult.unsupported(
          "Kafka topic names in Gravitino must be 1 to 128 characters long and contain only ASCII"
              + " alphanumerics, '_' and '-'.");
    }
    return CapabilityResult.SUPPORTED;
  }
}
