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
  private static final String TOPIC_NAME_PATTERN = "[a-zA-Z0-9._-]+";

  @Override
  public CapabilityResult managedStorage(Scope scope) {
    if (Objects.requireNonNull(scope) == Scope.SCHEMA) {
      return CapabilityResult.SUPPORTED;
    }
    return CapabilityResult.unsupported(
        String.format("Kafka catalog does not support managed storage for %s.", scope));
  }

  /**
   * Checks names against Gravitino's default rules and Kafka's topic character restrictions.
   *
   * @param scope The scope of the capability.
   * @param name The name to check.
   * @return Whether the name is supported in the given scope.
   */
  @Override
  public CapabilityResult specificationOnName(Scope scope, String name) {
    CapabilityResult result = Capability.super.specificationOnName(scope, name);
    if (!result.supported() || scope != Scope.TOPIC) {
      return result;
    }

    if (!name.matches(TOPIC_NAME_PATTERN)) {
      return CapabilityResult.unsupported(
          "Kafka topic names must contain only ASCII alphanumerics, '.', '_' and '-'.");
    }
    return result;
  }
}
