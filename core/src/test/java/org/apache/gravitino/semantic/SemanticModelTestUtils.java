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
package org.apache.gravitino.semantic;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import org.apache.gravitino.Entity;

/** Resolved source fixtures for tests that exercise the internal model write pipeline. */
public final class SemanticModelTestUtils {
  private SemanticModelTestUtils() {}

  /**
   * Builds deterministic associations without accessing a catalog; source resolution is tested in
   * REST.
   *
   * @param definition A test definition, including malformed definitions used by validation tests.
   * @return A replacement instruction with one association per valid dataset name.
   */
  public static SemanticModelSourceUpdate sources(SemanticModelDefinition definition) {
    if (definition == null || definition.datasets() == null) {
      return SemanticModelSourceUpdate.replace(List.of());
    }
    return SemanticModelSourceUpdate.replace(
        Arrays.stream(definition.datasets())
            .filter(Objects::nonNull)
            .map(Dataset::name)
            .filter(name -> name != null && !name.isEmpty())
            .distinct()
            .map(name -> new SemanticModelSource(name, Entity.EntityType.TABLE, 1L))
            .toList());
  }
}
