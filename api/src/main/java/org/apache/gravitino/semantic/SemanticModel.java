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

import java.util.Collections;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.gravitino.Audit;
import org.apache.gravitino.Auditable;
import org.apache.gravitino.annotation.Evolving;

/**
 * A schema-scoped analytical Semantic Model managed by Gravitino. The entity composes its immutable
 * Ossie-compatible definition with Gravitino properties and audit information.
 */
@Evolving
public interface SemanticModel extends Auditable {

  /**
   * Returns the Semantic Model name.
   *
   * @return The Semantic Model name.
   */
  String name();

  /**
   * Returns the Semantic Model comment.
   *
   * @return The comment, or {@code null} if it is not set.
   */
  @Nullable
  String comment();

  /**
   * Returns the immutable Semantic Model definition.
   *
   * @return The Semantic Model definition.
   */
  SemanticModelDefinition definition();

  /**
   * Returns the Gravitino-specific properties of the Semantic Model. Implementations must return an
   * immutable map or a defensive copy.
   *
   * @return The Semantic Model properties, or an empty map if no properties are set.
   */
  default Map<String, String> properties() {
    return Collections.emptyMap();
  }

  /**
   * Returns the audit information for the Semantic Model.
   *
   * @return The audit information.
   */
  @Override
  Audit auditInfo();
}
