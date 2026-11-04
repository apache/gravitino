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

import com.google.common.base.Preconditions;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.apache.gravitino.Entity;

/** A dataset's source resolved during caller-authorized definition validation. */
@Getter
@EqualsAndHashCode
public final class SemanticModelSource {
  private final String datasetName;
  private final Entity.EntityType sourceType;
  private final long sourceId;

  /**
   * Creates a resolved source association.
   *
   * @param datasetName The exact dataset name in the definition.
   * @param sourceType TABLE or VIEW.
   * @param sourceId The registered Gravitino entity ID.
   */
  public SemanticModelSource(String datasetName, Entity.EntityType sourceType, long sourceId) {
    Preconditions.checkArgument(
        datasetName != null && !datasetName.isEmpty(), "Dataset name is required");
    Preconditions.checkArgument(
        sourceType == Entity.EntityType.TABLE || sourceType == Entity.EntityType.VIEW,
        "Source must be a table or View");
    this.datasetName = datasetName;
    this.sourceType = sourceType;
    this.sourceId = sourceId;
  }
}
