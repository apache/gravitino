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
package org.apache.gravitino.storage.relational;

import org.apache.gravitino.MetadataObject;

/**
 * An optional capability for {@link RelationalBackend} implementations that can clean up relation
 * rows referencing metadata objects which no longer exist. Backends without this capability are
 * simply skipped by the garbage collector.
 */
public interface SupportsOrphanedRelationCleanup {

  /**
   * Soft-deletes relation rows that reference no live metadata object of the given type.
   *
   * @param metadataObjectType metadata object type to collect
   * @param limit maximum number of orphaned object IDs processed per relation table
   * @return number of relation rows soft-deleted
   */
  int softDeleteOrphanedRelations(MetadataObject.Type metadataObjectType, int limit);
}
