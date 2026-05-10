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

import org.apache.gravitino.Entity;

/** Extension point for hard deleting legacy relational metadata owned by plugins. */
public interface LegacyDataCleaner {

  /**
   * Returns the entity type handled by this cleaner.
   *
   * @return the handled entity type
   */
  Entity.EntityType entityType();

  /**
   * Hard deletes legacy metadata records.
   *
   * @param legacyTimeline delete records older than this timeline
   * @param limit maximum number of records to delete per invocation
   * @return the number of deleted records
   */
  int hardDeleteLegacyData(long legacyTimeline, int limit);
}
