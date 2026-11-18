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

import org.apache.gravitino.storage.relational.po.cache.OperateType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Diagnostic logging for entity change rows appended to the current transaction. */
public final class EntityChangeLogDiagnostics {
  private static final Logger LOG = LoggerFactory.getLogger(EntityChangeLogDiagnostics.class);

  private EntityChangeLogDiagnostics() {}

  /**
   * Logs a successful append without implying that the enclosing transaction committed.
   *
   * @param metalake the metalake name
   * @param entityType the entity type
   * @param operateType the change operation
   * @param fullName the encoded identifier stored in the row
   */
  public static void logAppended(
      String metalake, String entityType, OperateType operateType, String fullName) {
    LOG.debug(
        "entityChangeLog appendedToTransaction metalake={} entityType={} operateType={} fullName={}",
        metalake,
        entityType,
        operateType,
        fullName);
  }
}
