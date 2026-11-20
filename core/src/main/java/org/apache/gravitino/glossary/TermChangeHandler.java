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

package org.apache.gravitino.glossary;

import javax.annotation.Nullable;
import org.apache.gravitino.meta.GlossaryTermEntity;
import org.apache.gravitino.storage.relational.service.GlossaryTermRelService.RelationType;

/** Processes one effective glossary write while its transaction still holds all required locks. */
@FunctionalInterface
interface TermChangeHandler {
  /**
   * A null before-image denotes creation and a null after-image denotes deletion. Invoke persist
   * once to apply the change in the current transaction. Propagate a failure to its owner for
   * rollback.
   */
  void handle(
      @Nullable GlossaryTermEntity before, @Nullable GlossaryTermEntity after, Runnable persist);

  /** Supplies the lost relation and the survivor's resulting revision to cleanup audit handlers. */
  default void handleRelationLoss(
      GlossaryTermEntity before,
      GlossaryTermEntity after,
      GlossaryTermEntity removed,
      RelationType type,
      Runnable persist) {
    handle(before, after, persist);
  }
}
