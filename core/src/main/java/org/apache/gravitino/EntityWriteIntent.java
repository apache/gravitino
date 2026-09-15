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
package org.apache.gravitino;

/** The purpose and conflict semantics of an entity write. */
public enum EntityWriteIntent {
  /** Inserts a new entity; an existing name or ID is an error. */
  CREATE,
  /** Inserts if the name is absent, otherwise returns the existing entity unchanged. */
  CREATE_IF_ABSENT,
  /**
   * Registers an external entity without rebinding an existing name or ID. An existing entity at
   * the same name with the same ID is returned unchanged. Changes require RECONCILE instead.
   */
  IMPORT,
  /** Replaces an observed entity only while its name, ID, and storage version still match. */
  RECONCILE
}
