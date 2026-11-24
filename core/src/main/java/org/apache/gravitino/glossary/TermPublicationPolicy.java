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

/** Server-owned publication policy. Decisions apply to an exact immutable candidate of one term. */
@FunctionalInterface
public interface TermPublicationPolicy {
  /** Returns whether new definitions and edits in the metalake require a server decision. */
  boolean requiresApproval(String metalake);

  /**
   * Authorizes the current authenticated actor to publish or reject this exact submitted version.
   */
  default boolean canDecide(String metalake, long termId, long candidateVersionId) {
    return false;
  }
}
