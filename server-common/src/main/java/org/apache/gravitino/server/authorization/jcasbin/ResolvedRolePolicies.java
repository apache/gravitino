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
package org.apache.gravitino.server.authorization.jcasbin;

import java.util.List;
import java.util.Map;
import java.util.Set;

/** A role resolved outside the publication lock, including unresolved deny summaries. */
final class ResolvedRolePolicies {
  private final Map<PolicyKey, Effect> index;
  private final List<String> unresolvedObjects;
  private final Set<String> unresolvedDenies;

  ResolvedRolePolicies(
      Map<PolicyKey, Effect> index, List<String> unresolvedObjects, Set<String> unresolvedDenies) {
    this.index = index;
    this.unresolvedObjects = unresolvedObjects;
    this.unresolvedDenies = unresolvedDenies;
  }

  Map<PolicyKey, Effect> getIndex() {
    return index;
  }

  Set<String> getUnresolvedDenies() {
    return unresolvedDenies;
  }

  /** Descriptions of objects whose metadata IDs could not be resolved. */
  List<String> getUnresolvedObjects() {
    return unresolvedObjects;
  }

  boolean isComplete() {
    return unresolvedObjects.isEmpty();
  }
}
