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

/**
 * A role's securable objects resolved into concrete jcasbin {@code p} rows, together with the
 * objects that could not be resolved to a metadata id.
 *
 * <p>Separating resolution from application lets {@code JcasbinAuthorizer} keep every DB round-trip
 * outside the lock that serializes enforcer mutations, and lets it tell a fully loaded role apart
 * from a partially loaded one — only the former may be recorded as loaded.
 */
final class ResolvedRolePolicies {

  private final List<String[]> allowRows;
  private final List<String[]> denyRows;
  private final List<String> unresolvedObjects;

  ResolvedRolePolicies(
      List<String[]> allowRows, List<String[]> denyRows, List<String> unresolvedObjects) {
    this.allowRows = allowRows;
    this.denyRows = denyRows;
    this.unresolvedObjects = unresolvedObjects;
  }

  List<String[]> getAllowRows() {
    return allowRows;
  }

  List<String[]> getDenyRows() {
    return denyRows;
  }

  /** Descriptions of the securable objects whose metadata id could not be resolved. */
  List<String> getUnresolvedObjects() {
    return unresolvedObjects;
  }

  /** True when every securable object of the role was resolved and turned into policy rows. */
  boolean isComplete() {
    return unresolvedObjects.isEmpty();
  }
}
