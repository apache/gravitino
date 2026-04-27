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
package org.apache.gravitino.server.authorization.annotations;

/**
 * Defines the action to take when evaluating an authorization expression in {@link
 * AuthorizationExpression}.
 */
public enum ExpressionAction {
  /**
   * Evaluate the authorization expression and use the result to determine whether access is
   * granted. This is the default action.
   */
  EVALUATE,

  /**
   * Only check whether the metadata object exists, without performing a full authorization check.
   * Used to allow users who hold creation or view privileges (e.g. {@code CREATE_TABLE}, {@code
   * SELECT_VIEW}) to verify object existence so that operations like {@code tableExists()} work
   * correctly, even if they do not have read access to the full object.
   */
  CHECK_METADATA_OBJECT_EXISTS
}
