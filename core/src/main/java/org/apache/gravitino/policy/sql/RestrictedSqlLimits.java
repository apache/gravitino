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
package org.apache.gravitino.policy.sql;

/** Source resource limits shared by ANTLR token validation and AST construction. */
final class RestrictedSqlLimits {
  static final int MAX_DEPTH = 16;
  static final int MAX_NODES = 1024;
  static final int MAX_PARAMETERS = 256;
  static final int MAX_STRING_BYTES = 4096;
  static final int MAX_NUMBER_BYTES = 256;
  static final int MAX_IN_ELEMENTS = 1000;
  static final int MAX_PARENTHESES = 64;

  private RestrictedSqlLimits() {}
}
