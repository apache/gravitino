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
package org.apache.gravitino.policy.rego;

/** Resource limits for {@code restricted-rego-v1} source programs. */
final class RestrictedRegoLimits {
  static final int MAX_SOURCE_DEPTH = 8;
  static final int MAX_AST_NODES = 256;
  static final int MAX_STRING_BYTES = 4 * 1024;
  static final int MAX_ARRAY_ELEMENTS = 256;
  static final int MAX_TOTAL_ARRAY_ELEMENTS = 256;
  static final int MAX_NUMERIC_LITERAL_BYTES = 256;
  static final int MAX_PARENTHESIS_NESTING = 256;

  private RestrictedRegoLimits() {}
}
