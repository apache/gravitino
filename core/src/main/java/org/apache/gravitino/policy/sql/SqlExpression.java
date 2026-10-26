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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;

/** An immutable source node. CASE and SQL nulls are preserved for subsequent binding. */
final class SqlExpression {
  final String operator;
  final String value;
  final List<SqlExpression> children;
  @Nullable final SqlType type;

  SqlExpression(
      String operator, String value, @Nullable SqlType type, List<SqlExpression> children) {
    this.operator = operator;
    this.value = value;
    this.type = type;
    this.children = Collections.unmodifiableList(new ArrayList<>(children));
  }

  int depth() {
    int childDepth = children.stream().mapToInt(SqlExpression::depth).max().orElse(0);
    return children.isEmpty() && !operator.equals("CALL") ? 0 : childDepth + 1;
  }
}
