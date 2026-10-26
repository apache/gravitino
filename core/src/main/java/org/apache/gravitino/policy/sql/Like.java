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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/** A case-sensitive fixed-prefix SQL LIKE predicate. */
final class Like extends SqlExpression {
  Like(SqlExpression operand, Literal pattern) {
    super(Arrays.asList(operand, pattern));
  }

  SqlExpression operand() {
    return children().get(0);
  }

  Literal pattern() {
    return (Literal) children().get(1);
  }

  /** {@inheritDoc} */
  @Override
  public boolean equals(Object other) {
    return other instanceof Like && sameContents((Like) other);
  }

  @Override
  List<?> attributes() {
    return Collections.emptyList();
  }
}
