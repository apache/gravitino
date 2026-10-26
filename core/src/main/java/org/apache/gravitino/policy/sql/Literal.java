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
import javax.annotation.Nullable;

/** A literal retaining its exact decoded text, including negative zero. */
final class Literal extends SqlExpression {
  private final LiteralType literalType;
  private final String value;
  @Nullable private final SqlType type;

  Literal(LiteralType literalType, String value, @Nullable SqlType type) {
    super(Collections.emptyList());
    this.literalType = literalType;
    this.value = value;
    this.type = type;
  }

  LiteralType literalType() {
    return literalType;
  }

  String value() {
    return value;
  }

  @Nullable
  @Override
  SqlType type() {
    return type;
  }

  @Override
  int depth() {
    return 0;
  }

  /** {@inheritDoc} */
  @Override
  public boolean equals(Object other) {
    return other instanceof Literal && sameContents((Literal) other);
  }

  @Override
  List<?> attributes() {
    return Arrays.asList(literalType, value, type);
  }
}
