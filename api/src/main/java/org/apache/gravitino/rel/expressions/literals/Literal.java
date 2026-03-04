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
package org.apache.gravitino.rel.expressions.literals;

import org.apache.gravitino.annotation.Evolving;
import org.apache.gravitino.rel.expressions.Expression;
import org.apache.gravitino.rel.types.Type;

/**
 * Represents a constant literal value in the public expression API.
 *
 * @param <T> the JVM type of value held by the literal
 */
@Evolving
public interface Literal<T> extends Expression {

  /**
   * @return The literal value.
   */
  T value();

  /**
   * @return The data type of the literal.
   */
  Type dataType();

  @Override
  default Expression[] children() {
    return EMPTY_EXPRESSION;
  }
}
