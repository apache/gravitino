/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// Referred from Apache Spark's connector/catalog implementation
// sql/catalyst/src/main/java/org/apache/spark/sql/connector/expressions/Literal.java

package com.datastrato.gravitino.rel.expressions;

import io.substrait.type.Type;
import lombok.EqualsAndHashCode;

/**
 * Represents a constant literal value in the public expression API.
 *
 * @param <T> the JVM type of value held by the literal
 */
public interface Literal<T> extends Expression {
  /** Returns the literal value. */
  T value();

  /** Returns the data type of the literal. */
  Type dataType();

  @Override
  default Expression[] children() {
    return EMPTY_EXPRESSION;
  }

  static <T> LiteralValue<T> of(T value, Type dataType) {
    return new LiteralValue<>(value, dataType);
  }

  @EqualsAndHashCode
  final class LiteralValue<T> implements Literal<T> {
    private final T value;
    private final Type dataType;

    private LiteralValue(T value, Type dataType) {
      this.value = value;
      this.dataType = dataType;
    }

    @Override
    public T value() {
      return value;
    }

    @Override
    public Type dataType() {
      return dataType;
    }
  }
}
