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
// sql/catalyst/src/main/java/org/apache/spark/sql/connector/expressions/NamedReference.java

package com.datastrato.gravitino.rel.expressions;

import lombok.EqualsAndHashCode;

/** Represents a field or column reference in the public logical expression API. */
public interface NamedReference extends Expression {

  static FieldReference field(String... fieldName) {
    return new FieldReference(fieldName);
  }

  /**
   * Returns the referenced field name as an array of String parts.
   *
   * <p>Each string in the returned array represents a field name.
   */
  String[] fieldName();

  @Override
  default Expression[] children() {
    return EMPTY_EXPRESSION;
  }

  @Override
  default NamedReference[] references() {
    return new NamedReference[] {this};
  }

  @EqualsAndHashCode
  final class FieldReference implements NamedReference {
    private final String[] fieldName;

    private FieldReference(String[] fieldName) {
      this.fieldName = fieldName;
    }

    @Override
    public String[] fieldName() {
      return fieldName;
    }
  }
}
