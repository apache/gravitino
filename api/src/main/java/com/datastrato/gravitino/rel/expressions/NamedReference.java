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

import com.datastrato.gravitino.annotation.Evolving;
import java.util.Arrays;

/** Represents a field or column reference in the public logical expression API. */
@Evolving
public interface NamedReference extends Expression {

  /**
   * Returns a {@link FieldReference} for the given field name(s). The array of field name(s) is
   * used to reference nested fields. For example, if we have a struct column named "student" with a
   * data type of StructType{"name": StringType, "age": IntegerType}, we can reference the field
   * "name" by calling {@code field("student", "name")}.
   *
   * @param fieldName the field name(s)
   * @return a {@link FieldReference} for the given field name(s)
   */
  static FieldReference field(String[] fieldName) {
    return new FieldReference(fieldName);
  }

  /**
   * Returns a {@link FieldReference} for the given column name.
   *
   * @param columnName the column name
   * @return a {@link FieldReference} for the given column name.
   */
  static FieldReference field(String columnName) {
    return field(new String[] {columnName});
  }

  /**
   * Returns the referenced field name as an array of String parts.
   *
   * <p>Each string in the returned array represents a field name.
   *
   * @return the referenced field name as an array of String parts.
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

  /** A {@link NamedReference} that references a field or column. */
  final class FieldReference implements NamedReference {
    private final String[] fieldName;

    private FieldReference(String[] fieldName) {
      this.fieldName = fieldName;
    }

    @Override
    public String[] fieldName() {
      return fieldName;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      FieldReference that = (FieldReference) o;
      return Arrays.equals(fieldName, that.fieldName);
    }

    @Override
    public int hashCode() {
      return Arrays.hashCode(fieldName);
    }

    /** @return The string representation of the field reference. */
    @Override
    public String toString() {
      return String.join(".", fieldName);
    }
  }
}
