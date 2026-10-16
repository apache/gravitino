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

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/** A non-empty homogeneous array of non-null source literals. */
final class LiteralArray implements CanonicalExpression {
  private final LiteralType elementType;
  private final List<Literal> values;

  LiteralArray(LiteralType elementType, List<Literal> values) {
    this.elementType = elementType;
    this.values = values == null ? null : Collections.unmodifiableList(new ArrayList<>(values));
  }

  LiteralType elementType() {
    return elementType;
  }

  List<Literal> values() {
    return values;
  }

  @Override
  public void validate() throws IllegalArgumentException {
    Preconditions.checkArgument(
        elementType != null && elementType != LiteralType.NULL,
        "array element type must be non-null");
    Preconditions.checkArgument(values != null && !values.isEmpty(), "array cannot be empty");
    for (Literal value : values) {
      Preconditions.checkArgument(value != null, "array cannot contain a null node");
      value.validate();
      Preconditions.checkArgument(
          value.literalType() == elementType,
          "array literals must have one homogeneous non-null type");
    }
  }

  @Override
  public int depth() {
    return 0;
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof LiteralArray)) {
      return false;
    }
    LiteralArray that = (LiteralArray) other;
    return elementType == that.elementType && Objects.equals(values, that.values);
  }

  @Override
  public int hashCode() {
    return Objects.hash(elementType, values);
  }

  @Override
  public String toString() {
    return "LiteralArray{" + "elementType=" + elementType + ", values=" + values + '}';
  }
}
