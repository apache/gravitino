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
// sql/catalyst/src/main/java/org/apache/spark/sql/connector/expressions/Transform.java

package com.datastrato.gravitino.rel.expressions.transforms;

import com.datastrato.gravitino.annotation.Evolving;
import com.datastrato.gravitino.rel.expressions.Expression;
import com.datastrato.gravitino.rel.expressions.NamedReference;
import java.util.Objects;

/**
 * Represents a transform function in the public logical expression API.
 *
 * <p>For example, the transform date(ts) is used to derive a date value from a timestamp column.
 * The transform name is "date" and its argument is a reference to the "ts" column.
 */
@Evolving
public interface Transform extends Expression {
  /** @return The transform function name. */
  String name();

  /** @return The arguments passed to the transform function. */
  Expression[] arguments();

  /**
   * @return The preassigned partitions in the partitioning. Currently, only ListTransform and
   *     RangeTransform need to deal with assignments
   */
  default Expression[] assignments() {
    return Expression.EMPTY_EXPRESSION;
  }

  @Override
  default Expression[] children() {
    return arguments();
  }

  /** Base class for simple transforms of a single field. */
  abstract class SingleFieldTransform implements Transform {
    NamedReference ref;

    /** @return the referenced field name as an array of String parts. */
    public String[] fieldName() {
      return ref.fieldName();
    }

    @Override
    public NamedReference[] references() {
      return new NamedReference[] {ref};
    }

    @Override
    public Expression[] arguments() {
      return new Expression[] {ref};
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof SingleFieldTransform)) {
        return false;
      }
      SingleFieldTransform that = (SingleFieldTransform) o;
      return Objects.equals(ref, that.ref);
    }

    @Override
    public int hashCode() {
      return Objects.hash(ref);
    }
  }
}
