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
package org.apache.gravitino.dto.rel.expressions;

import lombok.EqualsAndHashCode;
import org.apache.gravitino.rel.expressions.literals.Literal;
import org.apache.gravitino.rel.types.Type;
import org.apache.gravitino.rel.types.Types;

/** Represents a Literal Data Transfer Object (DTO) that implements the Literal interface. */
@EqualsAndHashCode
public class LiteralDTO implements Literal<String>, FunctionArg {

  /** An instance of LiteralDTO with a value of "NULL" and a data type of Types.NullType.get(). */
  public static final LiteralDTO NULL = new LiteralDTO("NULL", Types.NullType.get());

  private final String value;
  private final Type dataType;

  private LiteralDTO(String value, Type dataType) {
    this.value = value;
    this.dataType = dataType;
  }

  /**
   * @return The value of the literal.
   */
  @Override
  public String value() {
    return value;
  }

  /**
   * @return The data type of the literal.
   */
  @Override
  public Type dataType() {
    return dataType;
  }

  /**
   * @return The type of the argument.
   */
  @Override
  public ArgType argType() {
    return ArgType.LITERAL;
  }

  @Override
  public String toString() {
    return "LiteralDTO{" + "value='" + value + '\'' + ", dataType=" + dataType + '}';
  }

  /** Builder for LiteralDTO. */
  public static class Builder {
    private String value;
    private Type dataType;

    /**
     * Set the value of the literal.
     *
     * @param value The value of the literal.
     * @return The builder.
     */
    public Builder withValue(String value) {
      this.value = value;
      return this;
    }

    /**
     * Set the data type of the literal.
     *
     * @param dataType The data type of the literal.
     * @return The builder.
     */
    public Builder withDataType(Type dataType) {
      this.dataType = dataType;
      return this;
    }

    /** Creates a new instance of {@link Builder}. */
    private Builder() {}

    /**
     * Builds a LiteralDTO instance.
     *
     * @return The LiteralDTO instance.
     */
    public LiteralDTO build() {
      return new LiteralDTO(value, dataType);
    }
  }

  /**
   * @return the builder for creating a new instance of LiteralDTO.
   */
  public static Builder builder() {
    return new Builder();
  }
}
