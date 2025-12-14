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
import org.apache.gravitino.rel.expressions.NamedReference;

/** Data transfer object representing a field reference. */
@EqualsAndHashCode
public class FieldReferenceDTO implements FunctionArg, NamedReference {

  /**
   * Creates a new instance of {@link FieldReferenceDTO}.
   *
   * @param fieldName The field name.
   * @return The new instance.
   */
  public static FieldReferenceDTO of(String... fieldName) {
    return new FieldReferenceDTO(fieldName);
  }

  private final String[] fieldName;

  private FieldReferenceDTO(String[] fieldName) {
    this.fieldName = fieldName;
  }

  /**
   * @return The field name.
   */
  @Override
  public String[] fieldName() {
    return fieldName;
  }

  /**
   * @return The name of the field reference.
   */
  @Override
  public ArgType argType() {
    return ArgType.FIELD;
  }

  /** Builder for {@link FieldReferenceDTO}. */
  public static class Builder {
    private String[] fieldName;

    /**
     * Set the field name for the field reference.
     *
     * @param fieldName The field name.
     * @return The builder.
     */
    public Builder withFieldName(String[] fieldName) {
      this.fieldName = fieldName;
      return this;
    }

    /**
     * Set the column name for the field reference.
     *
     * @param columnName The column name.
     * @return The builder.
     */
    public Builder withColumnName(String columnName) {
      this.fieldName = new String[] {columnName};
      return this;
    }

    /** Creates a new instance of {@link Builder}. */
    private Builder() {}

    /**
     * Build the field reference.
     *
     * @return The field reference.
     */
    public FieldReferenceDTO build() {
      return new FieldReferenceDTO(fieldName);
    }
  }

  /**
   * @return the builder for creating a new instance of FieldReferenceDTO.
   */
  public static Builder builder() {
    return new Builder();
  }
}
