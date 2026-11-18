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
package org.apache.gravitino.policy;

import com.google.common.collect.ImmutableSet;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.gravitino.MetadataObject;

/** Built-in policy content for masking columns of tagged tables or tagged columns. */
public final class ColumnMaskContent extends ReadRestrictionContent {

  private static final Set<MetadataObject.Type> SUPPORTED_OBJECT_TYPES =
      ImmutableSet.of(MetadataObject.Type.TABLE, MetadataObject.Type.COLUMN);

  @Nullable private final String expression;

  /** Default constructor for Jackson deserialization only. */
  private ColumnMaskContent() {
    this.expression = null;
  }

  ColumnMaskContent(String expression) {
    this.expression = expression;
    validate();
  }

  /**
   * Returns the authored column-mask expression.
   *
   * @return column-mask expression
   */
  public String expression() {
    return expression;
  }

  /** {@inheritDoc} */
  @Override
  public Set<MetadataObject.Type> supportedObjectTypes() {
    return SUPPORTED_OBJECT_TYPES;
  }

  @Override
  public Map<String, Object> rules() {
    return Collections.singletonMap("expression", expression);
  }

  @Override
  public void validate() throws IllegalArgumentException {
    super.validate();
    validateSource(expression, "column-mask expression");
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof ColumnMaskContent)) {
      return false;
    }
    ColumnMaskContent that = (ColumnMaskContent) other;
    return Objects.equals(expression, that.expression);
  }

  @Override
  public int hashCode() {
    return Objects.hash(expression);
  }

  @Override
  public String toString() {
    return "ColumnMaskContent{" + "expression='" + expression + '\'' + '}';
  }
}
