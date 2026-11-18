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

/** A declared local parameter, unresolved against a physical column. */
public final class InputReference extends SqlExpression {
  private final String name;
  private final SqlType type;

  InputReference(String name, SqlType type) {
    super(Collections.emptyList());
    this.name = name;
    this.type = type;
  }

  /**
   * Returns the normalized local parameter name.
   *
   * @return the normalized local parameter name
   */
  public String name() {
    return name;
  }

  /**
   * Returns the intrinsic logical type, or null when inferred from context.
   *
   * @return the intrinsic logical type, or null when inferred from context
   */
  @Override
  public SqlType type() {
    return type;
  }

  @Override
  int depth() {
    return 0;
  }

  /** {@inheritDoc} */
  @Override
  public boolean equals(Object other) {
    return other instanceof InputReference && sameContents((InputReference) other);
  }

  @Override
  List<?> attributes() {
    return Arrays.asList(name, type);
  }
}
