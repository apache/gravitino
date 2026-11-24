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
package org.apache.gravitino.semantic;

import com.google.common.base.Preconditions;

/** Selects a snapshot relative to the Semantic Model identity. */
public final class SemanticModelRevisionSelector {

  /** Selects the revision visible to consumers. */
  public static final SemanticModelRevisionSelector ACTIVE = new SemanticModelRevisionSelector(0);

  /** Selects the latest saved revision. */
  public static final SemanticModelRevisionSelector LATEST = new SemanticModelRevisionSelector(0);

  private final int number;

  private SemanticModelRevisionSelector(int number) {
    this.number = number;
  }

  /** Returns a selector for the given positive revision number. */
  public static SemanticModelRevisionSelector numbered(int number) {
    Preconditions.checkArgument(number > 0, "Revision must be positive: %s", number);
    return new SemanticModelRevisionSelector(number);
  }

  /** Returns the number for a numbered selector, or zero for a pointer selector. */
  public int getNumber() {
    return number;
  }
}
