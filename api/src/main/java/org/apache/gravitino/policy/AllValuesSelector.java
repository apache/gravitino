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

import org.apache.gravitino.annotation.Evolving;

/** A policy association selector that matches whenever the effective tag assignment exists. */
@Evolving
public final class AllValuesSelector implements PolicyAssociationSelector {

  /** The selector type for tag-presence matching regardless of assignment values. */
  public static final String TYPE = "ALL_VALUES";

  private static final AllValuesSelector INSTANCE = new AllValuesSelector();

  private AllValuesSelector() {}

  /**
   * Returns the selector that matches by tag presence, regardless of assignment values.
   *
   * @return The all-values selector.
   */
  public static AllValuesSelector get() {
    return INSTANCE;
  }

  @Override
  public String type() {
    return TYPE;
  }
}
