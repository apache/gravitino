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

package org.apache.gravitino.cli.outputs;

import static org.apache.gravitino.cli.outputs.OutputConstant.BASIC_ASCII;
import static org.apache.gravitino.cli.outputs.OutputConstant.FANCY_ASCII;

import com.google.common.collect.ImmutableList;
import java.util.List;

/**
 * Defines different styles for formatting and displaying data. Each style contains a specific set
 * of characters for rendering and configuration for whether to show boundaries between data rows.
 */
public enum Style {
  FANCY(FANCY_ASCII, false),
  FANCY2(FANCY_ASCII, true),
  BASIC(BASIC_ASCII, true),
  BASIC2(BASIC_ASCII, false);

  /**
   * A simple style using basic ASCII characters for display. Shows boundaries between data rows for
   * better visual separation.
   */
  private final ImmutableList<Character> characters;

  private final boolean showRowBoundaries;

  /**
   * Constructs a {@link Style} instance.
   *
   * @param characters the list of characters to use for the style
   * @param showRowBoundaries {@code true} to show boundaries between data rows, {@code false}
   *     otherwise
   */
  Style(ImmutableList<Character> characters, boolean showRowBoundaries) {
    this.characters = characters;
    this.showRowBoundaries = showRowBoundaries;
  }

  /**
   * Returns the list of characters used for this style.
   *
   * @return the list of characters used for rendering
   */
  public List<Character> getCharacters() {
    return characters;
  }

  /**
   * Indicates whether this style shows boundaries between data rows.
   *
   * @return {@code true} if boundaries between data rows are shown, {@code false} otherwise
   */
  public boolean isRowBoundariesEnabled() {
    return showRowBoundaries;
  }
}
